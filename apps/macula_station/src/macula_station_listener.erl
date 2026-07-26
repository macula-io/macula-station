%% @doc QUIC listener owner.
%%
%% Owns one `macula_transport:listener()' reference and drives an
%% async accept loop. Each `{quic, new_conn, ConnRef, _Info}' message
%% spawns a `macula_peering' handshake worker (under the peering
%% library's own `simple_one_for_one' supervisor) whose
%% `controlling_pid' is the station's single
%% `macula_station_peer_observer'. Peering events therefore land at
%% the observer, not here.
%%
%% The listener holds enough opts (identity, realms, capabilities,
%% certs) to build peering opts on each accept, plus a pid for the
%% observer it forwards handshake ownership to.
%%
%% == Per-identity peering cap (handshaking-only) ==
%%
%% The listener bounds concurrent *handshaking* workers — workers
%% that have not yet completed CONNECT/HELLO. Healthy connected
%% peers do NOT consume the cap.
%%
%% Three state maps:
%% <ul>
%%   <li>`handshaking' — workers spawned by `macula_peering:accept/2'
%%       that have not yet signalled `handshake_complete'. Cap
%%       applies here.</li>
%%   <li>`connected' — workers that have transitioned to the
%%       `connected' peering state and emitted the
%%       `{macula_peering, handshake_complete, ConnPid, PeerNodeId}'
%%       signal (macula SDK ≥ 4.2.0). Untracked count, only monitored
%%       for cleanup on death.</li>
%%   <li>`peers' — reverse index `PeerNodeId => {Ref, Pid}'. On a
%%       duplicate dial from the same identity, the prior worker is
%%       sent a graceful `{close, replaced_by_newer_handshake}' and
%%       its `connected' slot is freed when the resulting DOWN
%%       fires. Without this dedupe, a peer that re-dials before its
%%       prior conn is torn down accumulates `connected' workers on
%%       every retry — observed in production at 99 stuck workers
%%       from a single sister-station.</li>
%% </ul>
%%
%% When `handshaking' size reaches the cap, new inbound
%% `{quic, new_conn}' messages are rejected: the QUIC connection
%% is closed cleanly and a `_macula.peering.cap_exceeded'
%% diagnostic event is emitted.
%%
%% Cap is read from `application:get_env(macula_station,
%% peering_cap_per_identity, 1000)' at boot. Each identity's
%% listener has its own count.
%%
%% Why this design: V1 daemon clients dialed V2 stations with
%% un-parseable frames; peering workers stayed in `handshaking'
%% forever and the sup grew to 1000+ workers per box (see
%% `PLAN_FLYING_RESTART.md'). The 30s SDK-side handshake timeout
%% drains stuck workers, but until 4.1.0 the listener's cap counted
%% every alive worker, so a healthy fleet of stubs (~500 per
%% station) filled the slot pool and starved station↔station
%% handshakes (in production: 129k+ rejected inbounds on saturated
%% stations). Splitting the count restores the cap to its original
%% intent.
%%
%% Graceful shutdown: `terminate/2' closes the transport listener so
%% the port is released before the sup restarts us. Inbound peer
%% connections are owned by the peering workers; shutting down the
%% listener does not disturb in-flight peers.
-module(macula_station_listener).
-behaviour(gen_server).

-export([start_link/1, stop/1, listen_addr/1, stats/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, listen_addr/0, stats/0, cert_source/0]).

-ifdef(TEST).
%% Exports for unit tests — pure opts composition.
-export([peering_opts/1]).
-endif.

-type cert_source() ::
        {pem, CertFile :: file:name_all(), KeyFile :: file:name_all()}
      | {self_signed_pubkey, macula_identity:key_pair()}.

%% Opts may carry either:
%% - `cert_source' (preferred, since Tier 3 of the sovereign-overlay
%%   rollout — supports both file-anchored and pubkey-anchored
%%   listeners), OR
%% - the legacy `certfile' + `keyfile' pair (V1 path).
%%
%% Exactly one shape must be present. The init/1 callback resolves
%% `cert_source = {self_signed_pubkey, ...}' to a temp file pair.
-type opts() :: #{
    bind         := inet:ip_address() | string(),
    port         := inet:port_number(),
    cert_source  => cert_source(),
    certfile     => file:name_all(),
    keyfile      => file:name_all(),
    identity     := macula_identity:key_pair(),
    realms       := [macula_identity:pubkey()],
    capabilities := non_neg_integer(),
    observer     := pid()
}.

-type listen_addr() :: {inet:ip_address() | string(), inet:port_number()}.

-type stats() :: #{
    %% Sum of `handshaking' and `connected'. Preserved as the
    %% pre-4.1 field name so downstream inspectors keep working;
    %% the per-state breakdown is the load-bearing detail now.
    in_flight   := non_neg_integer(),
    handshaking := non_neg_integer(),
    connected   := non_neg_integer(),
    cap         := pos_integer(),
    rejected    := non_neg_integer()
}.

%% Cap on concurrent handshaking workers only. Healthy handshakes
%% finish in ~100ms; the 30s SDK handshake timeout bounds stuck
%% workers. 1000 is generous: it absorbs a fleet-wide reconnect
%% burst without ever pinching legit traffic.
-define(DEFAULT_PEERING_CAP, 1000).

-record(state, {
    listener    :: macula_transport:listener(),
    listen_addr :: listen_addr(),
    opts        :: opts(),
    cap         :: pos_integer(),
    %% Workers that have not yet signalled `handshake_complete'.
    %% Cap applies here.
    handshaking :: #{reference() => pid()},
    %% Workers that have completed handshake and transitioned to
    %% `connected'. Monitored only for cleanup on death.
    connected   :: #{reference() => pid()},
    %% Reverse index by verified peer identity. On handshake_complete
    %% with a `PeerNodeId' already present, the prior worker is sent
    %% `{close, replaced_by_newer_handshake}' and the entry is
    %% replaced. Cleared from this map on DOWN.
    peers       :: #{macula_identity:pubkey() => {reference(), pid()}},
    %% Conn → Worker pid for stray-event forwarding. macula_peering's
    %% accept path spawns the worker first then transfers QUIC
    %% ownership. Between `{quic, new_conn, Conn, _}' arriving here
    %% and `controlling_process(Conn, WorkerPid)' completing, any
    %% subsequent `{quic, new_stream, _, #{conn => Conn}}' message
    %% the QUIC NIF dispatches lands in OUR mailbox (the listener
    %% still owned the conn at dispatch time). The pre-fix
    %% `handle_info(_, S)' wildcard dropped them silently and the
    %% worker waited forever for a stream — verified live across
    %% the fleet (every station accumulated tens of stuck workers,
    %% all with `peer_node_id = undefined' and `buf_size = 0').
    %% Forwarding the stray frames to the right worker closes the
    %% race without changing the SDK's accept signature.
    conn_to_worker = #{} :: #{macula_transport:connection() => pid()},
    rejected    :: non_neg_integer()
}).

%%==================================================================
%% API
%%==================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

-spec listen_addr(pid()) -> listen_addr().
listen_addr(Pid) ->
    gen_server:call(Pid, listen_addr).

-spec stats(pid()) -> stats().
stats(Pid) ->
    gen_server:call(Pid, stats).

%%==================================================================
%% gen_server
%%==================================================================

init(Opts0) ->
    process_flag(trap_exit, true),
    case resolve_cert_source(Opts0) of
        {ok, Opts} ->
            on_listen(macula_transport:listen(listen_opts(Opts)), Opts);
        {error, Reason} ->
            {stop, {cert_source_failed, Reason}}
    end.

on_listen({ok, Listener}, Opts) ->
    ok = macula_transport:accept(Listener),
    Cap = application:get_env(macula_station, peering_cap_per_identity,
                              ?DEFAULT_PEERING_CAP),
    {ok, #state{listener       = Listener,
                listen_addr    = derive_addr(Opts),
                opts           = Opts,
                cap            = Cap,
                handshaking    = #{},
                connected      = #{},
                peers          = #{},
                conn_to_worker = #{},
                rejected       = 0}};
on_listen({error, Reason}, _Opts) ->
    {stop, {listen_failed, Reason}}.

handle_call(listen_addr, _From, #state{listen_addr = A} = S) ->
    {reply, A, S};
handle_call(stats, _From, #state{handshaking = HS, connected = C,
                                  cap = Cap, rejected = R} = S) ->
    HSize = maps:size(HS),
    CSize = maps:size(C),
    {reply, #{in_flight   => HSize + CSize,
              handshaking => HSize,
              connected   => CSize,
              cap         => Cap,
              rejected    => R}, S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({quic, new_conn, Conn, _Info}, S) ->
    on_new_conn(Conn, over_capacity(S), S);
handle_info({macula_peering, handshake_complete, WorkerPid, PeerNodeId}, S) ->
    {noreply, on_handshake_complete(WorkerPid, PeerNodeId, S)};
handle_info({'DOWN', Ref, process, Pid, _Reason}, S) ->
    {noreply, drop_ref(Ref, Pid, S)};
%% Stray QUIC events for an accepted conn that the listener still
%% briefly owned at dispatch time. Forward to the worker the SDK
%% spawned for this Conn. Without this clause the wildcard below
%% silently dropped them and the worker waited the full
%% handshake_timeout (30s) for a stream that already arrived.
handle_info({quic, new_stream, _Stream, #{conn := Conn}} = Msg,
            #state{conn_to_worker = M} = S) ->
    forward_stray_quic(maps:find(Conn, M), Msg),
    {noreply, S};
handle_info({quic, closed, Conn, _Detail} = Msg,
            #state{conn_to_worker = M} = S) ->
    forward_stray_quic(maps:find(Conn, M), Msg),
    {noreply, S#state{conn_to_worker = maps:remove(Conn, M)}};
handle_info(_Msg, S) ->
    {noreply, S}.

forward_stray_quic({ok, Pid}, Msg) when is_pid(Pid) ->
    Pid ! Msg, ok;
forward_stray_quic(_, _Msg) ->
    ok.

terminate(_Reason, #state{listener = L}) ->
    _ = catch macula_transport:close_listener(L),
    ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%==================================================================
%% Internals
%%==================================================================

listen_opts(#{bind := Bind, port := Port, certfile := C, keyfile := K}) ->
    #{bind => Bind, port => Port, certfile => C, keyfile => K}.

%% Materialize the cert/key file pair from `cert_source', if
%% present. Existing legacy callers pass `certfile'/`keyfile'
%% directly — those are accepted unchanged.
-spec resolve_cert_source(opts()) ->
    {ok, opts()} | {error, term()}.
resolve_cert_source(#{certfile := _, keyfile := _} = Opts) ->
    {ok, Opts};
resolve_cert_source(#{cert_source := {pem, CertFile, KeyFile}} = Opts) ->
    {ok, Opts#{certfile => CertFile, keyfile => KeyFile}};
resolve_cert_source(#{cert_source := {self_signed_pubkey, {Pubkey, Privkey}},
                      identity := _} = Opts) ->
    %% Generate a self-signed cert wrapping the station-keypair pubkey,
    %% write to a per-pid temp dir, hand the paths off. Stations are
    %% realm-agnostic infrastructure — no realm-derived SANs are added.
    %% `macula-net' is the routing substrate; the listener does not
    %% bind realm-scoped addresses.
    case macula_quic:generate_self_signed_cert(Pubkey, Privkey, []) of
        {ok, {CertPem, KeyPem}} ->
            write_temp_cert_pair(CertPem, KeyPem, Opts);
        {error, _} = E ->
            E
    end;
resolve_cert_source(_Opts) ->
    {error, missing_cert_source}.

write_temp_cert_pair(CertPem, KeyPem, Opts) ->
    %% Per-process temp dir so concurrent listeners on the same box
    %% don't trample each other's files. The dir is left in place
    %% across listener restarts (file path is stable for the
    %% lifetime of this BEAM node, recreated on each boot).
    Dir = filename:join([temp_root(), "macula_station_listener",
                         pid_to_dir(self())]),
    case filelib:ensure_dir(filename:join(Dir, "x")) of
        ok ->
            CertFile = filename:join(Dir, "cert.pem"),
            KeyFile  = filename:join(Dir, "key.pem"),
            ok = file:write_file(CertFile, CertPem),
            ok = file:write_file(KeyFile, KeyPem),
            ok = file:change_mode(KeyFile, 8#600),
            {ok, Opts#{certfile => CertFile, keyfile => KeyFile}};
        {error, _} = E ->
            E
    end.

temp_root() ->
    case os:getenv("TMPDIR") of
        false -> "/tmp";
        ""    -> "/tmp";
        T     -> T
    end.

pid_to_dir(Pid) ->
    %% Erlang-pid syntax `<0.123.0>' has angle brackets that aren't
    %% safe in paths. Replace with underscores.
    P = pid_to_list(Pid),
    [case C of
         $< -> $_;
         $> -> $_;
         $. -> $_;
         _  -> C
     end || C <- P].

derive_addr(#{bind := Bind, port := Port}) -> {Bind, Port}.

over_capacity(#state{handshaking = HS, cap = Cap}) ->
    maps:size(HS) >= Cap.

%% Cap reached — close the QUIC connection cleanly, re-arm the
%% listener, emit a structured diagnostic, and bump the rejected
%% counter. No peering worker is spawned for this connection.
on_new_conn(Conn, true, #state{listener = L, listen_addr = Addr,
                                cap = Cap, handshaking = HS,
                                connected = C, rejected = R} = S) ->
    _ = macula_transport:close_connection(Conn),
    _ = macula_transport:accept(L),
    macula_diagnostics:event(<<"_macula.peering.cap_exceeded">>, #{
        listen_addr => Addr,
        cap         => Cap,
        handshaking => maps:size(HS),
        connected   => maps:size(C)
    }),
    {noreply, S#state{rejected = R + 1}};
on_new_conn(Conn, false, S) ->
    {noreply, accept_conn(Conn, S)}.

accept_conn(Conn, #state{opts = Opts, listener = L, handshaking = HS,
                          conn_to_worker = CW} = S) ->
    {NewHS, NewCW} = on_peering_accept(
                       macula_peering:accept(Conn, peering_opts(Opts)),
                       Conn, HS, CW),
    %% Re-arm the listener for the next inbound connection. Ignoring
    %% the result is safe: on a broken listener we would receive no
    %% further `new_conn' messages; supervisor restart re-binds.
    _ = macula_transport:accept(L),
    S#state{handshaking = NewHS, conn_to_worker = NewCW}.

%% Monitor the spawned peering worker so we can clean up when it dies
%% (handshake fail, GOODBYE drain, peer close, or caller-initiated stop).
%% The monitor ref doubles as the map key. New workers always start in
%% `handshaking'; on `handshake_complete' they migrate to `connected'.
%% Also stamp Conn → WorkerPid so any stray QUIC events that landed in
%% our mailbox before `controlling_process' transferred ownership get
%% forwarded to the right worker.
on_peering_accept({ok, WorkerPid}, Conn, HS, CW) when is_pid(WorkerPid) ->
    Ref = erlang:monitor(process, WorkerPid),
    {HS#{Ref => WorkerPid}, CW#{Conn => WorkerPid}};
on_peering_accept({error, _}, _Conn, HS, CW) ->
    {HS, CW}.

%% Worker emitted `{macula_peering, handshake_complete, ConnPid,
%% PeerNodeId}' — move it from `handshaking' to `connected' and
%% reconcile the `peers' index. If a prior worker exists for the
%% same `PeerNodeId', send it `{close, replaced_by_newer_handshake}'
%% (graceful drain via the SDK's draining state) and let the DOWN
%% reaper free its `connected' slot. Idempotent on unknown pid.
on_handshake_complete(Pid, PeerNodeId,
                       #state{handshaking = HS, connected = C,
                              peers = P} = S) ->
    promote_to_connected(find_ref_by_pid(Pid, HS), Pid, PeerNodeId,
                         HS, C, P, S).

promote_to_connected(error, _Pid, _PeerNodeId, _HS, _C, _P, S) ->
    S;
promote_to_connected(Ref, Pid, PeerNodeId, HS, C, P, S) ->
    NewP = replace_peer_entry(PeerNodeId, Ref, Pid, P),
    S#state{handshaking = maps:remove(Ref, HS),
            connected   = C#{Ref => Pid},
            peers       = NewP}.

%% Insert the new {Ref, Pid} for `PeerNodeId'. If a prior entry
%% exists, fire-and-forget the graceful close cast at the old worker
%% before overwriting. The old worker drains via the SDK's `draining'
%% state and emits its own DOWN, which `drop_ref/2' then reaps from
%% `connected' and `peers'.
replace_peer_entry(PeerNodeId, Ref, Pid, P) ->
    maybe_close_old_worker(maps:find(PeerNodeId, P), Pid, PeerNodeId),
    P#{PeerNodeId => {Ref, Pid}}.

maybe_close_old_worker({ok, {_OldRef, OldPid}}, NewPid, PeerNodeId)
        when OldPid =/= NewPid ->
    macula_peering:close(OldPid, replaced_by_newer_handshake),
    macula_diagnostics:event(<<"_macula.peering.duplicate_replaced">>, #{
        peer_node_id_prefix =>
            binary:part(PeerNodeId, 0, min(8, byte_size(PeerNodeId)))
    });
maybe_close_old_worker(_, _, _) ->
    ok.

peering_opts(#{identity := Id, realms := R, capabilities := C,
               observer := Observer}) ->
    Base = #{
        role            => server,
        identity        => Id,
        realms          => R,
        capabilities    => C,
        controlling_pid => Observer,
        %% Tells the peering worker to send us a single
        %% {macula_peering, handshake_complete, self()} message the
        %% moment it transitions from `handshaking' to `connected'.
        %% Drives the handshaking → connected slot migration.
        accept_owner    => self(),
        %% Stamp `RecvAtUs' on every inbound-frame notification so
        %% peer_observer / dht / pubsub_dispatcher can compute mailbox
        %% wait. Requires macula >= 4.4.7; older SDK runtimes ignore
        %% this opt and emit the legacy tuple shapes (which all three
        %% recipients still match for backward compatibility).
        timing_enabled  => true
    },
    %% Route DHT-class frames directly to the DHT server, bypassing
    %% the observer's mailbox. peer_observer at steady state runs
    %% 200-400 deep under live DHT chatter (~85% of inbound frames
    %% are store/store_ack); funnelling all of that through the same
    %% gen_server delays everything else. SDK >= 4.4.3 honours this
    %% opt; earlier SDKs ignore it and DHT frames flow via
    %% controlling_pid (backward-compatible).
    %%
    %% Both recipients are passed as REGISTERED NAMES, not pids. macula
    %% >= 7.1.0 re-resolves a name on every frame, so a recipient
    %% crash-restart is transparent. Passing `whereis/1' here captured
    %% the pid once for the life of the connection: after a restart the
    %% SDK kept posting to the dead pid (its guard was `is_pid/1', true
    %% for a dead pid) and every frame was silently discarded by the VM
    %% until the connection was torn down. It also made the bypass
    %% depend on a boot race — a connection accepted before the
    %% recipient registered never got the bypass at all.
    Base1 = Base#{dht_recipient => macula_dht},
    %% Route pubsub-class frames (subscribe, unsubscribe, publish,
    %% event) to the dedicated pubsub dispatcher. After the DHT
    %% bypass shipped (4.4.3), `event' frames became the dominant
    %% load on peer_observer — multi-publisher cases fire bursts of
    %% Ed25519-verify-per-event work. SDK >= 4.4.4 honours this opt;
    %% earlier SDKs ignore it and pubsub frames flow via
    %% controlling_pid (backward-compatible).
    Base1#{pubsub_recipient => macula_station_route_pubsub_frames}.

%% DOWN routing — a monitored worker died. Could be in either lifecycle
%% map. Probe both; the matching entry is removed. Always also drop
%% any `peers' entry whose monitor ref matches — covers both the
%% normal case (worker exited after replacement) and the race where
%% the DOWN arrives before `handshake_complete' wrote a fresh entry.
%% Also scrub any `conn_to_worker' entries pointing at this Pid so
%% the stray-event forwarder doesn't relay to a dead worker.
drop_ref(Ref, Pid, #state{handshaking = HS, connected = C, peers = P,
                          conn_to_worker = CW} = S0) ->
    S = drop_ref_in(maps:is_key(Ref, HS), Ref, HS, C, S0),
    S#state{peers = drop_peer_by_ref(Ref, P),
            conn_to_worker = drop_conns_for_pid(Pid, CW)}.

drop_ref_in(true, Ref, HS, _C, S) ->
    S#state{handshaking = maps:remove(Ref, HS)};
drop_ref_in(false, Ref, _HS, C, S) ->
    S#state{connected = maps:remove(Ref, C)}.

drop_conns_for_pid(Pid, CW) ->
    maps:filter(fun(_Conn, P) -> P =/= Pid end, CW).

%% Remove only the entry whose monitor ref matches. A peer that has
%% already been replaced by a newer worker has a different ref under
%% the same `PeerNodeId' key — that newer entry must survive the
%% old worker's DOWN.
drop_peer_by_ref(Ref, P) ->
    maps:filter(fun(_PeerNodeId, {R, _Pid}) -> R =/= Ref end, P).

%% Find the monitor ref for a given worker pid by scanning the map.
%% O(n) — acceptable: only fires once per accepted conn at the moment
%% of handshake completion, and the handshaking map is bounded by
%% the configured cap.
find_ref_by_pid(Pid, M) ->
    maps:fold(fun(R, P, _Acc) when P =:= Pid -> R;
                 (_, _, Acc) -> Acc
              end, error, M).
