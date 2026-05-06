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
%% Two state maps:
%% <ul>
%%   <li>`handshaking' — workers spawned by `macula_peering:accept/2'
%%       that have not yet signalled `handshake_complete'. Cap
%%       applies here.</li>
%%   <li>`connected' — workers that have transitioned to the
%%       `connected' peering state and emitted the
%%       `{macula_peering, handshake_complete, ConnPid}' signal
%%       (macula SDK ≥ 4.1.0). Untracked count, only monitored for
%%       cleanup on death.</li>
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
    {ok, #state{listener    = Listener,
                listen_addr = derive_addr(Opts),
                opts        = Opts,
                cap         = Cap,
                handshaking = #{},
                connected   = #{},
                rejected    = 0}};
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
handle_info({macula_peering, handshake_complete, WorkerPid}, S) ->
    {noreply, on_handshake_complete(WorkerPid, S)};
handle_info({'DOWN', Ref, process, _Pid, _Reason}, S) ->
    {noreply, drop_ref(Ref, S)};
handle_info(_Msg, S) ->
    {noreply, S}.

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
    %% realm-agnostic infrastructure — no realm-derived SANs are added
    %% (the yggdrasil-derived SAN that lived here predated the
    %% sovereign-substrate cutover; macula-net replaces yggdrasil and
    %% does not bind realm-scoped addresses to station listeners).
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

accept_conn(Conn, #state{opts = Opts, listener = L, handshaking = HS} = S) ->
    NewHS = on_peering_accept(macula_peering:accept(Conn, peering_opts(Opts)),
                              HS),
    %% Re-arm the listener for the next inbound connection. Ignoring
    %% the result is safe: on a broken listener we would receive no
    %% further `new_conn' messages; supervisor restart re-binds.
    _ = macula_transport:accept(L),
    S#state{handshaking = NewHS}.

%% Monitor the spawned peering worker so we can clean up when it dies
%% (handshake fail, GOODBYE drain, peer close, or caller-initiated stop).
%% The monitor ref doubles as the map key. New workers always start in
%% `handshaking'; on `handshake_complete' they migrate to `connected'.
on_peering_accept({ok, WorkerPid}, HS) when is_pid(WorkerPid) ->
    Ref = erlang:monitor(process, WorkerPid),
    HS#{Ref => WorkerPid};
on_peering_accept({error, _}, HS) ->
    HS.

%% Worker emitted `{macula_peering, handshake_complete, ConnPid}' —
%% move it from `handshaking' to `connected'. Idempotent (a stray
%% signal for an unknown pid is silently dropped) so a race between
%% handshake_complete and DOWN cannot corrupt state.
on_handshake_complete(Pid, #state{handshaking = HS, connected = C} = S) ->
    promote_to_connected(find_ref_by_pid(Pid, HS), Pid, HS, C, S).

promote_to_connected(error, _Pid, _HS, _C, S) ->
    S;
promote_to_connected(Ref, Pid, HS, C, S) ->
    S#state{handshaking = maps:remove(Ref, HS),
            connected   = C#{Ref => Pid}}.

peering_opts(#{identity := Id, realms := R, capabilities := C,
               observer := Observer}) ->
    #{
        role            => server,
        identity        => Id,
        realms          => R,
        capabilities    => C,
        controlling_pid => Observer,
        %% Tells the peering worker to send us a single
        %% {macula_peering, handshake_complete, self()} message the
        %% moment it transitions from `handshaking' to `connected'.
        %% Drives the handshaking → connected slot migration.
        accept_owner    => self()
    }.

%% DOWN routing — a monitored worker died. Could be in either map.
%% Probe both; the matching entry is removed.
drop_ref(Ref, #state{handshaking = HS, connected = C} = S) ->
    drop_ref_in(maps:is_key(Ref, HS), Ref, HS, C, S).

drop_ref_in(true, Ref, HS, _C, S) ->
    S#state{handshaking = maps:remove(Ref, HS)};
drop_ref_in(false, Ref, _HS, C, S) ->
    S#state{connected = maps:remove(Ref, C)}.

%% Find the monitor ref for a given worker pid by scanning the map.
%% O(n) — acceptable: only fires once per accepted conn at the moment
%% of handshake completion, and the handshaking map is bounded by
%% the configured cap.
find_ref_by_pid(Pid, M) ->
    maps:fold(fun(R, P, _Acc) when P =:= Pid -> R;
                 (_, _, Acc) -> Acc
              end, error, M).
