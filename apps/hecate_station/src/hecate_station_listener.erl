%% @doc QUIC listener owner.
%%
%% Owns one `hecate_transport:listener()' reference and drives an
%% async accept loop. Each `{quic, new_conn, ConnRef, _Info}' message
%% spawns a `macula_peering' handshake worker (under the peering
%% library's own `simple_one_for_one' supervisor) whose
%% `controlling_pid' is the station's single
%% `hecate_station_peer_observer'. Peering events therefore land at
%% the observer, not here.
%%
%% The listener holds enough opts (identity, realms, capabilities,
%% certs) to build peering opts on each accept, plus a pid for the
%% observer it forwards handshake ownership to.
%%
%% == Per-identity peering cap ==
%%
%% The listener tracks the count of currently-alive peering workers
%% it has spawned (via monitor refs). When the count reaches the
%% configured cap, new inbound `{quic, new_conn}' messages are
%% rejected: the QUIC connection is closed cleanly and a
%% `_macula.peering.cap_exceeded' diagnostic event is emitted.
%%
%% Cap is read from `application:get_env(hecate_station,
%% peering_cap_per_identity, 500)' at boot. Each identity's listener
%% has its own count; one identity flooding cannot suffocate others.
%%
%% This protects against the stuck-handshaking accumulation observed
%% in `PLAN_FLYING_RESTART.md' — V1 daemon clients dialed V2 stations
%% with un-parseable frames, peering workers stayed in `handshaking'
%% forever, sup grew to 1000+ workers per box. The cap blocks new
%% conns once the limit hits; a subsequent SDK-side handshake
%% timeout will drain the existing stuck workers.
%%
%% Graceful shutdown: `terminate/2' closes the transport listener so
%% the port is released before the sup restarts us. Inbound peer
%% connections are owned by the peering workers; shutting down the
%% listener does not disturb in-flight peers.
-module(hecate_station_listener).
-behaviour(gen_server).

-export([start_link/1, stop/1, listen_addr/1, stats/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, listen_addr/0, stats/0]).

-type opts() :: #{
    bind         := inet:ip_address() | string(),
    port         := inet:port_number(),
    certfile     := file:name_all(),
    keyfile      := file:name_all(),
    identity     := macula_identity:key_pair(),
    realms       := [macula_identity:pubkey()],
    capabilities := non_neg_integer(),
    observer     := pid()
}.

-type listen_addr() :: {inet:ip_address() | string(), inet:port_number()}.

-type stats() :: #{
    in_flight := non_neg_integer(),
    cap       := pos_integer(),
    rejected  := non_neg_integer()
}.

-define(DEFAULT_PEERING_CAP, 500).

-record(state, {
    listener    :: hecate_transport:listener(),
    listen_addr :: listen_addr(),
    opts        :: opts(),
    cap         :: pos_integer(),
    in_flight   :: #{reference() => pid()},
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

init(Opts) ->
    process_flag(trap_exit, true),
    on_listen(hecate_transport:listen(listen_opts(Opts)), Opts).

on_listen({ok, Listener}, Opts) ->
    ok = hecate_transport:accept(Listener),
    Cap = application:get_env(hecate_station, peering_cap_per_identity,
                              ?DEFAULT_PEERING_CAP),
    {ok, #state{listener    = Listener,
                listen_addr = derive_addr(Opts),
                opts        = Opts,
                cap         = Cap,
                in_flight   = #{},
                rejected    = 0}};
on_listen({error, Reason}, _Opts) ->
    {stop, {listen_failed, Reason}}.

handle_call(listen_addr, _From, #state{listen_addr = A} = S) ->
    {reply, A, S};
handle_call(stats, _From, #state{in_flight = IF, cap = Cap, rejected = R} = S) ->
    {reply, #{in_flight => maps:size(IF), cap => Cap, rejected => R}, S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({quic, new_conn, Conn, _Info}, S) ->
    on_new_conn(Conn, over_capacity(S), S);
handle_info({'DOWN', Ref, process, _Pid, _Reason},
            #state{in_flight = IF} = S) ->
    {noreply, S#state{in_flight = maps:remove(Ref, IF)}};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, #state{listener = L}) ->
    _ = catch hecate_transport:close_listener(L),
    ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%==================================================================
%% Internals
%%==================================================================

listen_opts(#{bind := Bind, port := Port, certfile := C, keyfile := K}) ->
    #{bind => Bind, port => Port, certfile => C, keyfile => K}.

derive_addr(#{bind := Bind, port := Port}) -> {Bind, Port}.

over_capacity(#state{in_flight = IF, cap = Cap}) ->
    maps:size(IF) >= Cap.

%% Cap reached — close the QUIC connection cleanly, re-arm the
%% listener, emit a structured diagnostic, and bump the rejected
%% counter. No peering worker is spawned for this connection.
on_new_conn(Conn, true, #state{listener = L, listen_addr = Addr,
                                cap = Cap, in_flight = IF,
                                rejected = R} = S) ->
    _ = hecate_transport:close_connection(Conn),
    _ = hecate_transport:accept(L),
    macula_diagnostics:event(<<"_macula.peering.cap_exceeded">>, #{
        listen_addr => Addr,
        cap         => Cap,
        in_flight   => maps:size(IF)
    }),
    {noreply, S#state{rejected = R + 1}};
on_new_conn(Conn, false, S) ->
    {noreply, accept_conn(Conn, S)}.

accept_conn(Conn, #state{opts = Opts, listener = L, in_flight = IF} = S) ->
    NewIF = on_peering_accept(macula_peering:accept(Conn, peering_opts(Opts)),
                              IF),
    %% Re-arm the listener for the next inbound connection. Ignoring
    %% the result is safe: on a broken listener we would receive no
    %% further `new_conn' messages; supervisor restart re-binds.
    _ = hecate_transport:accept(L),
    S#state{in_flight = NewIF}.

%% Monitor the spawned peering worker so we can decrement the in-flight
%% counter when it dies (handshake fail, GOODBYE drain, peer close, or
%% caller-initiated stop). The monitor ref doubles as the map key.
on_peering_accept({ok, WorkerPid}, IF) when is_pid(WorkerPid) ->
    Ref = erlang:monitor(process, WorkerPid),
    IF#{Ref => WorkerPid};
on_peering_accept({error, _}, IF) ->
    IF.

peering_opts(#{identity := Id, realms := R, capabilities := C,
               observer := Observer}) ->
    #{
        role            => server,
        identity        => Id,
        realms          => R,
        capabilities    => C,
        controlling_pid => Observer
    }.
