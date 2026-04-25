%% @doc QUIC listener owner.
%%
%% Owns one `hecate_transport:listener()' reference and drives an
%% async accept loop. Each `{quic, new_conn, ConnRef, _Info}' message
%% spawns a `hecate_peering' handshake worker (under the peering
%% library's own `simple_one_for_one' supervisor) whose
%% `controlling_pid' is the station's single
%% `hecate_station_peer_observer'. Peering events therefore land at
%% the observer, not here.
%%
%% The listener holds enough opts (identity, realms, capabilities,
%% certs) to build peering opts on each accept, plus a pid for the
%% observer it forwards handshake ownership to.
%%
%% Graceful shutdown: `terminate/2' closes the transport listener so
%% the port is released before the sup restarts us. Inbound peer
%% connections are owned by the peering workers; shutting down the
%% listener does not disturb in-flight peers.
-module(hecate_station_listener).
-behaviour(gen_server).

-export([start_link/1, stop/1, listen_addr/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, listen_addr/0]).

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

-record(state, {
    listener    :: hecate_transport:listener(),
    listen_addr :: listen_addr(),
    opts        :: opts()
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

%%==================================================================
%% gen_server
%%==================================================================

init(Opts) ->
    process_flag(trap_exit, true),
    on_listen(hecate_transport:listen(listen_opts(Opts)), Opts).

on_listen({ok, Listener}, Opts) ->
    ok = hecate_transport:accept(Listener),
    {ok, #state{listener    = Listener,
                listen_addr = derive_addr(Opts),
                opts        = Opts}};
on_listen({error, Reason}, _Opts) ->
    {stop, {listen_failed, Reason}}.

handle_call(listen_addr, _From, #state{listen_addr = A} = S) ->
    {reply, A, S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({quic, new_conn, Conn, _Info}, S) ->
    {noreply, accept_conn(Conn, S)};
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

accept_conn(Conn, #state{opts = Opts, listener = L} = S) ->
    _ = hecate_peering:accept(Conn, peering_opts(Opts)),
    %% Re-arm the listener for the next inbound connection. Ignoring
    %% the result is safe: on a broken listener we would receive no
    %% further `new_conn' messages; supervisor restart re-binds.
    _ = hecate_transport:accept(L),
    S.

peering_opts(#{identity := Id, realms := R, capabilities := C,
               observer := Observer}) ->
    #{
        role            => server,
        identity        => Id,
        realms          => R,
        capabilities    => C,
        controlling_pid => Observer
    }.
