%% @doc Outbound peer-link worker — one per configured outbound peer URL.
%%
%% Drives a single `macula_peering:connect/1' worker, captures the
%% peer's verified `node_id' once CONNECT/HELLO completes, registers
%% itself in `macula_station_peer_links', and reconnects on
%% disconnect with exponential backoff.
%%
%% Used by the seed-dial bootstrap tier: each link's `(host, port,
%% node_id)' tuple becomes a `verified_peer()' for the cascade once
%% the link reaches the connected state.
-module(macula_station_outbound_link).
-behaviour(gen_server).

-export([start_link/1, stop/1, peer_node_id/1, conn_pid/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0]).

-type opts() :: #{
    url             := binary(),
    identity        := macula_identity:key_pair(),
    capabilities    => non_neg_integer()
}.

-define(INITIAL_BACKOFF_MS,  1_000).
-define(MAX_BACKOFF_MS,     60_000).
-define(HANDSHAKE_TIMEOUT_MS, 30_000).

-record(state, {
    url             :: binary(),
    host            :: binary(),
    port            :: inet:port_number(),
    identity        :: macula_identity:key_pair(),
    capabilities    :: non_neg_integer(),
    conn_pid        :: pid() | undefined,
    peer_node_id    :: macula_identity:pubkey() | undefined,
    backoff_ms      = ?INITIAL_BACKOFF_MS :: pos_integer(),
    reconnect_timer :: reference() | undefined
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%% @doc Read the peer's verified `node_id' if the handshake has
%% completed. Returns `undefined' while still connecting / reconnecting.
-spec peer_node_id(pid()) -> macula_identity:pubkey() | undefined.
peer_node_id(Pid) ->
    gen_server:call(Pid, peer_node_id, 1_000).

%% @doc Read the underlying `macula_peering_conn' worker pid for this
%% link, if a dial is in progress or established. Returns `undefined'
%% while the link is between connection attempts. Used by the
%% `peer_observer' init reconciliation to derive its conns map from
%% the current outbound state without depending on having received
%% the original `connected' notification.
-spec conn_pid(pid()) -> pid() | undefined.
conn_pid(Pid) ->
    gen_server:call(Pid, conn_pid, 1_000).

%%====================================================================
%% gen_server
%%====================================================================

init(#{url := Url, identity := Kp} = Opts) ->
    process_flag(trap_exit, true),
    {Host, Port} = parse_url(Url),
    State = #state{
        url          = Url,
        host         = Host,
        port         = Port,
        identity     = Kp,
        capabilities = maps:get(capabilities, Opts, 0)
    },
    %% Register in peer_links immediately so the registry can monitor
    %% us; the `node_id' is filled in once the handshake completes.
    ok = macula_station_peer_links:register(Url, self()),
    self() ! dial,
    {ok, State}.

handle_call(peer_node_id, _From, #state{peer_node_id = N} = S) ->
    {reply, N, S};
handle_call(conn_pid, _From, #state{conn_pid = C} = S) ->
    {reply, C, S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(dial, S) ->
    do_dial(S);
handle_info({macula_peering, connected, ConnPid, NodeId} = Msg,
            #state{conn_pid = ConnPid, url = Url} = S)
  when is_binary(NodeId) ->
    ok = macula_station_peer_links:set_peer_node_id(Url, NodeId),
    forward_to_observer(Msg),
    %% Successful handshake — reset backoff for the next disconnect.
    {noreply, S#state{peer_node_id = NodeId,
                      backoff_ms   = ?INITIAL_BACKOFF_MS}};
handle_info({macula_peering, disconnected, ConnPid, _Reason} = Msg,
            #state{conn_pid = ConnPid} = S) ->
    forward_to_observer(Msg),
    {noreply, schedule_reconnect(reset_conn(S))};
handle_info({macula_peering, frame, _ConnPid, _Frame} = Msg, S) ->
    %% Forward to the peer_observer so SWIM/DHT/CALL/PUBSUB frames
    %% inbound on outbound dials route through the same dispatcher
    %% inbound accepts use. The link itself does not process frames.
    forward_to_observer(Msg),
    {noreply, S};
handle_info({'EXIT', ConnPid, _Reason}, #state{conn_pid = ConnPid} = S) ->
    {noreply, schedule_reconnect(reset_conn(S))};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, #state{conn_pid = undefined}) -> ok;
terminate(_Reason, #state{conn_pid = Pid}) ->
    catch macula_peering:close(Pid),
    ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%====================================================================
%% Internals
%%====================================================================

forward_to_observer(Msg) ->
    case whereis(macula_station_peer_observer) of
        undefined -> ok;
        Pid       -> Pid ! Msg, ok
    end.

do_dial(#state{host = H, port = P, identity = Kp,
               capabilities = Caps} = S) ->
    Target = #{host => H, port => P, timeout_ms => ?HANDSHAKE_TIMEOUT_MS},
    Result = macula_peering:connect(#{
        role            => client,
        identity        => Kp,
        realms          => [],
        capabilities    => Caps,
        controlling_pid => self(),
        target          => Target
    }),
    handle_dial_result(Result, S).

handle_dial_result({ok, ConnPid}, S) ->
    {noreply, S#state{conn_pid = ConnPid}};
handle_dial_result({error, _Reason}, S) ->
    {noreply, schedule_reconnect(reset_conn(S))}.

reset_conn(#state{url = Url} = S) ->
    ok = macula_station_peer_links:clear_peer_node_id(Url),
    S#state{conn_pid = undefined, peer_node_id = undefined}.

schedule_reconnect(#state{backoff_ms = Backoff,
                          reconnect_timer = OldTimer} = S) ->
    cancel_timer(OldTimer),
    Timer = erlang:send_after(Backoff, self(), dial),
    Next  = min(Backoff * 2, ?MAX_BACKOFF_MS),
    S#state{reconnect_timer = Timer, backoff_ms = Next}.

cancel_timer(undefined) -> ok;
cancel_timer(Ref)       -> erlang:cancel_timer(Ref), ok.

parse_url(<<"quic://", Rest/binary>>)  -> parse_host_port(Rest);
parse_url(<<"https://", Rest/binary>>) -> parse_host_port(Rest);
parse_url(B) when is_binary(B)         -> parse_host_port(B).

parse_host_port(B) ->
    case binary:split(B, <<":">>) of
        [H, P] -> {H, binary_to_integer(P)};
        [H]    -> {H, 4433}
    end.
