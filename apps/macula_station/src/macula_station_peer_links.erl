%% @doc Outbound station-link registry — singleton.
%%
%% Tracks the live outbound `macula_station_outbound_link' workers this
%% station has dialled to peer stations. Each entry carries the URL
%% the link was dialled with, the link worker pid, and the peer's
%% verified `node_id' (filled in once the CONNECT/HELLO handshake
%% completes — links register themselves in two stages).
%%
%% Cross-relay fabric modules (`bloom_exchange', `peering_router',
%% `relay_ping') and the `seed_dial' bootstrap tier
%% (`macula_station_via_seed_dial') query the registry for
%% the list of live peers.
%%
%% == Registration lifecycle ==
%%
%% A link calls `register/2' as soon as its worker pid exists (so the
%% registry can monitor it for crash cleanup), and `set_peer_node_id/2'
%% once the handshake yields the peer's verified node-id. The
%% `verified_peers/0' snapshot only includes entries that have made
%% it past the second stage. On disconnect the link calls
%% `clear_peer_node_id/1' to drop out of `verified_peers/0' until the
%% next successful handshake.
-module(macula_station_peer_links).
-behaviour(gen_server).

-export([start_link/0, stop/0]).
-export([register/2, set_peer_node_id/2, clear_peer_node_id/1,
         unregister/1, connections/0, connected_hostnames/0,
         verified_peers/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([verified_peer/0]).

-type verified_peer() :: #{
    url      := binary(),
    pid      := pid(),
    node_id  := macula_identity:pubkey(),
    host     := binary(),
    port     := inet:port_number()
}.

-record(entry, {
    url      :: binary(),
    pid      :: pid(),
    monitor  :: reference(),
    node_id  :: macula_identity:pubkey() | undefined,
    host     :: binary(),
    port     :: inet:port_number()
}).

-record(state, {
    %% Live outbound station_links keyed by URL (e.g. <<"quic://host:port">>).
    by_url = #{} :: #{binary() => #entry{}},
    %% Pid → URL reverse index drives `'DOWN'' cleanup when a link dies.
    by_pid = #{} :: #{pid() => binary()}
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec stop() -> ok.
stop() ->
    gen_server:stop(?MODULE).

%% @doc Register an outbound link by URL + worker pid. The peer's
%% `node_id' is filled in via `set_peer_node_id/2' once the handshake
%% completes; until then `verified_peers/0' will not include this
%% entry.
-spec register(binary(), pid()) -> ok.
register(Url, LinkPid) when is_binary(Url), is_pid(LinkPid) ->
    gen_server:cast(?MODULE, {register, Url, LinkPid}).

%% @doc Stamp the peer's verified node-id onto a previously-registered
%% link. Called by `macula_station_outbound_link' on receipt of
%% `{macula_peering, connected, _, NodeId}'.
-spec set_peer_node_id(binary(), macula_identity:pubkey()) -> ok.
set_peer_node_id(Url, NodeId) when is_binary(Url), is_binary(NodeId) ->
    gen_server:cast(?MODULE, {set_peer_node_id, Url, NodeId}).

%% @doc Drop the cached node-id for a link without unregistering the
%% link itself. Called when the underlying peering connection closes
%% so the link drops out of `verified_peers/0' until it reconnects.
-spec clear_peer_node_id(binary()) -> ok.
clear_peer_node_id(Url) when is_binary(Url) ->
    gen_server:cast(?MODULE, {clear_peer_node_id, Url}).

%% @doc Drop an outbound link by URL.
-spec unregister(binary()) -> ok.
unregister(Url) when is_binary(Url) ->
    gen_server:cast(?MODULE, {unregister, Url}).

%% @doc Snapshot of `[{Url, LinkPid}]' for live outbound links. Returns
%% `[]' when the registry is empty or unreachable.
-spec connections() -> [{binary(), pid()}].
connections() ->
    try gen_server:call(?MODULE, connections, 500)
    catch _:_ -> []
    end.

%% @doc Snapshot of peer hostnames extracted from registered URLs.
-spec connected_hostnames() -> [binary()].
connected_hostnames() ->
    [Host || #{host := Host} <- verified_peers()].

%% @doc Snapshot of links that have completed the CONNECT/HELLO
%% handshake. Used by `macula_station_via_seed_dial' to
%% feed the cascade with peers learnt from outbound dials.
-spec verified_peers() -> [verified_peer()].
verified_peers() ->
    try gen_server:call(?MODULE, verified_peers, 500)
    catch _:_ -> []
    end.

%%====================================================================
%% gen_server
%%====================================================================

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call(connections, _From, #state{by_url = U} = S) ->
    {reply, [{Url, E#entry.pid} || {Url, E} <- maps:to_list(U)], S};
handle_call(verified_peers, _From, #state{by_url = U} = S) ->
    {reply, [entry_to_verified(E) || E <- maps:values(U), E#entry.node_id =/= undefined], S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast({register, Url, LinkPid}, S) ->
    {noreply, do_register(Url, LinkPid, S)};
handle_cast({set_peer_node_id, Url, NodeId}, S) ->
    {noreply, do_set_node_id(Url, NodeId, S)};
handle_cast({clear_peer_node_id, Url}, S) ->
    {noreply, do_clear_node_id(Url, S)};
handle_cast({unregister, Url}, S) ->
    {noreply, drop_url(Url, S)};
handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({'DOWN', _Ref, process, Pid, _Reason},
            #state{by_pid = P} = S) ->
    case maps:find(Pid, P) of
        {ok, Url} -> {noreply, drop_url(Url, S)};
        error     -> {noreply, S}
    end;
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%====================================================================
%% Internals
%%====================================================================

do_register(Url, LinkPid, S0) ->
    %% Replace any prior entry for the same URL.
    #state{by_url = U, by_pid = P} = drop_url(Url, S0),
    Ref = erlang:monitor(process, LinkPid),
    {Host, Port} = parse_url(Url),
    Entry = #entry{url = Url, pid = LinkPid, monitor = Ref,
                   node_id = undefined, host = Host, port = Port},
    #state{by_url = U#{Url => Entry},
           by_pid = P#{LinkPid => Url}}.

do_set_node_id(Url, NodeId, #state{by_url = U} = S) ->
    case maps:find(Url, U) of
        {ok, E} -> S#state{by_url = U#{Url => E#entry{node_id = NodeId}}};
        error   -> S
    end.

do_clear_node_id(Url, #state{by_url = U} = S) ->
    case maps:find(Url, U) of
        {ok, E} -> S#state{by_url = U#{Url => E#entry{node_id = undefined}}};
        error   -> S
    end.

drop_url(Url, #state{by_url = U, by_pid = P} = S) ->
    case maps:find(Url, U) of
        {ok, #entry{pid = Pid, monitor = Ref}} ->
            erlang:demonitor(Ref, [flush]),
            S#state{by_url = maps:remove(Url, U),
                    by_pid = maps:remove(Pid, P)};
        error ->
            S
    end.

entry_to_verified(#entry{url = U, pid = Pid, node_id = N,
                         host = H, port = Port}) ->
    #{url => U, pid => Pid, node_id => N, host => H, port => Port}.

parse_url(<<"quic://", Rest/binary>>)  -> parse_host_port(Rest);
parse_url(<<"https://", Rest/binary>>) -> parse_host_port(Rest);
parse_url(B) when is_binary(B)         -> parse_host_port(B).

%% Bracketed IPv6 (`[::1]:36422` or bare `[::1]`) MUST be matched
%% before the plain-host path: an IPv6 address contains colons of its
%% own, so splitting on the first `:' in `[::1]:36422' without
%% recognising the brackets first cuts inside the address
%% (`binary:split/2' on `":"` yields `["[", ":1]:36422"]'), and
%% `binary_to_integer/1' on the port half then crashes this
%% gen_server — every real caller re-derives the URL fresh next
%% register, so the entry is merely delayed, but the whole registry
%% (every OTHER entry too) resets to empty on the restart in between.
%% Production `outbound_peers' config is host+port pairs (DNS
%% hostnames), never IPv6 literals — brackets only occur in test-
%% harness loopback URLs — but the crash is loud enough elsewhere
%% (asymmetric-loss / content-transfer station-to-station tests use
%% `dial_outbound/2' against `::1') that it needs the real fix.
parse_host_port(<<"[", Rest/binary>>) ->
    parse_bracketed_host(binary:split(Rest, <<"]">>));
parse_host_port(B) ->
    case binary:split(B, <<":">>) of
        [H, P] -> {H, binary_to_integer(P)};
        [H]    -> {H, 4433}
    end.

parse_bracketed_host([Host, <<>>]) ->
    {Host, 4433};
parse_bracketed_host([Host, <<":", Port/binary>>]) ->
    {Host, binary_to_integer(Port)}.
