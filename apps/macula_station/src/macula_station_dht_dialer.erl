%% @doc On-demand outbound dial for peers discovered mid-DHT-walk.
%%
%% `macula_station_dht_transport:send_frame/2' can only reach a NodeId
%% this station already holds a connection to (`conn_for/2'); on a miss it
%% returns `{error, no_route}' and gives up. That is fine for the k-closest
%% peers already in our routing table (we are connected to those by
%% definition), but a genuine multi-round Kademlia walk
%% (`macula_station_dht_handlers') discovers NEW candidates mid-walk, via
%% the `addresses' a NODES reply carries on each `station_ref()'
%% (`macula_dht_protocol:entry_to_station_ref/2', fixed 2026-07-27 to stop
%% publishing `[]'). Those candidates are worthless without something that
%% dials them.
%%
%% This module is that something: given a NodeId and the `addresses' from
%% its `station_ref()', it dials, verifies the handshake-proven NodeId
%% actually matches the one the walk was chasing, and — only then —
%% registers the connection with `macula_station_peer_observer' exactly as
%% `macula_station_outbound_link' does for a configured peer
%% (`connected_outbound' with the dialled endpoint), so `conn_for/2' finds
%% it on the next `send_frame' and every later caller — DHT, RPC, pubsub,
%% content — gets to reuse the same connection.
%%
%% == Trust ==
%%
%% A `station_ref()' arrives from a peer we asked FIND_VALUE, i.e. from
%% someone we have not fully vetted for THIS specific claim. The dialled
%% peer's HELLO signature is what actually proves identity (same
%% CONNECT/HELLO trust boundary as every other Macula dial); this module's
%% own job is to refuse to act on that proof unless it matches the NodeId
%% the caller asked for. A mismatch is refused and the connection is
%% closed rather than registered — the alternative is letting a peer's
%% NODES reply redirect us to dial (and trust) an address of its choosing
%% under someone else's name, which would poison every consumer of
%% `conn_for/2', not just the DHT walk that triggered the dial.
%%
%% == Lifecycle ==
%%
%% One connection at a time per `ensure_dialed/3' caller; concurrent calls
%% for the SAME NodeId from different walk branches may each dial —
%% harmless, `macula_station_peer_observer' already resolves duplicate
%% conns for one peer (see its inbound/outbound race handling), and the
%% loser's connection is simply an extra idle one until its own silence
%% detection (none here — this module registers and gets out of the way;
%% the resulting connection is a completely ordinary peering connection
%% from every consumer's point of view, no different from one
%% `macula_station_outbound_link' would have made).
-module(macula_station_dht_dialer).
-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0, ensure_dialed/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(pending, {
    node_id    :: macula_identity:pubkey(),
    addresses  :: [map()],
    from       :: gen_server:from(),
    timer      :: reference()
}).

-record(state, {
    pending = #{} :: #{pid() => #pending{}}
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Ensure a connection to `NodeId' exists, dialling `Addresses' (the
%% `addresses' field of its `station_ref()') if it does not.
%%
%% Returns as soon as either an existing connection is found or a fresh
%% dial completes (verified) or fails; never longer than `TimeoutMs' plus
%% a small margin for the reply to land. `Addresses' empty is answered
%% immediately with `{error, no_address}' — publishing no endpoint is the
%% honest signal that this peer cannot be reached and is not this
%% module's problem to solve.
-spec ensure_dialed(macula_identity:pubkey(), [map()], pos_integer()) ->
        ok | {error, term()}.
ensure_dialed(<<_:256>> = NodeId, Addresses, TimeoutMs)
  when is_list(Addresses), is_integer(TimeoutMs), TimeoutMs > 0 ->
    try
        gen_server:call(?MODULE, {ensure_dialed, NodeId, Addresses, TimeoutMs},
                        TimeoutMs + 1_000)
    catch
        exit:{timeout, _} -> {error, timeout};
        exit:{noproc, _}  -> {error, not_started}
    end.

%%====================================================================
%% gen_server
%%====================================================================

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call({ensure_dialed, NodeId, Addresses, Timeout}, From, S) ->
    dispatch_ensure_dialed(already_connected(NodeId), NodeId, Addresses,
                           Timeout, From, S);
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({macula_peering, connected, ConnPid, PeerNodeId}, S) ->
    {noreply, on_connected(ConnPid, PeerNodeId, S)};
handle_info({dial_timeout, ConnPid}, S) ->
    {noreply, on_dial_timeout(ConnPid, S)};
handle_info({macula_peering, disconnected, ConnPid, Reason}, S) ->
    {noreply, on_disconnected(ConnPid, Reason, S)};
handle_info({'EXIT', ConnPid, Reason}, #state{pending = P} = S)
  when is_map_key(ConnPid, P) ->
    {noreply, on_disconnected(ConnPid, Reason, S)};
handle_info({'EXIT', _Pid, _Reason}, S) ->
    {noreply, S};
handle_info({macula_peering, frame, ConnPid, Frame}, S) ->
    %% This module stays the QUIC `controlling_pid' for every
    %% connection it dials (set in `dial_opts_with_self/0', needed to
    %% catch the `connected'/`disconnected' notifications above) — it
    %% is never handed off, unlike a dedicated stream relayed via
    %% `macula_station_outbound_link', which transfers ownership once
    %% its job is done. Every later frame on this connection keeps
    %% arriving here for the connection's whole lifetime, and must be
    %% forwarded, exactly as `macula_station_outbound_link' forwards
    %% any frame type it does not handle itself — dropping it made a
    %% dialer-established connection write-only from this station's
    %% own point of view: it could relay a CALL out via
    %% `macula_station_peer_observer:on_remote_lookup/5' sending
    %% straight over the raw ConnPid, but could never receive a reply,
    %% a fresh CALL, or anything else back, since nothing else owned
    %% this mailbox. Found live 2026-08-21: the walk's on-demand dial
    %% created a genuine direct link where none existed before, and
    %% calls INTO the dialling station over it timed out 100% of the
    %% time with zero error signal beyond a debug log line.
    forward_to_observer({macula_peering, frame, ConnPid, Frame}),
    {noreply, S};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.
code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%====================================================================
%% ensure_dialed
%%====================================================================

already_connected(NodeId) ->
    case whereis(macula_station_peer_observer) of
        undefined -> {error, not_started};
        Observer  -> macula_station_peer_observer:conn_for(Observer, NodeId)
    end.

dispatch_ensure_dialed({ok, _ConnPid}, _NodeId, _Addresses, _Timeout, _From, S) ->
    {reply, ok, S};
dispatch_ensure_dialed({error, not_started}, _NodeId, _Addresses, _Timeout,
                       _From, S) ->
    {reply, {error, not_started}, S};
dispatch_ensure_dialed(error, NodeId, Addresses, Timeout, From, S) ->
    start_dial(first_address(Addresses), NodeId, Addresses, Timeout, From, S).

first_address([Addr | _]) -> {ok, Addr};
first_address([])         -> error.

start_dial(error, _NodeId, _Addresses, _Timeout, _From, S) ->
    {reply, {error, no_address}, S};
start_dial({ok, Addr}, NodeId, Addresses, Timeout, From, S) ->
    dispatch_dial(dial_opts_with_self(), Addr, NodeId, Addresses, Timeout,
                 From, S).

dial_opts_with_self() ->
    with_self(macula_station:dial_opts()).

with_self({ok, Opts}) -> {ok, Opts#{controlling_pid => self()}};
with_self({error, _} = E) -> E.

dispatch_dial({error, Reason}, _Addr, _NodeId, _Addresses, _Timeout, _From, S) ->
    {reply, {error, Reason}, S};
dispatch_dial({ok, Opts}, #{host := Host, port := Port}, NodeId, Addresses,
              Timeout, From, S) ->
    Target = #{host => Host, port => Port, timeout_ms => Timeout},
    on_connect(macula_peering:connect(Opts#{target => Target}),
              NodeId, Addresses, Timeout, From, S).

on_connect({error, Reason}, _NodeId, _Addresses, _Timeout, _From, S) ->
    {reply, {error, Reason}, S};
on_connect({ok, ConnPid}, NodeId, Addresses, Timeout, From,
          #state{pending = P} = S) ->
    Timer = erlang:send_after(Timeout, self(), {dial_timeout, ConnPid}),
    Entry = #pending{node_id = NodeId, addresses = Addresses, from = From,
                     timer = Timer},
    {noreply, S#state{pending = P#{ConnPid => Entry}}}.

%%====================================================================
%% Async dial resolution
%%====================================================================

on_connected(ConnPid, PeerNodeId, #state{pending = P} = S) ->
    settle_connected(maps:take(ConnPid, P), ConnPid, PeerNodeId, S).

settle_connected(error, _ConnPid, _PeerNodeId, S) ->
    %% Stray or already-resolved (timeout raced the handshake) — ignore.
    S;
settle_connected({#pending{node_id = NodeId} = Entry, NewP}, ConnPid,
                 PeerNodeId, S) when PeerNodeId =:= NodeId ->
    _ = erlang:cancel_timer(Entry#pending.timer),
    register_outbound(ConnPid, NodeId, Entry#pending.addresses),
    gen_server:reply(Entry#pending.from, ok),
    S#state{pending = NewP};
settle_connected({#pending{node_id = NodeId} = Entry, NewP}, ConnPid,
                 PeerNodeId, S) ->
    _ = erlang:cancel_timer(Entry#pending.timer),
    ?LOG_WARNING("[dht_dialer] refusing conn ~p: dialled for ~p, "
                 "handshake proved ~p", [ConnPid, NodeId, PeerNodeId]),
    catch macula_peering:close(ConnPid, node_id_mismatch),
    gen_server:reply(Entry#pending.from,
                     {error, {node_id_mismatch, NodeId, PeerNodeId}}),
    S#state{pending = NewP}.

register_outbound(ConnPid, NodeId, Addresses) ->
    forward_to_observer({macula_peering, connected_outbound, ConnPid, NodeId,
                         Addresses}).

on_dial_timeout(ConnPid, #state{pending = P} = S) ->
    settle_timeout(maps:take(ConnPid, P), ConnPid, S).

settle_timeout(error, _ConnPid, S) ->
    S;
settle_timeout({Entry, NewP}, ConnPid, S) ->
    catch macula_peering:close(ConnPid, dial_timeout),
    gen_server:reply(Entry#pending.from, {error, timeout}),
    S#state{pending = NewP}.

on_disconnected(ConnPid, Reason, #state{pending = P} = S) ->
    forward_to_observer({macula_peering, disconnected_outbound, ConnPid,
                         Reason}),
    settle_disconnected(maps:take(ConnPid, P), S).

settle_disconnected(error, S) ->
    S;
settle_disconnected({Entry, NewP}, S) ->
    %% Died before ever completing the handshake (rare: EXIT racing the
    %% dial timeout). The dial simply failed.
    _ = erlang:cancel_timer(Entry#pending.timer),
    gen_server:reply(Entry#pending.from, {error, disconnected}),
    S#state{pending = NewP}.

forward_to_observer(Msg) ->
    case whereis(macula_station_peer_observer) of
        undefined -> ok;
        Pid       -> Pid ! Msg, ok
    end.
