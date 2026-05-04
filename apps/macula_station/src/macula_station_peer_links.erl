%% @doc Outbound station-link registry — singleton.
%%
%% Tracks the `[{Url, LinkPid}]' list of `macula_station_link' workers
%% this station has dialled to peer stations. Replaces the per-identity
%% `macula_station_overlay_seeder' that V1's yggdrasil-overlay model
%% used. Cross-relay fabric modules (`bloom_exchange', `peering_router',
%% `relay_ping') query this for the list of live outbound peer links.
%%
%% == State today ==
%%
%% The single-station boot pipeline does NOT yet bring up outbound
%% station_links automatically — that wiring is the next slice of the
%% multi-identity rip-out. For now `connections/0' returns `[]', which
%% is the same value the consumer modules previously hard-coded inline.
%% Centralising the empty-list response here gives those callers a
%% single seam to start querying once outbound-dial code lands.
%%
%% == Future ==
%%
%% A future outbound-peering layer (TBD module name) will register
%% station_links via `register/2' and unregister via `unregister/1'.
%% At that point the callers of `connections/0' / `connected_hostnames/0'
%% pick up live data without further code changes on their side.
-module(macula_station_peer_links).
-behaviour(gen_server).

-export([start_link/0, stop/0]).
-export([register/2, unregister/1, connections/0, connected_hostnames/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {
    %% Live outbound station_links keyed by URL (e.g. <<"quic://host:port">>).
    by_url = #{} :: #{binary() => pid()},
    %% Pid → URL reverse index drives `'DOWN'' cleanup when a link dies.
    by_pid = #{} :: #{pid() => {binary(), reference()}}
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

%% @doc Register an outbound station_link.
-spec register(binary(), pid()) -> ok.
register(Url, LinkPid) when is_binary(Url), is_pid(LinkPid) ->
    gen_server:cast(?MODULE, {register, Url, LinkPid}).

%% @doc Drop an outbound station_link by URL.
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
    [hostname_of(Url) || {Url, _Pid} <- connections()].

%%====================================================================
%% gen_server
%%====================================================================

init([]) ->
    process_flag(trap_exit, true),
    {ok, #state{}}.

handle_call(connections, _From, #state{by_url = U} = S) ->
    {reply, maps:to_list(U), S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast({register, Url, LinkPid}, S) ->
    {noreply, do_register(Url, LinkPid, S)};
handle_cast({unregister, Url}, S) ->
    {noreply, do_unregister(Url, S)};
handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({'DOWN', Ref, process, Pid, _Reason},
            #state{by_url = U, by_pid = P} = S) ->
    case maps:find(Pid, P) of
        {ok, {Url, Ref}} ->
            {noreply, S#state{by_url = maps:remove(Url, U),
                              by_pid = maps:remove(Pid, P)}};
        _ ->
            {noreply, S}
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
    #state{by_url = U#{Url => LinkPid},
           by_pid = P#{LinkPid => {Url, Ref}}}.

do_unregister(Url, S) ->
    drop_url(Url, S).

drop_url(Url, #state{by_url = U, by_pid = P} = S) ->
    case maps:find(Url, U) of
        {ok, Pid} ->
            case maps:find(Pid, P) of
                {ok, {_, Ref}} -> erlang:demonitor(Ref, [flush]);
                error          -> ok
            end,
            S#state{by_url = maps:remove(Url, U),
                    by_pid = maps:remove(Pid, P)};
        error ->
            S
    end.

hostname_of(<<"quic://", Rest/binary>>)  -> strip_port(Rest);
hostname_of(<<"https://", Rest/binary>>) -> strip_port(Rest);
hostname_of(B) when is_binary(B)         -> strip_port(B).

strip_port(B) ->
    case binary:split(B, <<":">>) of
        [H | _] -> H
    end.
