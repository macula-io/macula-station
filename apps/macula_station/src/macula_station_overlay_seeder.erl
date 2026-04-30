%%% @doc Per-identity overlay seeder — initiates outbound peering
%%% connections to a configured list of station hostnames so the
%%% relay-mesh has actual relay-to-relay edges.
%%%
%%% Without this, macula_station_peer_observer reports zero peers
%%% (no module ever calls macula_station:connect_to/1 from inside
%%% the codebase) and the realm topology view has no edges to draw.
%%%
%%% The seed list comes from the identity's `MACULA_OVERLAY_SEEDS_*'
%%% env vars, falling back to a global `MACULA_OVERLAY_SEEDS' env.
%%% Format: comma-separated `quic://host:port' or `host:port'
%%% entries. Empty / missing → no outbound connects, observer stays
%%% empty (legacy behaviour).
%%%
%%% Connections are fire-and-forget on init. peer_observer registers
%%% them via the standard `{macula_peering, connected, _, _}` event;
%%% the announcer's caps_hint emit picks them up on the next refresh.
%%% On disconnect peer_observer drops them; this module does NOT
%%% auto-reconnect — operators restart the identity if seeds need
%%% to be re-tried (rare; sessions outlive most failures via QUIC
%%% reuse).
-module(macula_station_overlay_seeder).
-behaviour(gen_server).

-export([start_link/1, stop/1, connected_hostnames/1, connections/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type opts() :: #{
    identity      := macula_identity:key_pair(),
    peer_observer := pid(),
    realms        => [macula_identity:pubkey()],
    capabilities  => non_neg_integer(),
    %% Seed list — overrides env var when present (test injection).
    seeds         => [binary()],
    identity_key  => term()  %% logger metadata
}.

-export_type([opts/0]).

-record(state, {
    identity      :: macula_identity:key_pair(),
    peer_observer :: pid(),
    realms        :: [macula_identity:pubkey()],
    capabilities  :: non_neg_integer(),
    seeds         :: [binary()],
    conns         :: [{binary(), pid()}]   %% list of {Url, Pid} on success
}).

%% Default port — matches MACULA_QUIC_PORT in production.
-define(DEFAULT_PORT, 4433).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{identity := _, peer_observer := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%% @doc Return the list of hostnames the seeder is currently connected
%% to (one per still-alive outbound peering). The announcer joins this
%% with peer_observer's locally-resolved peers so the relay-to-relay
%% edges show up even when the peer's own node_record hasn't yet
%% replicated into the local DHT.
-spec connected_hostnames(pid()) -> [binary()].
connected_hostnames(Pid) ->
    try gen_server:call(Pid, connected_hostnames, 1_000)
    catch _:_ -> []
    end.

%% @doc Return the full `[{Url, LinkPid}]' list of currently-alive
%% outbound peering connections. Used by `macula_station_bloom_exchange'
%% to broadcast its local Bloom filter to every peer station, and by
%% the peering forwarder to hand off published frames.
-spec connections(pid()) -> [{binary(), pid()}].
connections(Pid) ->
    try gen_server:call(Pid, connections, 1_000)
    catch _:_ -> []
    end.

%%====================================================================
%% gen_server
%%====================================================================

init(Opts) ->
    process_flag(trap_exit, true),
    set_logger_identity(Opts),
    State0 = build_state(Opts),
    %% Schedule the connect attempts off-process so init/1 returns
    %% promptly — peer_observer has already started by the time
    %% identity_sup boots us, so we don't race the observer.
    self() ! seed,
    {ok, State0}.

build_state(#{identity := Kp, peer_observer := Obs} = Opts) ->
    Seeds = case maps:find(seeds, Opts) of
                {ok, S} when is_list(S) -> S;
                error                   -> seeds_from_env()
            end,
    #state{
        identity      = Kp,
        peer_observer = Obs,
        realms        = maps:get(realms, Opts, []),
        capabilities  = maps:get(capabilities, Opts, 0),
        seeds         = Seeds,
        conns         = []
    }.

set_logger_identity(#{identity_key := Key}) ->
    logger:set_process_metadata(#{identity_id => Key});
set_logger_identity(_) ->
    ok.

handle_call(connected_hostnames, _From, #state{conns = Conns} = S) ->
    Hosts = [hostname_of(Url) || {Url, _Pid} <- Conns],
    {reply, [H || H <- Hosts, H =/= undefined], S};
handle_call(connections, _From, #state{conns = Conns} = S) ->
    {reply, Conns, S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

%% Strip scheme + port off a seed URL to produce the hostname the
%% realm/JS visualiser expects (e.g. "relay-de-nuremberg.macula.io").
hostname_of(Url) ->
    case parse_url(Url) of
        {ok, Host, _Port} when is_binary(Host) -> Host;
        _ -> undefined
    end.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info(seed, #state{seeds = []} = S) ->
    {noreply, S};
handle_info(seed, S) ->
    Conns = lists:foldl(fun(Url, Acc) ->
        case dial(Url, S) of
            {ok, Pid} ->
                logger:info(
                  "[overlay_seeder] connected ~s pid=~p", [Url, Pid]),
                [{Url, Pid} | Acc];
            {error, Reason} ->
                logger:warning(
                  "[overlay_seeder] dial ~s failed: ~p", [Url, Reason]),
                Acc
        end
    end, [], S#state.seeds),
    {noreply, S#state{conns = Conns}};
handle_info({'EXIT', Pid, _Reason}, #state{conns = Conns} = S) ->
    {noreply, S#state{conns = lists:keydelete(Pid, 2, Conns)}};
handle_info(_, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

code_change(_OldVsn, S, _Extra) ->
    {ok, S}.

%%====================================================================
%% Connect
%%====================================================================

dial(Url, #state{identity = Kp,
                 peer_observer = Obs,
                 realms = Realms,
                 capabilities = Caps}) ->
    case parse_url(Url) of
        {ok, Host, Port} ->
            %% role is set by macula_peering:connect/1 internally but
            %% the typespec marks it required, so include it here too
            %% to avoid a dialyzer contract warning. The duplicate
            %% Opts#{role => client} in connect/1 is idempotent.
            macula_peering:connect(#{
                role            => client,
                identity        => Kp,
                realms          => Realms,
                capabilities    => Caps,
                controlling_pid => Obs,
                target          => #{host => Host, port => Port}
            });
        {error, Reason} ->
            {error, Reason}
    end.

%% Accepts `quic://host:port', `host:port' or bare `host'. Bare host
%% defaults to the production QUIC port.
parse_url(<<"quic://", Rest/binary>>) -> parse_host_port(Rest);
parse_url(<<"https://", Rest/binary>>) -> parse_host_port(Rest);
parse_url(Bin) when is_binary(Bin) -> parse_host_port(Bin).

parse_host_port(Bin) ->
    case binary:split(Bin, <<":">>, [global]) of
        [Host]            -> {ok, Host, ?DEFAULT_PORT};
        [Host, PortBin]   ->
            case strip_path(PortBin) of
                {ok, PortStr} -> port_to_target(Host, PortStr);
                error         -> {error, {bad_url, Bin}}
            end;
        _                 -> {error, {bad_url, Bin}}
    end.

strip_path(PortBin) ->
    case binary:split(PortBin, <<"/">>) of
        [PortBin1]    -> {ok, PortBin1};
        [PortBin1, _] -> {ok, PortBin1};
        _             -> error
    end.

port_to_target(Host, PortStr) ->
    try
        {ok, Host, binary_to_integer(PortStr)}
    catch
        error:badarg -> {error, {bad_port, PortStr}}
    end.

%%====================================================================
%% Env-var seeds
%%====================================================================

%% Read MACULA_OVERLAY_SEEDS as comma-separated `host:port' or
%% `quic://host:port' entries. Empty / unset returns [].
seeds_from_env() ->
    case os:getenv("MACULA_OVERLAY_SEEDS") of
        false -> [];
        ""    -> [];
        S     -> parse_csv(S)
    end.

parse_csv(S) ->
    [list_to_binary(string:trim(E)) || E <- string:split(S, ",", all),
                                       string:trim(E) =/= ""].
