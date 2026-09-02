%% @doc Backbone peering-redundancy watchdog.
%%
%% Ensures a station always tries to hold at least `min_station_peers'
%% live, station-classified peering links -- not just whatever is in
%% the fixed `outbound_peers' list from config.json, which is dialled
%% once at boot (`macula_station_outbound_links_sup') and never
%% revisited. Without this, a station with one backbone edge stays at
%% one forever, even when the DHT already knows about dozens of other
%% stations it could peer with.
%%
%% Measured live on `station-it-milan' (2026-08-26): the only core
%% station with a single station-to-station peer, it carried a
%% `bloom_not_neighbour' rate (knew who wanted a topic, no direct
%% neighbour to reach them) of ~40%, against ~7% on a 4-peer station.
%% Not a wedged link -- wire and process state were fully healthy --
%% just nowhere for gossip to route around ordinary background loss.
%%
%% == Realm-agnostic by construction ==
%%
%% Candidate discovery and selection use only DHT-known station
%% diversity metadata (asn/country/tier from `macula_dht_entry')  --
%% the same fields `macula_dht_diversity' already scores replica
%% placement with (Part 3 §4.3/§5.2). No realm concept appears
%% anywhere in this module, matching `#station_cfg.capabilities''s own
%% doc comment: stations are realm-agnostic infrastructure per the
%% railroad mental model. Realm-scoped overlay membership (HyParView /
%% Plumtree) is deliberately a separate concern, homed in a future
%% `hecate-realm' / `macula-realm' service, not here
%% (`plans/PLAN_STATION_INTEGRATION.md' §8.4).
%%
%% == Candidate discovery ==
%%
%% The DHT routing table is station-only by construction:
%% `macula_station_peer_observer' only calls `macula_dht:observe_async/2'
%% once a peer's `is_station' flag resolves true, or for a statically
%% configured outbound peer (which is always a station by config
%% convention) -- an ordinary daemon connection never enters the
%% table. So `macula_dht:k_closest/3' against this station's own id is
%% already a station-only candidate pool; no separate DHT record
%% lookup is needed.
%%
%% == Selection ==
%%
%% `macula_dht_diversity:novelty_score/2' ranks each candidate against
%% whatever currently-connected stations also appear in the sampled
%% pool, preferring one that adds ASN/country/tier diversity over one
%% that just piles onto an already-represented uplink. This is a
%% approximation against a SAMPLE of current peers (whichever ones the
%% `k_closest' call happened to also return), not the full current
%% peer set's diversity -- good enough to steer selection away from
%% obvious duplication without a second DHT round-trip per tick.
%%
%% == Degree penalty (the "everyone converges on the same hub" fix) ==
%%
%% `novelty_score/2' alone has no term for how loaded the CANDIDATE
%% already is -- only for how it compares to what the dialling station
%% already holds. Measured live on `station-de-frankfurt' (2026-09-02):
%% it carried 9 station peers against a fleet-wide `min_station_peers'
%% of 3, tripped `macula_station_health_publisher''s peering_router
%% pathological tripwire dozens of times over 3 hours (15-25k reds/s
%% against a 10k/s ceiling), and the design doc this module already
%% cites states the router's own tick cost is O(realms x topics x
%% peers) -- so a 3x-target station pays 3x any other station's
%% control-plane cost before counting event volume. Frankfurt kept
%% winning novelty scoring independently across many stations simply
%% for being the oldest, most-replicated DHT entry, and every new
%% station bootstrapped with it as the sole static seed added one more
%% guaranteed edge before this watchdog's own diversification could
%% steer elsewhere.
%%
%% `apply_degree_penalty/3' subtracts a term derived from each
%% candidate's OWN self-reported peer count -- read from its
%% `node_record' (already gossiped fleet-wide via ordinary DHT
%% replication for the realm topology view; see
%% `macula_station_announcer''s `peers' field) via a LOCAL, in-process
%% `macula_dht:find_local_record/2' lookup, no network round-trip. A
%% candidate with no locally-cached record is treated as degree 0 (no
%% penalty) -- unknown is not evidence of overload, and punishing it
%% would just bias selection toward whichever candidates happen to be
%% least visible.
%%
%% == Dial and stability ==
%%
%% At most ONE new dial fires per tick, via
%% `macula_station_outbound_links_sup:dial/3' (a `temporary' child --
%% this module owns reconnect/backoff decisions for peers it selected,
%% not the supervisor). A dialled candidate — whether the dial
%% succeeds or fails — goes on a `cooldown_ms' cooldown before it is
%% eligible for re-selection: a peer that connects will simply show up
%% as already-connected on the next tick and never be reselected
%% anyway; a peer that fails is protected from being redialled every
%% tick. Firing at most one dial per tick, rather than filling every
%% missing slot at once, also keeps a whole fleet of stations from
%% correlating on dialling the same "best" candidate simultaneously.
%%
%% == Trust model ==
%%
%% Dials go through `macula_station_outbound_links_sup:dial/3', the
%% same URL-based, not-identity-pinned trust model every static
%% `outbound_peers' config entry already uses: whoever answers the
%% CONNECT/HELLO handshake at that host:port is accepted, then
%% classified `is_station' from its own signed capability bits
%% (`macula_station_peer_observer'). This module does NOT verify the
%% connecting peer's NodeId matches the DHT entry's -- deliberately
%% different from `macula_station_dht_dialer:ensure_dialed/3', which
%% pins identity because it is chasing one SPECIFIC NodeId a pending
%% request needs. This watchdog has no such specific target: it wants
%% "any additional capability-verified station peer", and a
%% capability-signed HELLO from whoever holds that endpoint today is
%% already exactly as trusted as a static backbone peer is.
-module(macula_station_peering_redundancy).
-behaviour(gen_server).

-include("macula_station_cfg.hrl").

-export([
    start_link/1, stop/1,
    state/1,
    force_tick/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-export([rank/2, eligible/3, on_cooldown/3, pick_endpoint/1, under_target/2,
         apply_degree_penalty/3, degree_penalty/2, peer_degree/2]).
-endif.

-export_type([opts/0, status/0]).

-type opts() :: #{
    dht                 := pid(),
    observer            := pid(),
    identity            := macula_identity:key_pair(),
    capabilities        => non_neg_integer(),
    peering_redundancy  := macula_station_config:peering_redundancy_cfg(),
    %% Optional observer for test assertions -- receives
    %% `{macula_station_peering_redundancy, dialed, NodeId, Result}'
    %% after every dial attempt.
    notify              => pid(),
    %% Test seam: defaults to
    %% `fun macula_station_outbound_links_sup:dial/3'. Standing up a
    %% real dial needs the whole `macula'/`macula_transport' QUIC
    %% stack running (`macula_peering_conn_sup' et al) -- overkill for
    %% testing selection/debounce logic, and exactly the reason
    %% `macula_station_rebootstrap''s own tests inject a stub
    %% discoverer instead of a real bootstrap tier.
    dial_fun            => dial_fun()
}.

-type dial_fun() :: fun((macula_station_outbound_links_sup:peer(),
                          macula_identity:key_pair(), non_neg_integer()) ->
                             {ok, pid()} | {error, term()}).

-type status() :: #{
    stations          := non_neg_integer(),
    min_station_peers := pos_integer(),
    on_cooldown       := non_neg_integer(),
    dials             := non_neg_integer()
}.

-record(state, {
    dht          :: pid(),
    observer     :: pid(),
    identity     :: macula_identity:key_pair(),
    capabilities :: non_neg_integer(),
    cfg          :: macula_station_config:peering_redundancy_cfg(),
    notify       :: pid() | undefined,
    dial_fun     :: dial_fun(),
    dials  = 0   :: non_neg_integer(),
    %% NodeId => cooldown-until monotonic ms.
    cooldown = #{} :: #{macula_identity:pubkey() => integer()}
}).

%%==================================================================
%% API
%%==================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%% @doc Current watchdog status. Handy for `/status' + tests.
-spec state(pid()) -> status().
state(Pid) -> gen_server:call(Pid, state).

%% @doc Run one check synchronously (instead of waiting for the
%% periodic tick). Used by tests that do not want to sleep.
-spec force_tick(pid()) -> status().
force_tick(Pid) -> gen_server:call(Pid, force_tick).

%%==================================================================
%% gen_server
%%==================================================================

init(#{dht := Dht, observer := Obs, identity := Kp,
       peering_redundancy := #peering_redundancy_cfg{} = Cfg} = Opts)
  when is_pid(Dht), is_pid(Obs) ->
    State = #state{
        dht          = Dht,
        observer     = Obs,
        identity     = Kp,
        capabilities = maps:get(capabilities, Opts, 0),
        cfg          = Cfg,
        notify       = maps:get(notify, Opts, undefined),
        dial_fun     = maps:get(dial_fun, Opts,
                                 fun macula_station_outbound_links_sup:dial/3)
    },
    schedule_tick(Cfg),
    {ok, State}.

handle_call(state, _From, S) ->
    {reply, status(S), S};
handle_call(force_tick, _From, S) ->
    NewS = run_tick(S),
    {reply, status(NewS), NewS};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) -> {noreply, S}.

handle_info(tick, #state{cfg = Cfg} = S) ->
    schedule_tick(Cfg),
    {noreply, run_tick(S)};
handle_info(_Msg, S) -> {noreply, S}.

terminate(_Reason, _S) -> ok.
code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%==================================================================
%% Tick logic
%%==================================================================

schedule_tick(#peering_redundancy_cfg{check_period_ms = Ms}) ->
    erlang:send_after(Ms, self(), tick).

run_tick(#state{observer = Obs, cfg = Cfg} = S) ->
    #{stations := Current, station_ids := CurrentIds} =
        macula_station_peer_observer:station_view(Obs),
    maybe_dial(under_target(Current, Cfg), CurrentIds, S).

-spec under_target(non_neg_integer(), macula_station_config:peering_redundancy_cfg()) ->
    boolean().
under_target(Current, #peering_redundancy_cfg{min_station_peers = Target}) ->
    Current < Target.

maybe_dial(false, _CurrentIds, S) -> S;
maybe_dial(true,  CurrentIds,  S) -> dial_best_candidate(CurrentIds, S).

dial_best_candidate(CurrentIds, #state{dht = Dht,
                                        cfg = #peering_redundancy_cfg{
                                            candidate_pool    = Pool,
                                            min_station_peers = Target}} = S) ->
    SelfId  = macula_dht:self_id(Dht),
    Entries = macula_dht:k_closest(Dht, SelfId, Pool),
    {CurrentEntries, Rest} = lists:partition(
        fun(E) -> lists:member(macula_dht_entry:node_id(E), CurrentIds) end, Entries),
    Candidates = eligible(Rest, CurrentIds, S#state.cooldown),
    Ranked = rank(Candidates, CurrentEntries),
    pick_and_dial(apply_degree_penalty(Ranked, Dht, Target), S).

eligible(Candidates, CurrentIds, Cooldown) ->
    Now = now_ms(),
    [C || C <- Candidates,
          not lists:member(macula_dht_entry:node_id(C), CurrentIds),
          not on_cooldown(macula_dht_entry:node_id(C), Cooldown, Now)].

on_cooldown(NodeId, Cooldown, Now) ->
    case maps:find(NodeId, Cooldown) of
        {ok, Until} -> Now < Until;
        error       -> false
    end.

rank(Candidates, CurrentEntries) ->
    Scored = [{macula_dht_diversity:novelty_score(C, CurrentEntries), C} || C <- Candidates],
    lists:sort(fun({S1, _}, {S2, _}) -> S1 >= S2 end, Scored).

%% Re-scores an already-diversity-ranked list against each candidate's
%% OWN reported load, so an already-overloaded hub cannot keep winning
%% on ASN/country/tier diversity alone. See the moduledoc's "Degree
%% penalty" section for the incident this closes.
apply_degree_penalty(Ranked, Dht, Target) ->
    Adjusted = [{Score - degree_penalty(peer_degree(Dht, macula_dht_entry:node_id(C)), Target), C}
                || {Score, C} <- Ranked],
    lists:sort(fun({S1, _}, {S2, _}) -> S1 >= S2 end, Adjusted).

%% Zero at or below the fleet's own healthy target; above it, one full
%% novelty-scale point (matches `macula_dht_diversity''s per-dimension
%% weight) per multiple of the target the candidate is already
%% carrying, capped at novelty_score/2's own maximum (3.0) so this can
%% fully outweigh a maximally-diverse candidate but never produces a
%% runaway score.
-define(DEGREE_PENALTY_CAP, 3.0).

degree_penalty(PeerCount, Target) when PeerCount =< Target -> 0.0;
degree_penalty(PeerCount, Target) ->
    min(?DEGREE_PENALTY_CAP, (PeerCount - Target) / Target).

%% This station's local view of a candidate's own peer count, from
%% whatever `node_record' it has locally cached (an ordinary DHT
%% record, not a live query -- see the moduledoc). `undefined'/absent
%% both read as 0: no visibility is not evidence of overload.
peer_degree(Dht, NodeId) ->
    node_record_peer_count(macula_dht:find_local_record(Dht, NodeId)).

node_record_peer_count([]) -> 0;
node_record_peer_count([Record | Rest]) ->
    case node_record_peers(Record) of
        {ok, Peers} -> peers_length(Peers);
        error       -> node_record_peer_count(Rest)
    end.

%% `16#01' is `macula_record''s own (unexported) TYPE_NODE_RECORD tag
%% -- `macula_station_announcer:publish_tombstone/2' already hardcodes
%% the same literal for the same reason: the SDK builds records with
%% it but does not export it for classification.
node_record_peers(#{type := 16#01} = Record) ->
    #{peers := Peers} = macula_record:read_node_record(Record),
    {ok, Peers};
node_record_peers(_NonNodeRecord) ->
    error.

peers_length(undefined)               -> 0;
peers_length(Peers) when is_list(Peers) -> length(Peers).

pick_and_dial([], S) -> S;
pick_and_dial([{_Score, Candidate} | _], S) -> dial_candidate(Candidate, S).

dial_candidate(Candidate, #state{identity = Kp, capabilities = Caps,
                                  cfg = #peering_redundancy_cfg{cooldown_ms = CooldownMs},
                                  notify = Notify, dial_fun = DialFun,
                                  dials = Dials, cooldown = Cooldown} = S) ->
    NodeId = macula_dht_entry:node_id(Candidate),
    Result = dial_peer(pick_endpoint(macula_dht_entry:endpoints(Candidate)), Kp, Caps, DialFun),
    notify_dial(Notify, NodeId, Result),
    S#state{
        dials    = Dials + 1,
        cooldown = maps:put(NodeId, now_ms() + CooldownMs, Cooldown)
    }.

dial_peer(undefined, _Kp, _Caps, _DialFun) -> {error, no_endpoints};
dial_peer(Peer, Kp, Caps, DialFun) -> DialFun(Peer, Kp, Caps).

%% Endpoints come from `macula_dht_entry:endpoints/1' in the same
%% `#{host, port, transport}' shape `macula_station_peer_observer'
%% already builds them in (`link_endpoints/1'). First one wins; a
%% station-only entry realistically carries one QUIC listen address.
pick_endpoint([]) -> undefined;
pick_endpoint([#{host := H, port := P} | _]) -> #{host => H, port => P};
pick_endpoint([_ | Rest]) -> pick_endpoint(Rest).

notify_dial(undefined, _NodeId, _Result) -> ok;
notify_dial(Pid, NodeId, Result) when is_pid(Pid) ->
    Pid ! {macula_station_peering_redundancy, dialed, NodeId, Result},
    ok.

status(#state{observer = Obs, cfg = #peering_redundancy_cfg{min_station_peers = Target},
              cooldown = Cooldown, dials = Dials}) ->
    #{stations          := Current} = macula_station_peer_observer:station_view(Obs),
    Now = now_ms(),
    #{
        stations          => Current,
        min_station_peers => Target,
        on_cooldown       => maps:size(maps:filter(fun(_, Until) -> Now < Until end, Cooldown)),
        dials             => Dials
    }.

now_ms() -> erlang:monotonic_time(millisecond).
