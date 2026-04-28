%% @doc Per-identity station announcer.
%%
%% Each station identity advertises itself in the mesh by publishing
%% a signed `node_record' (`macula_record', type tag `0x01') into
%% its own DHT on init. The record carries the identity's public
%% key (which IS the record key per the spec), realm list,
%% capabilities, and optional display metadata.
%%
%% Records are refreshed before TTL expires (default: refresh at 75%
%% of TTL). On graceful shutdown the announcer publishes a signed
%% `tombstone' (`type=0x0C') so peers learn the identity is gone
%% without waiting for the TTL.
%%
%% == Lifecycle (per identity_sup) ==
%%
%% Started by `hecate_station_identity_sup' as a procedural child
%% AFTER both `hecate_handler_registry' (Track 2) and
%% `hecate_station_dht_handlers' (Track 2.5) are up. On any
%% one_for_all restart the announcer is recreated and re-publishes
%% the node_record on init — peers see a fresh record with a new
%% UUIDv7 version.
%%
%% == Why not "heartbeat" pubsub? ==
%%
%% Spec records are state, not events. A signed `node_record'
%% with `expires_at' tells subscribers "I'm here for the next N
%% minutes" without re-broadcasting every 30s. Late-joining
%% subscribers query `find_records_by_type/2' for the snapshot.
%% Refresh interval is operator-tunable but defaults to 75% of
%% TTL — well above the floor where TTL gaps would cause the
%% record to disappear from peer caches.
-module(hecate_station_announcer).
-behaviour(gen_server).

-export([start_link/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0]).

-type opts() :: #{
    dht          := pid(),
    identity     := macula_identity:key_pair(),
    %% Optional metadata propagated into the node_record payload.
    realms       => [macula_identity:pubkey()],
    capabilities => non_neg_integer(),
    display_name => binary(),
    caps_hint    => binary(),
    %% Geo + reach metadata (macula 3.4+).
    hostname     => binary(),
    endpoint     => binary(),
    city         => binary(),
    country      => binary(),
    lat          => float() | integer(),
    lng          => float() | integer(),
    ttl_ms       => pos_integer(),
    %% Fraction of TTL after which to refresh (default 0.75).
    refresh_at_fraction => float(),
    %% Per-identity peer observer. Queried at every refresh so the
    %% record's `peers' field stays in lockstep with the live overlay
    %% session set. Optional — when absent the announcer omits the
    %% peers field (matches the pre-3.12 SDK shape).
    peer_observer => pid(),
    %% Per-identity overlay seeder pid — exposes the list of hostnames
    %% this identity has dialled outbound. The announcer adds these to
    %% the caps_hint peer list so the realm topology can render edges
    %% even before peers' own node_records replicate into local DHT.
    overlay_seeder => pid(),
    %% Logger metadata.
    identity_key => term()
}.

-define(DEFAULT_TTL_MS, 600_000).             %% 10 min
-define(DEFAULT_REFRESH_FRACTION, 0.75).      %% refresh at 75% of TTL

-record(state, {
    dht          :: pid(),
    identity     :: macula_identity:key_pair(),
    record_opts  :: macula_record:node_record_opts(),
    realms       :: [macula_identity:pubkey()],
    capabilities :: non_neg_integer(),
    ttl_ms       :: pos_integer(),
    refresh_ms   :: pos_integer(),
    timer_ref    :: reference() | undefined,
    %% Per-identity peer observer pid. `undefined' for unit tests that
    %% don't bring up a real observer.
    peer_observer :: pid() | undefined,
    %% Per-identity overlay seeder pid — `undefined' when no outbound
    %% peering is configured.
    overlay_seeder :: pid() | undefined
}).

%%====================================================================
%% Public API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{dht := _, identity := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid, shutdown, 5_000).

%%====================================================================
%% gen_server
%%====================================================================

init(Opts) ->
    process_flag(trap_exit, true),
    set_logger_identity(Opts),
    State = build_state(Opts),
    %% Publish on init — re-runs on every one_for_all restart cycle.
    publish_node_record(State),
    {ok, schedule_refresh(State)}.

build_state(#{dht := Dht, identity := Kp} = Opts) ->
    Ttl       = maps:get(ttl_ms, Opts, ?DEFAULT_TTL_MS),
    Fraction  = maps:get(refresh_at_fraction, Opts, ?DEFAULT_REFRESH_FRACTION),
    RefreshMs = max(1_000, round(Ttl * Fraction)),
    #state{
        dht            = Dht,
        identity       = Kp,
        record_opts    = node_record_opts(Opts),
        realms         = maps:get(realms, Opts, []),
        capabilities   = maps:get(capabilities, Opts, 0),
        ttl_ms         = Ttl,
        refresh_ms     = RefreshMs,
        timer_ref      = undefined,
        peer_observer  = maps:get(peer_observer, Opts, undefined),
        overlay_seeder = maps:get(overlay_seeder, Opts, undefined)
    }.

node_record_opts(Opts) ->
    %% Stations always emit `kind = station' so subscribers can route
    %% station-presence events on `_mesh.station.*' topics distinct
    %% from `_mesh.daemon.*'. Macula 3.10.1+.
    Base = #{ttl_ms => maps:get(ttl_ms, Opts, ?DEFAULT_TTL_MS),
             kind   => <<"station">>},
    %% Macula 3.4+ accepts hostname/endpoint/city/country/lat/lng on
    %% node_record so the realm dashboard can render the station
    %% without a side-channel topology poll.
    Optional = [display_name, caps_hint, station_id,
                hostname, endpoint, city, country, lat, lng],
    add_optional(Optional, Opts, Base).

add_optional([], _Src, Acc) -> Acc;
add_optional([K | Rest], Src, Acc) ->
    add_optional(Rest, Src, maybe_put(K, Src, Acc)).

maybe_put(K, Src, Acc) ->
    case maps:find(K, Src) of
        {ok, V} -> Acc#{K => V};
        error   -> Acc
    end.

set_logger_identity(#{identity_key := Key}) ->
    logger:set_process_metadata(#{identity_id => Key});
set_logger_identity(_) ->
    ok.

handle_call(_Msg, _From, S) -> {reply, {error, unknown_call}, S}.
handle_cast(_Msg, S)        -> {noreply, S}.

handle_info({refresh, Ref}, #state{timer_ref = Ref} = S) ->
    publish_node_record(S),
    {noreply, schedule_refresh(S)};
handle_info(_, S) ->
    {noreply, S}.

terminate(Reason, S) ->
    publish_tombstone_if_graceful(Reason, S),
    ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%====================================================================
%% Publish + tombstone
%%====================================================================

publish_node_record(#state{dht = Dht, identity = Kp,
                           record_opts = RecOpts,
                           realms = Realms,
                           capabilities = Caps,
                           peer_observer = ObsPid,
                           overlay_seeder = SeedPid} = _S) ->
    Pub      = macula_identity:public(Kp),
    %% Merge the live overlay-peer pubkey list from `peer_observer'
    %% into the record opts on every refresh. The set may have
    %% changed between ticks (peers up/down), so we re-read each
    %% time rather than caching at boot.
    OptsWithPeers = with_current_peers(ObsPid, Dht, SeedPid, RecOpts),
    Unsigned = macula_record:node_record(Pub, Realms, Caps, OptsWithPeers),
    Signed   = macula_record:sign(Unsigned, Kp),
    ok = hecate_dht:put_record(Dht, Signed),
    ok.

with_current_peers(undefined, _Dht, SeedPid, RecOpts) ->
    %% No peer_observer (test harness path) — fall back to the seeder.
    add_seeder_hosts(SeedPid, RecOpts, []);
with_current_peers(ObsPid, Dht, SeedPid, RecOpts) ->
    %% peer_observer:peers/1 returns `[{pid(), pubkey()}]' for every
    %% QUIC peer this identity has. Filter to kind=station via local
    %% DHT lookup. The realm topology renderer matches edges by
    %% HOSTNAME, so we resolve each peer's hostname from the same
    %% DHT lookup and emit a comma-separated list in caps_hint.
    %%
    %% Outbound peers seeded by `hecate_station_overlay_seeder' may
    %% not have replicated their node_record into our local DHT yet,
    %% so the seeder also exposes its dialled hostnames directly —
    %% we union both lists into the announce payload.
    Pubkeys = [PK || {_Pid, PK} <- safe_peers(ObsPid)],
    DhtHosts =
        [Host
         || PK <- Pubkeys,
            {ok, Host} <- [station_peer_hostname(Dht, PK)]],
    StationPubkeys =
        [PK
         || PK <- Pubkeys,
            {ok, _} <- [station_peer_hostname(Dht, PK)]],
    RecOpts1 = RecOpts#{peers => StationPubkeys},
    add_seeder_hosts(SeedPid, RecOpts1, DhtHosts).

add_seeder_hosts(SeedPid, RecOpts, DhtHosts) ->
    SeederHosts = seeder_hostnames(SeedPid),
    AllHosts    = lists:usort(DhtHosts ++ SeederHosts),
    HostsCsv    = iolist_to_binary(lists:join($,, AllHosts)),
    case HostsCsv of
        <<>> -> RecOpts;
        _    -> RecOpts#{caps_hint => <<"peers=", HostsCsv/binary>>}
    end.

seeder_hostnames(undefined) -> [];
seeder_hostnames(SeedPid)   ->
    try hecate_station_overlay_seeder:connected_hostnames(SeedPid)
    catch _:_ -> []
    end.

%% Returns `{ok, Hostname}' iff the peer's local DHT record exists,
%% has `kind=station', and carries a non-empty hostname. Anything
%% else falls through to `error', filtering the peer out.
station_peer_hostname(Dht, Pubkey) ->
    classify_peer_record(hecate_dht:find_local_record(Dht, Pubkey)).

classify_peer_record([Record | _]) when is_map(Record) ->
    Payload = macula_record:payload(Record),
    on_peer_kind(payload_kind_is_station(Payload), Payload);
classify_peer_record(_) ->
    error.

on_peer_kind(true, Payload) ->
    case payload_hostname(Payload) of
        Bin when is_binary(Bin), byte_size(Bin) > 0 -> {ok, Bin};
        _ -> error
    end;
on_peer_kind(false, _Payload) ->
    error.

%% Mirrors `hecate_station_fact_publisher:payload_kind/1' across the
%% canonical CBOR shape (`{text, K} => {text, V}') and the wire-decoded
%% atom-key shape (`from_wire_envelope/1' atomizes recognised keys).
payload_kind_is_station(#{{text, <<"kind">>} := {text, <<"station">>}}) -> true;
payload_kind_is_station(#{{text, <<"kind">>} := <<"station">>})         -> true;
payload_kind_is_station(#{<<"kind">>          := {text, <<"station">>}}) -> true;
payload_kind_is_station(#{<<"kind">>          := <<"station">>})         -> true;
payload_kind_is_station(#{kind                := {text, <<"station">>}}) -> true;
payload_kind_is_station(#{kind                := <<"station">>})         -> true;
payload_kind_is_station(#{kind                := station})               -> true;
payload_kind_is_station(_)                                               -> false.

payload_hostname(#{{text, <<"hostname">>} := {text, H}}) when is_binary(H) -> H;
payload_hostname(#{{text, <<"hostname">>} := H})         when is_binary(H) -> H;
payload_hostname(#{<<"hostname">>          := {text, H}}) when is_binary(H) -> H;
payload_hostname(#{<<"hostname">>          := H})         when is_binary(H) -> H;
payload_hostname(#{hostname                := {text, H}}) when is_binary(H) -> H;
payload_hostname(#{hostname                := H})         when is_binary(H) -> H;
payload_hostname(_)                                                          -> undefined.

%% Defensive call against the observer — if it's down or busy, fall
%% back to an empty peer list rather than crashing the whole announcer
%% (which would kill the entire identity sub-tree). Logged at info so
%% intermittent observer hangs are visible without spamming.
safe_peers(ObsPid) ->
    try hecate_station_peer_observer:peers(ObsPid)
    catch _:Reason ->
        logger:info(
          "[announcer] peer_observer query failed: ~p — publishing without peers",
          [Reason]),
        []
    end.

%% Reasons that mean "we're going away on purpose" — tombstone the
%% node_record so peers can clear their caches without waiting for
%% TTL. Crashes (`{error, _}', `killed', etc.) intentionally skip
%% the tombstone — letting TTL handle abnormal exits is safer than
%% having a half-broken process publish a confusing tombstone.
publish_tombstone_if_graceful(shutdown, S) ->
    publish_tombstone(S, shutdown);
publish_tombstone_if_graceful({shutdown, _}, S) ->
    publish_tombstone(S, shutdown);
publish_tombstone_if_graceful(normal, S) ->
    publish_tombstone(S, normal);
publish_tombstone_if_graceful(_, _S) ->
    ok.

publish_tombstone(#state{dht = Dht, identity = Kp}, Reason) ->
    Pub      = macula_identity:public(Kp),
    Unsigned = macula_record:tombstone(Pub, _NodeRecordType = 16#01,
                                        Reason),
    Signed   = macula_record:sign(Unsigned, Kp),
    _ = catch hecate_dht:put_record(Dht, Signed),
    ok.

%%====================================================================
%% Refresh timer
%%====================================================================

schedule_refresh(#state{refresh_ms = Ms} = S) ->
    Ref = make_ref(),
    erlang:send_after(Ms, self(), {refresh, Ref}),
    S#state{timer_ref = Ref}.
