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
    OptsWithPeers = with_current_peers(ObsPid, Dht, SeedPid, RecOpts),
    Unsigned0 = macula_record:node_record(Pub, Realms, Caps, OptsWithPeers),
    %% Inject the V1-style rich identity metadata into the payload —
    %% macula 3.12's node_record/4 only emits the fields it knows
    %% about (kind/hostname/endpoint/city/country/lat/lng), so any
    %% extras must be appended directly. Lights up the realm detail
    %% panel (version / OTP / RAM / CPU cores / bind addr / etc).
    Unsigned  = inject_identity_metadata(Unsigned0, OptsWithPeers),
    Signed    = macula_record:sign(Unsigned, Kp),
    ok = hecate_dht:put_record(Dht, Signed),
    ok.

%% Stamp runtime + station identity metadata onto the announce
%% payload as canonical CBOR text fields. Mirrors V1
%% macula_relay_identity's PrimaryIdentity map.
inject_identity_metadata(#{payload := Payload0} = Record, RecOpts) ->
    Extras = identity_metadata(RecOpts),
    Payload = maps:fold(
                fun(K, V, Acc) -> Acc#{ {text, K} => {text, V} } end,
                Payload0, Extras),
    Record#{payload => Payload}.

identity_metadata(RecOpts) ->
    Mb = system_ram_mb(),
    Cores = integer_to_binary(erlang:system_info(logical_processors)),
    Map0 = #{
        <<"version">>   => station_version(),
        <<"otp">>       => list_to_binary(erlang:system_info(otp_release)),
        <<"ram_mb">>    => integer_to_binary(Mb),
        <<"cpu_cores">> => Cores
    },
    Map1 = add_env(Map0, <<"provider">>,  "MACULA_PROVIDER"),
    add_bind_addr(Map1, RecOpts).

%% Surface the IPv6 listener bind from identity_config as `ipv6' so
%% the realm topology label shows it at street-zoom.
add_bind_addr(Map, RecOpts) ->
    case maps:get(bind, RecOpts, undefined) of
        Bin when is_binary(Bin), byte_size(Bin) > 0 -> Map#{<<"ipv6">> => Bin};
        L when is_list(L), L =/= ""                 -> Map#{<<"ipv6">> => list_to_binary(L)};
        _                                            -> Map
    end.

station_version() ->
    case application:get_key(hecate_station, vsn) of
        {ok, Vsn} -> list_to_binary(Vsn);
        undefined -> <<"dev">>
    end.

%% Best-effort RAM total. /proc/meminfo on Linux; 0 elsewhere.
system_ram_mb() ->
    case os:type() of
        {unix, linux} -> linux_ram_mb();
        _             -> 0
    end.

linux_ram_mb() ->
    case file:read_file("/proc/meminfo") of
        {ok, Data} ->
            case re:run(Data, "MemTotal:\\s+(\\d+)", [{capture, [1], list}]) of
                {match, [KBStr]} -> list_to_integer(KBStr) div 1024;
                nomatch          -> 0
            end;
        {error, _} -> 0
    end.

add_env(Map, Key, EnvVar) ->
    case os:getenv(EnvVar) of
        false -> Map;
        ""    -> Map;
        Val   -> Map#{Key => list_to_binary(Val)}
    end.

with_current_peers(ObsPid, Dht, SeedPid, RecOpts) ->
    %% Three sources for relay-to-relay edges, unioned into caps_hint:
    %%
    %%   1. KLEINBERG OVERLAY (load-bearing) — geographic small-world
    %%      computation against every kind=station record in local DHT.
    %%      Local cluster + K-nearest + log-N long-range bands. Same
    %%      algorithm V1 used; ports `macula_relay_overlay'.
    %%
    %%   2. peer_observer LIVE PEERS (operational signal) — resolved
    %%      to hostnames via local DHT lookup, kind=station only.
    %%      Captures real QUIC peerings the seeder/listener brought up.
    %%
    %%   3. SEEDER DIALED HOSTS (bootstrap fallback) — peers we dialed
    %%      whose own announce hasn't yet replicated into our DHT.
    %%
    %% The realm topology only needs ONE of these to populate edges,
    %% but the union keeps coverage robust against churn + replication
    %% lag.
    KleinbergHosts = kleinberg_hostnames(Dht, RecOpts),
    DhtHosts       = peer_observer_hosts(ObsPid, Dht),
    SeederHosts    = seeder_hostnames(SeedPid),
    StationPubkeys = peer_observer_pubkeys(ObsPid, Dht),

    AllHosts = lists:usort(KleinbergHosts ++ DhtHosts ++ SeederHosts),
    HostsCsv = iolist_to_binary(lists:join($,, AllHosts)),

    RecOpts1 = RecOpts#{peers => StationPubkeys},
    case HostsCsv of
        <<>> -> RecOpts1;
        _    -> RecOpts1#{caps_hint => <<"peers=", HostsCsv/binary>>}
    end.

peer_observer_hosts(undefined, _Dht) -> [];
peer_observer_hosts(ObsPid, Dht) ->
    [Host
     || {_Pid, PK} <- safe_peers(ObsPid),
        {ok, Host} <- [station_peer_hostname(Dht, PK)]].

peer_observer_pubkeys(undefined, _Dht) -> [];
peer_observer_pubkeys(ObsPid, Dht) ->
    [PK
     || {_Pid, PK} <- safe_peers(ObsPid),
        {ok, _} <- [station_peer_hostname(Dht, PK)]].

%% Walk every record in the local DHT, keep kind=station ones, build
%% the {Hostname, Lat, Lng} list and feed it to compute_peers/4.
kleinberg_hostnames(Dht, RecOpts) ->
    case my_geo(RecOpts) of
        {MyHost, MyLat, MyLng} when is_binary(MyHost) ->
            All = local_dht_stations(Dht),
            hecate_station_overlay:compute_peers(MyHost, MyLat, MyLng, All);
        _ ->
            []
    end.

%% Pull our own hostname/lat/lng out of `RecOpts' (set at boot from
%% identity_config). compute_peers/4 needs them to filter self-out and
%% measure haversine distance.
my_geo(RecOpts) ->
    Host = maps:get(hostname, RecOpts, undefined),
    Lat  = to_number(maps:get(lat, RecOpts, undefined)),
    Lng  = to_number(maps:get(lng, RecOpts, undefined)),
    {Host, Lat, Lng}.

to_number(N) when is_number(N) -> float(N);
to_number(_)                   -> undefined.

%% Walk every local DHT record once, project into the
%% `{Hostname, Lat, Lng}' shape compute_peers/4 expects. Records
%% without geo (lat/lng nil or 0) and non-station kinds are dropped.
local_dht_stations(Dht) ->
    Records = try hecate_dht:list_records(Dht)
              catch _:_ -> []
              end,
    lists:filtermap(fun project_station/1, Records).

project_station(Record) when is_map(Record) ->
    Payload = macula_record:payload(Record),
    project_station(payload_kind_is_station(Payload), Payload);
project_station(_) -> false.

project_station(false, _Payload) -> false;
project_station(true, Payload) ->
    case payload_hostname(Payload) of
        Host when is_binary(Host), byte_size(Host) > 0 ->
            project_station_geo(Host, payload_geo(Payload));
        _ -> false
    end.

project_station_geo(Host, {Lat, Lng}) when is_number(Lat), is_number(Lng) ->
    {true, {Host, float(Lat), float(Lng)}};
project_station_geo(_Host, _) -> false.

%% Geo coordinates travel as text strings (`{text, "50.123"}') in the
%% canonical CBOR shape — float canonicalisation is fragile across
%% language impls. Try every representation we might see.
payload_geo(P) ->
    {payload_lat(P), payload_lng(P)}.

payload_lat(P) -> read_geo_field(P, <<"lat">>, lat).
payload_lng(P) -> read_geo_field(P, <<"lng">>, lng).

read_geo_field(P, TextKey, AtomKey) ->
    parse_geo(geo_lookup(P, TextKey, AtomKey)).

geo_lookup(P, TextKey, AtomKey) ->
    case maps:find({text, TextKey}, P) of
        {ok, V} -> V;
        error   ->
            case maps:find(TextKey, P) of
                {ok, V} -> V;
                error   -> maps:get(AtomKey, P, undefined)
            end
    end.

parse_geo({text, Bin}) when is_binary(Bin) -> parse_geo(Bin);
parse_geo(Bin) when is_binary(Bin) ->
    case binary:split(Bin, <<".">>) of
        [_]    -> parse_int(Bin);
        [_, _] -> parse_float(Bin)
    end;
parse_geo(N) when is_number(N) -> float(N);
parse_geo(_) -> undefined.

parse_float(Bin) ->
    try binary_to_float(Bin)
    catch error:badarg -> undefined
    end.

parse_int(Bin) ->
    try float(binary_to_integer(Bin))
    catch error:badarg -> undefined
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

%% Mirrors `hecate_station_record_fanout:payload_kind/1' across the
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
