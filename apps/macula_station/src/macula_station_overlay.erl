%%% @doc Geographic small-world overlay (Kleinberg model).
%%%
%%% Computes the peer set for a station identity given the geographic
%%% distribution of all known stations. Ported from V1 `macula_relay'
%%% (`macula_relay_overlay.erl`) — the migration to V2 macula-station
%%% dropped this module, so the realm topology view had no edges to
%%% draw between station identities.
%%%
%%% The peer set has three layers:
%%%   * `local'      — every station within `MACULA_OVERLAY_RADIUS_KM'
%%%                    (default 100km), fully-connected local cluster.
%%%   * `K-nearest'  — `?K_MIN_NEAREST' nearest by haversine distance,
%%%                    so isolated identities still have neighbours.
%%%   * `long-range' — O(log N) Kleinberg-style links picked from
%%%                    distance bands (small-world property: short
%%%                    path lengths across the whole graph).
%%%
%%% The overlay is LOGICAL — `compute_peers/4' returns a list of
%%% hostnames the topology view should render edges to. It does NOT
%%% open any QUIC connections; downstream callers may use the result
%%% to drive outbound dialing if desired (e.g. the seeder), but the
%%% primary consumer is the announcer's `caps_hint' emit so the
%%% realm topology can render the edges immediately.
-module(macula_station_overlay).

-export([compute_peers/4]).

-define(LOCAL_RADIUS_KM, 100).
-define(K_MIN_NEAREST,    3).
-define(EARTH_RADIUS_KM,  6371).

-type station() :: {Hostname :: binary(),
                    Lat      :: float(),
                    Lng      :: float()}.

-export_type([station/0]).

%%====================================================================
%% API
%%====================================================================

%% @doc Compute overlay peers for a station identity.
%% `MyHostname' is the station's own hostname (excluded from the
%% result). `AllStations' is the full list of known stations
%% `[{Hostname, Lat, Lng}]' — typically extracted from the local
%% DHT's `kind=station' node_records.
%%
%% Returns a sorted, deduplicated list of peer hostnames.
-spec compute_peers(binary(), float(), float(), [station()]) -> [binary()].
compute_peers(MyHostname, MyLat, MyLng, AllStations)
  when is_binary(MyHostname) ->
    case is_geolocated(MyLat, MyLng) of
        false -> [];
        true  -> do_compute_peers(MyHostname, MyLat, MyLng, AllStations)
    end.

do_compute_peers(MyHostname, MyLat, MyLng, AllStations) ->
    Others = build_others(MyHostname, MyLat, MyLng, AllStations),
    Radius = env_int("MACULA_OVERLAY_RADIUS_KM", ?LOCAL_RADIUS_KM),
    Local     = select_within_radius(Others, Radius),
    Nearest   = select_nearest(Others, ?K_MIN_NEAREST),
    LongRange = select_long_range(Others, Local ++ Nearest),
    lists:usort(Local ++ Nearest ++ LongRange).

build_others(MyHostname, MyLat, MyLng, AllStations) ->
    [{Host, haversine(MyLat, MyLng, Lat, Lng)}
     || {Host, Lat, Lng} <- AllStations,
        Host =/= MyHostname,
        is_binary(Host),
        is_geolocated(Lat, Lng)].

%%====================================================================
%% Peer selection
%%====================================================================

%% All stations within radius (km) — fully connected local cluster.
select_within_radius(Others, RadiusKm) ->
    [H || {H, D} <- Others, D =< RadiusKm].

%% K nearest by distance — guarantees connectivity for isolated nodes.
select_nearest(Others, K) ->
    Sorted = lists:sort(fun({_, D1}, {_, D2}) -> D1 =< D2 end, Others),
    [H || {H, _} <- lists:sublist(Sorted, K)].

%% O(log N) Kleinberg-style long-range links. Sorts candidates by
%% distance, divides into NumLongRange bands, picks the middle of each
%% band — gives evenly spaced links across the distance spectrum
%% (nearby, medium, far) for the small-world property.
select_long_range(Others, AlreadySelected) ->
    N = length(Others),
    NumLongRange = max(2, round(math:log2(max(2, N)))),
    Candidates =
        [{H, D} || {H, D} <- Others,
                   not lists:member(H, AlreadySelected),
                   D > 0],
    case Candidates of
        [] -> [];
        _  -> pick_long_range_bands(Candidates, NumLongRange)
    end.

pick_long_range_bands(Candidates, NumLongRange) ->
    Sorted   = lists:sort(fun({_, D1}, {_, D2}) -> D1 =< D2 end, Candidates),
    Len      = length(Sorted),
    BandSize = max(1, Len div NumLongRange),
    Indices  = [min(Len, I * BandSize + BandSize div 2)
                || I <- lists:seq(0, NumLongRange - 1)],
    UniqueIndices = lists:usort(Indices),
    [H || Idx <- UniqueIndices,
          Idx >= 1, Idx =< Len,
          {H, _} <- [lists:nth(Idx, Sorted)]].

%%====================================================================
%% Geo helpers
%%====================================================================

is_geolocated(Lat, Lng) when is_number(Lat), is_number(Lng) ->
    not (Lat == 0 andalso Lng == 0);
is_geolocated(_, _) -> false.

haversine(Lat1, Lng1, Lat2, Lng2) ->
    DLat = deg_to_rad(Lat2 - Lat1),
    DLng = deg_to_rad(Lng2 - Lng1),
    A = math:sin(DLat / 2) * math:sin(DLat / 2) +
        math:cos(deg_to_rad(Lat1)) * math:cos(deg_to_rad(Lat2)) *
        math:sin(DLng / 2) * math:sin(DLng / 2),
    C = 2 * math:atan2(math:sqrt(A), math:sqrt(1 - A)),
    ?EARTH_RADIUS_KM * C.

deg_to_rad(Deg) -> Deg * math:pi() / 180.0.

env_int(Key, Default) ->
    case os:getenv(Key) of
        false -> Default;
        ""    -> Default;
        Val   ->
            try list_to_integer(Val)
            catch _:_ -> Default
            end
    end.
