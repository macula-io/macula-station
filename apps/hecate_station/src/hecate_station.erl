%% @doc Hecate Station — public API facade.
%%
%% Phase 1: a station is a `hecate_station_server' gen_server linked to
%% the caller. Multiple stations can run in one BEAM VM (used by the
%% walking-skeleton CT suite).
%%
%% Production deployment will wire a single station via the application's
%% supervision tree from `sys.config'; that path lands in Phase 8.
-module(hecate_station).

-export([
    start_link/1,
    stop/1, stop/2,
    identity/1,
    listen_addr/1,
    connect_to/2,
    peers/1,
    tombstones/1,
    swim_members/1,
    version/0,
    %% Sup-driven runtime accessors (Session 8.2+).
    dht/0,
    swim/0,
    observer/0,
    listener/0,
    listen_addr/0,
    connect_to/1,
    realm_sup/0,
    realms/0,
    realm/1,
    cache/0,
    rebootstrap/0,
    %% Internal — called by `hecate_station_app' during boot/teardown.
    remember_dial_opts/1,
    forget_dial_opts/0
]).

-spec start_link(hecate_station_config:opts()) -> {ok, pid()} | {error, term()}.
start_link(Spec) ->
    hecate_station_server:start_link(Spec).

-spec stop(pid()) -> {ok, [macula_record:record()]}.
stop(Pid) ->
    hecate_station_server:stop(Pid).

-spec stop(pid(), atom()) -> {ok, [macula_record:record()]}.
stop(Pid, Reason) ->
    hecate_station_server:stop(Pid, Reason).

-spec identity(pid()) -> macula_identity:key_pair().
identity(Pid) ->
    hecate_station_server:identity(Pid).

-spec listen_addr(pid()) -> {inet:ip_address() | string(), inet:port_number()}.
listen_addr(Pid) ->
    hecate_station_server:listen_addr(Pid).

-spec connect_to(pid(), hecate_station_server:connect_target()) ->
    {ok, pid()} | {error, term()}.
connect_to(Pid, Target) ->
    hecate_station_server:connect_to(Pid, Target).

-spec peers(pid()) -> [{pid(), map()}].
peers(Pid) ->
    hecate_station_server:peers(Pid).

-spec tombstones(pid()) -> [macula_record:record()].
tombstones(Pid) ->
    hecate_station_server:tombstones(Pid).

-spec swim_members(pid()) -> [hecate_swim:member()].
swim_members(Pid) ->
    hecate_station_server:swim_members(Pid).

-spec version() -> binary().
version() ->
    <<"0.1.0-phase1">>.

%%------------------------------------------------------------------
%% Sup-driven runtime accessors
%%------------------------------------------------------------------

%% @doc Pid of the station's supervised DHT, if the app is booted.
-spec dht() -> {ok, pid()} | {error, not_started}.
dht() -> resolve(hecate_dht).

%% @doc Pid of the station's supervised SWIM, if the app is booted.
-spec swim() -> {ok, pid()} | {error, not_started}.
swim() -> resolve(hecate_swim).

%% @doc Pid of the station's peer observer, if the app is booted.
-spec observer() -> {ok, pid()} | {error, not_started}.
observer() -> resolve(hecate_station_peer_observer).

%% @doc Pid of the station's QUIC listener, if the app is booted.
-spec listener() -> {ok, pid()} | {error, not_started}.
listener() -> resolve(hecate_station_listener).

%% @doc Pid of the realm supervisor, if the app is booted.
-spec realm_sup() -> {ok, pid()} | {error, not_started}.
realm_sup() -> resolve(hecate_station_realm_sup).

%% @doc Every supervised realm pid on this station.
-spec realms() -> [pid()].
realms() ->
    enumerate_realms(realm_sup()).

enumerate_realms({ok, _}) -> hecate_station_realm_sup:children();
enumerate_realms(_)       -> [].

%% @doc Resolve a realm id to the matching realm gen_server pid.
-spec realm(<<_:256>>) -> {ok, pid()} | {error, not_started | not_found}.
realm(RealmId) when is_binary(RealmId), byte_size(RealmId) =:= 32 ->
    lookup_realm(observer(), RealmId).

lookup_realm({ok, Obs}, RealmId) ->
    map_lookup(hecate_station_peer_observer:realm_for(Obs, RealmId));
lookup_realm(_, _) ->
    {error, not_started}.

map_lookup({ok, Pid})    -> {ok, Pid};
map_lookup(error)        -> {error, not_found}.

%% @doc Pid of the routing-table cache gen_server (if configured).
-spec cache() -> {ok, pid()} | {error, not_started}.
cache() -> resolve(hecate_station_cache).

%% @doc Pid of the re-bootstrap watchdog (if configured).
-spec rebootstrap() -> {ok, pid()} | {error, not_started}.
rebootstrap() -> resolve(hecate_station_rebootstrap).

%% @doc `{Bind, Port}' the listener is bound to. Errors if the app is
%% not booted.
-spec listen_addr() -> hecate_station_listener:listen_addr()
                     | {error, not_started}.
listen_addr() ->
    listen_addr_of(listener()).

listen_addr_of({ok, Pid}) -> hecate_station_listener:listen_addr(Pid);
listen_addr_of(Err)       -> Err.

%% @doc Dial a peer. Handshake events (connected / frame / disconnected)
%% flow to the station's observer. Returns the peering worker pid on
%% success; the caller does not usually need it — SWIM and the DHT will
%% learn about the peer automatically once the handshake completes.
-spec connect_to(hecate_station_server:connect_target()) ->
    {ok, pid()} | {error, term()}.
connect_to(Target) ->
    dial(dial_opts(), Target).

dial({ok, Opts}, Target) ->
    macula_peering:connect(Opts#{target => Target});
dial({error, _} = E, _Target) ->
    E.

%% `hecate_station_app' stores the post-boot dial template
%% (identity / realms / capabilities) here so `connect_to/1' can
%% reconstruct peering opts without re-reading the config file.
-define(DIAL_KEY, {hecate_station, dial_opts}).

dial_opts() ->
    compose_dial(observer(), persistent_term:get(?DIAL_KEY, undefined)).

compose_dial({ok, Observer}, #{identity := _} = Template) ->
    {ok, Template#{
        role            => client,
        controlling_pid => Observer
    }};
compose_dial(_ObserverResult, _Template) ->
    {error, not_started}.

resolve(Name) ->
    deliver(whereis(Name)).

deliver(undefined) -> {error, not_started};
deliver(Pid) when is_pid(Pid) -> {ok, Pid}.

%% @doc Internal — called by `hecate_station_app:start/2' after the
%% observer + listener are up. Exposes the dial template to
%% `connect_to/1'. Not exported in the user-facing API.
-spec remember_dial_opts(#{identity       := macula_identity:key_pair(),
                           realms         := [macula_identity:pubkey()],
                           capabilities   := non_neg_integer()}) -> ok.
remember_dial_opts(Template) ->
    persistent_term:put(?DIAL_KEY, Template),
    ok.

-spec forget_dial_opts() -> boolean().
forget_dial_opts() ->
    persistent_term:erase(?DIAL_KEY).
