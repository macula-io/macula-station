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
    cache/0,
    rebootstrap/0,
    admin/0,
    admin_addr/0,
    shutdown/0,
    shutdown/1,
    prepare_shutdown/1,
    current_identity/0,
    tombstone_type/0,
    %% Internal — called by `hecate_station_app' during boot/teardown.
    remember_dial_opts/1,
    forget_dial_opts/0
]).

-spec start_link(hecate_station_config:opts()) -> {ok, pid()} | {error, term()}.
start_link(Spec) ->
    hecate_station_server:start_link(Spec).

-spec stop(pid()) -> {ok, [hecate_record:record()]}.
stop(Pid) ->
    hecate_station_server:stop(Pid).

-spec stop(pid(), atom()) -> {ok, [hecate_record:record()]}.
stop(Pid, Reason) ->
    hecate_station_server:stop(Pid, Reason).

-spec identity(pid()) -> hecate_identity:key_pair().
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

-spec tombstones(pid()) -> [hecate_record:record()].
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

%% @doc Pid of the routing-table cache gen_server (if configured).
-spec cache() -> {ok, pid()} | {error, not_started}.
cache() -> resolve(hecate_station_cache).

%% @doc Pid of the re-bootstrap watchdog (if configured).
-spec rebootstrap() -> {ok, pid()} | {error, not_started}.
rebootstrap() -> resolve(hecate_station_rebootstrap).

%% @doc Pid of the admin HTTP listener (if configured).
-spec admin() -> {ok, pid()} | {error, not_started}.
admin() -> resolve(hecate_station_admin).

%% @doc The actual port the admin listener is bound to.
-spec admin_addr() -> {ok, inet:port_number()} | {error, not_started}.
admin_addr() ->
    admin_port_of(admin()).

admin_port_of({ok, Pid})    -> {ok, hecate_station_admin:listen_port(Pid)};
admin_port_of(E)            -> E.

%%------------------------------------------------------------------
%% Graceful shutdown
%%
%% Publishes a tombstone for the station's own `node_record' into
%% the local DHT (and, best-effort, to peer DHTs), flushes the
%% routing-table cache to disk, and tears down the sup tree. Exits
%% the application cleanly — a subsequent `whereis(hecate_station_sup)'
%% returns `undefined'.
%%
%% Crashes (unclean exits) skip this path; the tombstone's TTL + the
%% abandoned-record expiry in Part 4 §11 handle that case instead.
%%------------------------------------------------------------------

%% The record type tombstoned by `shutdown/0,1'. A station's
%% `node_record' lives under type tag `0x01' (see `hecate_record'
%% docs); the tombstone carries that tag so peers mark the right
%% record superseded.
-define(NODE_RECORD_TYPE, 16#01).

%% Key under which `hecate_station_app' caches the outbound-dial
%% template at boot. Defined up front so graceful shutdown below +
%% `connect_to/1' further down can both refer to it.
-define(DIAL_KEY, {hecate_station, dial_opts}).

%% @doc Graceful shutdown with the default reason `operator_stop'.
-spec shutdown() -> ok | {error, not_started}.
shutdown() ->
    shutdown(operator_stop).

%% @doc Graceful shutdown with an operator-specified reason.
%% The reason is carried on the tombstone so peers can distinguish
%% `retired' (deliberate) from `operator_stop' (routine) etc.
-spec shutdown(atom()) -> ok | {error, not_started}.
shutdown(Reason) when is_atom(Reason) ->
    finish_shutdown(prepare_shutdown(Reason)).

finish_shutdown(ok) ->
    _ = teardown_sup(),
    ok;
finish_shutdown({error, _} = E) ->
    E.

%% @doc Operator-safe pre-shutdown — publish tombstone + flush cache,
%% but LEAVE the sup tree alone. This is what
%% `hecate_station_app:prep_stop/1' calls: when the operator invokes
%% `application:stop(hecate_station)', the OTP application master
%% terminates the sup on its own, so we only need to do the
%% pre-teardown bookkeeping here.
-spec prepare_shutdown(atom()) -> ok | {error, not_started}.
prepare_shutdown(Reason) when is_atom(Reason) ->
    execute_pre_shutdown(dht(), current_identity(), Reason).

execute_pre_shutdown({ok, Dht}, {ok, Kp}, Reason) ->
    _ = publish_tombstone(Dht, Kp, Reason),
    _ = flush_cache(),
    ok;
execute_pre_shutdown(_DhtResult, _IdResult, _Reason) ->
    {error, not_started}.

publish_tombstone(Dht, Kp, Reason) ->
    Tomb = build_tombstone(Kp, Reason),
    ok   = hecate_dht:put_record(Dht, Tomb),
    %% Best-effort replicate to peers in a spawned helper so the
    %% shutdown hot path never blocks on QUIC liveness. If the
    %% cascade seeded the DHT with unreachable endpoints (the
    %% common case when a station goes down while its peers are
    %% still on the same partition) the store round would
    %% otherwise pin shutdown to its full timeout.
    _ = spawn(fun() ->
            hecate_dht:store(Dht, Tomb,
                             #{overall_timeout_ms => 2_000,
                               store_timeout_ms   => 500})
        end),
    ok.

build_tombstone(Kp, Reason) ->
    Pub      = hecate_identity:public(Kp),
    Unsigned = hecate_record:tombstone(Pub, ?NODE_RECORD_TYPE, Reason),
    hecate_record:sign(Unsigned, Kp).

flush_cache() ->
    flush_cache_if_running(cache()).

flush_cache_if_running({ok, Pid}) ->
    _ = hecate_station_cache:flush(Pid),
    ok;
flush_cache_if_running(_) ->
    ok.

teardown_sup() ->
    teardown_sup_pid(whereis(hecate_station_sup)).

teardown_sup_pid(undefined) ->
    ok;
teardown_sup_pid(Pid) when is_pid(Pid) ->
    Ref = monitor(process, Pid),
    _ = forget_dial_opts(),
    exit(Pid, shutdown),
    await_sup_down(Pid, Ref).

%% Graceful first, brutal fallback. Plan §8.7 says "5 s under normal
%% conditions" — that is the end-to-end budget. If the cascade of
%% children exceeds that window, fall back to `kill' so the caller
%% never hangs longer than the operator expects. The tombstone +
%% cache flush already happened upstream, so a late-stage kill is
%% safe.
await_sup_down(Pid, Ref) ->
    receive {'DOWN', Ref, process, Pid, _} -> ok
    after 3_000 ->
        exit(Pid, kill),
        receive {'DOWN', Ref, process, Pid, _} -> ok
        after 2_000 -> timeout
        end
    end.

%% @doc Returns the station's Ed25519 key pair if the app is
%% booted. The identity is cached in `persistent_term' by the boot
%% pipeline; callers outside `hecate_station_app' use this accessor
%% rather than re-reading the file.
-spec current_identity() -> {ok, hecate_identity:key_pair()} | {error, not_started}.
current_identity() ->
    identity_of(persistent_term:get(?DIAL_KEY, undefined)).

identity_of(#{identity := Kp}) -> {ok, Kp};
identity_of(_)                 -> {error, not_started}.

%% @doc The record type tag used for station node_records (and the
%% tag `shutdown/0,1' tombstones publish).
-spec tombstone_type() -> 16#01.
tombstone_type() -> ?NODE_RECORD_TYPE.

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
    hecate_peering:connect(Opts#{target => Target});
dial({error, _} = E, _Target) ->
    E.

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
-spec remember_dial_opts(#{identity       := hecate_identity:key_pair(),
                           realms         := [hecate_identity:pubkey()],
                           capabilities   := non_neg_integer()}) -> ok.
remember_dial_opts(Template) ->
    persistent_term:put(?DIAL_KEY, Template),
    ok.

-spec forget_dial_opts() -> boolean().
forget_dial_opts() ->
    persistent_term:erase(?DIAL_KEY).
