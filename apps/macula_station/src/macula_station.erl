%% @doc Hecate Station — public API facade.
%%
%% Phase 1: a station is a `macula_station_server' gen_server linked to
%% the caller. Multiple stations can run in one BEAM VM (used by the
%% walking-skeleton CT suite).
%%
%% Production deployment will wire a single station via the application's
%% supervision tree from `sys.config'; that path lands in Phase 8.
-module(macula_station).

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
    wait_until_ready/1,
    readiness/0,
    diagnostics/0,
    connect_to/1,
    cache/0,
    rebootstrap/0,
    peering_redundancy/0,
    admin/0,
    admin_addr/0,
    shutdown/0,
    shutdown/1,
    prepare_shutdown/1,
    current_identity/0,
    tombstone_type/0,
    %% Internal — called by `macula_station_app' during boot/teardown.
    remember_dial_opts/1,
    forget_dial_opts/0,
    %% Internal — called by `macula_station_dht_dialer' to build a base
    %% dial template with the station's own identity, replacing
    %% `controlling_pid' with its own pid before actually dialling.
    dial_opts/0
]).

-ifdef(TEST).
%% Exports for unit tests — pure opts composition.
-export([compose_dial/2]).
-endif.

-spec start_link(macula_station_config:opts()) -> {ok, pid()} | {error, term()}.
start_link(Spec) ->
    macula_station_server:start_link(Spec).

-spec stop(pid()) -> {ok, [macula_record:m_record()]}.
stop(Pid) ->
    macula_station_server:stop(Pid).

-spec stop(pid(), atom()) -> {ok, [macula_record:m_record()]}.
stop(Pid, Reason) ->
    macula_station_server:stop(Pid, Reason).

-spec identity(pid()) -> macula_identity:key_pair().
identity(Pid) ->
    macula_station_server:identity(Pid).

-spec listen_addr(pid()) -> {inet:ip_address() | string(), inet:port_number()}.
listen_addr(Pid) ->
    macula_station_server:listen_addr(Pid).

-spec connect_to(pid(), macula_station_server:connect_target()) ->
    {ok, pid()} | {error, term()}.
connect_to(Pid, Target) ->
    macula_station_server:connect_to(Pid, Target).

-spec peers(pid()) -> [{pid(), map()}].
peers(Pid) ->
    macula_station_server:peers(Pid).

-spec tombstones(pid()) -> [macula_record:m_record()].
tombstones(Pid) ->
    macula_station_server:tombstones(Pid).

-spec swim_members(pid()) -> [macula_swim:member()].
swim_members(Pid) ->
    macula_station_server:swim_members(Pid).

%% Was a hardcoded literal ("0.1.0-phase1", every build, forever) --
%% same fix and same reasoning as `macula_station_announcer:station_version/0'.
-spec version() -> binary().
version() ->
    case os:getenv("MACULA_STATION_GIT_SHA") of
        false -> <<"dev">>;
        Sha    -> list_to_binary(Sha)
    end.

%%------------------------------------------------------------------
%% Sup-driven runtime accessors
%%------------------------------------------------------------------

%% @doc Pid of the station's supervised DHT, if the app is booted.
-spec dht() -> {ok, pid()} | {error, not_started}.
dht() -> resolve(macula_dht).

%% @doc Pid of the station's supervised SWIM, if the app is booted.
-spec swim() -> {ok, pid()} | {error, not_started}.
swim() -> resolve(macula_swim).

%% @doc Pid of the station's peer observer, if the app is booted.
-spec observer() -> {ok, pid()} | {error, not_started}.
observer() -> resolve(macula_station_peer_observer).

%% @doc Pid of the station's QUIC listener, if the app is booted.
-spec listener() -> {ok, pid()} | {error, not_started}.
listener() -> resolve(macula_station_listener).

%% @doc Pid of the routing-table cache gen_server (if configured).
-spec cache() -> {ok, pid()} | {error, not_started}.
cache() -> resolve(macula_station_cache).

%% @doc Pid of the re-bootstrap watchdog (if configured).
-spec rebootstrap() -> {ok, pid()} | {error, not_started}.
rebootstrap() -> resolve(macula_station_rebootstrap).

-spec peering_redundancy() -> {ok, pid()} | {error, not_started}.
peering_redundancy() -> resolve(macula_station_peering_redundancy).

%% @doc Pid of the admin HTTP listener (if configured).
-spec admin() -> {ok, pid()} | {error, not_started}.
admin() -> resolve(macula_station_admin).

%% @doc The actual port the admin listener is bound to.
-spec admin_addr() -> {ok, inet:port_number()} | {error, not_started}.
admin_addr() ->
    admin_port_of(admin()).

admin_port_of({ok, Pid})    -> {ok, macula_station_admin:listen_port(Pid)};
admin_port_of(E)            -> E.

%%------------------------------------------------------------------
%% Graceful shutdown
%%
%% Publishes a tombstone for the station's own `node_record' into
%% the local DHT (and, best-effort, to peer DHTs), flushes the
%% routing-table cache to disk, and tears down the sup tree. Exits
%% the application cleanly — a subsequent `whereis(macula_station_sup)'
%% returns `undefined'.
%%
%% Crashes (unclean exits) skip this path; the tombstone's TTL + the
%% abandoned-record expiry in Part 4 §11 handle that case instead.
%%------------------------------------------------------------------

%% The record type tombstoned by `shutdown/0,1'. A station's
%% `node_record' lives under type tag `0x01' (see `macula_record'
%% docs); the tombstone carries that tag so peers mark the right
%% record superseded.
-define(NODE_RECORD_TYPE, 16#01).

%% Key under which `macula_station_app' caches the outbound-dial
%% template at boot. Defined up front so graceful shutdown below +
%% `connect_to/1' further down can both refer to it.
-define(DIAL_KEY, {macula_station, dial_opts}).

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
%% `macula_station_app:prep_stop/1' calls: when the operator invokes
%% `application:stop(macula_station)', the OTP application master
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
    ok   = macula_dht:put_record(Dht, Tomb),
    %% Best-effort replicate to peers in a spawned helper so the
    %% shutdown hot path never blocks on QUIC liveness. If the
    %% cascade seeded the DHT with unreachable endpoints (the
    %% common case when a station goes down while its peers are
    %% still on the same partition) the store round would
    %% otherwise pin shutdown to its full timeout.
    _ = spawn(fun() ->
            macula_dht:store(Dht, Tomb,
                             #{overall_timeout_ms => 2_000,
                               store_timeout_ms   => 500})
        end),
    ok.

build_tombstone(Kp, Reason) ->
    Pub      = macula_identity:public(Kp),
    Unsigned = macula_record:tombstone(Pub, ?NODE_RECORD_TYPE, Reason),
    macula_record:sign(Unsigned, Kp).

flush_cache() ->
    flush_cache_if_running(cache()).

flush_cache_if_running({ok, Pid}) ->
    _ = macula_station_cache:flush(Pid),
    ok;
flush_cache_if_running(_) ->
    ok.

teardown_sup() ->
    teardown_sup_pid(whereis(macula_station_sup)).

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
%% pipeline; callers outside `macula_station_app' use this accessor
%% rather than re-reading the file.
-spec current_identity() -> {ok, macula_identity:key_pair()} | {error, not_started}.
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
-spec listen_addr() -> macula_station_listener:listen_addr()
                     | {error, not_started}.
listen_addr() ->
    listen_addr_of(listener()).

listen_addr_of({ok, Pid}) -> macula_station_listener:listen_addr(Pid);
listen_addr_of(Err)       -> Err.

%% @doc Block until the station is fully operational (sup alive,
%% listener bound, DHT/SWIM/observer registered) or until `Timeout'
%% expires. On success, returns `{ok, ListenAddr}' atomically — the
%% returned address is the SAME value observed during the readiness
%% check, so callers do not race against a station that went down
%% in the microseconds between "ready" and a follow-up `listen_addr/0'
%% call. On timeout, returns `{error, #{reason := timeout, last_state
%% := Snapshot}}' with a rich diagnostic so the caller knows WHY
%% readiness wasn't achieved (which child is missing, which app is
%% running, last known state of every supervised pid).
%%
%% Polls every 100ms. Used by integration tests to know when to start
%% issuing operations against a freshly-spawned station; production
%% code typically does not need this — proper supervision drives
%% readiness through the boot pipeline.
-spec wait_until_ready(timeout()) ->
    {ok, macula_station_listener:listen_addr()} | {error, map()}.
wait_until_ready(Timeout) when is_integer(Timeout), Timeout >= 0 ->
    wait_until_ready_loop(Timeout).

wait_until_ready_loop(Remaining) when Remaining =< 0 ->
    {error, #{reason => timeout, last_state => diagnostics()}};
wait_until_ready_loop(Remaining) ->
    case readiness() of
        {ready, ListenAddr} -> {ok, ListenAddr};
        _NotReady ->
            timer:sleep(100),
            wait_until_ready_loop(Remaining - 100)
    end.

%% @doc One-shot readiness probe. Returns `{ready, ListenAddr}' if
%% every critical subsystem is alive (sup, listener bound, DHT, SWIM,
%% observer); returns `{not_ready, Snapshot}' otherwise. Cheap (no
%% sleeps); the snapshot is suitable for a `/readyz' endpoint or a
%% poll loop. Returning the bound address atomically with the ready
%% verdict closes the race that otherwise opens between a
%% `readiness/0' check and a follow-up `listen_addr/0' call.
-spec readiness() ->
    {ready, macula_station_listener:listen_addr()} | {not_ready, map()}.
readiness() ->
    readiness_for(critical_subsystems_alive(), listen_addr()).

readiness_for(true, {Host, Port} = Addr)
  when is_integer(Port), Port > 0,
       (is_tuple(Host) orelse is_list(Host)) ->
    {ready, Addr};
readiness_for(_, _) ->
    {not_ready, diagnostics()}.

critical_subsystems_alive() ->
    is_ok(listener()) andalso is_ok(dht())
        andalso is_ok(swim()) andalso is_ok(observer()).

is_ok({ok, _}) -> true;
is_ok(_)       -> false.

%% @doc Snapshot of the station's runtime state. Used by
%% `wait_until_ready/1' for timeout diagnostics; also useful for
%% admin tooling and CI integration tests.
-spec diagnostics() -> map().
diagnostics() ->
    #{
        sup_alive       => is_pid_alive(whereis(macula_station_sup)),
        sup_children    => safe_which_children(macula_station_sup),
        listener        => listener(),
        listen_addr     => listen_addr(),
        dht             => dht(),
        swim            => swim(),
        observer        => observer(),
        running_apps    => [A || {A, _, _} <- application:which_applications()]
    }.

is_pid_alive(undefined)              -> false;
is_pid_alive(Pid) when is_pid(Pid)   -> is_process_alive(Pid).

safe_which_children(Name) ->
    try supervisor:which_children(Name)
    catch _:_ -> not_started
    end.

%% @doc Dial a peer. Handshake events (connected / frame / disconnected)
%% flow to the station's observer. Returns the peering worker pid on
%% success; the caller does not usually need it — SWIM and the DHT will
%% learn about the peer automatically once the handshake completes.
-spec connect_to(macula_station_server:connect_target()) ->
    {ok, pid()} | {error, term()}.
connect_to(Target) ->
    dial(dial_opts(), Target).

dial({ok, Opts}, Target) ->
    macula_peering:connect(Opts#{target => Target});
dial({error, _} = E, _Target) ->
    E.

%% @doc Base dial opts (identity, capabilities, DHT/pubsub fast-path
%% recipients) with `controlling_pid' set to this station's own observer.
%% `connect_to/1' uses this as-is; `macula_station_dht_dialer' overrides
%% `controlling_pid' with its own pid before dialling, so it — not the
%% observer — sees the `connected' event first and can trust-check it.
-spec dial_opts() -> {ok, macula_peering:opts()} | {error, not_started}.
dial_opts() ->
    compose_dial(observer(), persistent_term:get(?DIAL_KEY, undefined)).

compose_dial({ok, Observer}, #{identity := _} = Template) ->
    Opts0 = Template#{
        role            => client,
        controlling_pid => Observer
    },
    %% See macula_station_listener:peering_opts/1 for the rationale —
    %% station→station outbound dials use the same direct-to-DHT /
    %% direct-to-pubsub fast paths as the accept side, and pass the
    %% recipients as REGISTERED NAMES so a recipient restart does not
    %% strand the connection on a dead pid.
    Opts1 = Opts0#{dht_recipient    => macula_dht,
                   pubsub_recipient => macula_station_route_pubsub_frames},
    {ok, Opts1};
compose_dial(_ObserverResult, _Template) ->
    {error, not_started}.

resolve(Name) ->
    deliver(whereis(Name)).

deliver(undefined) -> {error, not_started};
deliver(Pid) when is_pid(Pid) -> {ok, Pid}.

%% @doc Internal — called by `macula_station_app:start/2' after the
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
