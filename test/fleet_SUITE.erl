%% @doc Fleet CT — Session 8.8.
%%
%% Spawns N independent BEAM VMs via `peer:start_link/1' and boots
%% one `macula_station' application inside each. Every peer runs the
%% full supervision tree (DHT, SWIM, observer, listener, realms,
%% cache, rebootstrap) on an ephemeral loopback port, with a fresh
%% data_dir and its own Ed25519 identity. Because each BEAM has its
%% own registered-name scope, the single-VM constraints that forced
%% us to defer two-station CT in 8.3/8.4/8.7 do not apply here — we
%% finally get to exercise the sup-driven mode across real network
%% boundaries.
%%
%% This suite is a peer-node stand-in for the beam cluster (§8.8
%% `scripts/fleet-ct.sh'). The scenarios it covers:
%%
%% <ul>
%%   <li><b>cold_boot_and_meet</b> — two peers boot with empty
%%       caches; one dials the other; both DHTs end up with the
%%       other's NodeId (tier=t0) and both SWIMs list each other
%%       as `alive'.</li>
%%   <li><b>kill_detection</b> — after the meet, kill one peer VM
%%       (`peer:stop'); assert the survivor's SWIM marks the dead
%%       node `confirmed_failed' within the plan's 10 s budget.</li>
%%   <li><b>pubsub_purge_after_peer_disconnect</b> — seeds local pubsub
%%       interest on one peer, waits for the peering router to
%%       propagate it to the other as a real wire SUBSCRIBE, kills
%%       the seeding peer's VM outright, and asserts the survivor's
%%       copy is deferred-purged (still present right after the kill,
%%       gone once `?PUBSUB_PURGE_GRACE_MS' elapses and isolation is
%%       re-confirmed) — see
%%       `plans/DESIGN_SUBSCRIPTION_LIFECYCLE_GC.md'.</li>
%% </ul>
%%
%% The bigger 4-node / partition / heal scenarios from the plan §8.8
%% file list live in the operator-facing `scripts/fleet-ct.sh'
%% because they require real beam nodes + iptables; see
%% `PLAN_DEFERRED_WORK.md' for why.
-module(fleet_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    cold_boot_and_meet/1,
    kill_detection/1,
    pubsub_purge_after_peer_disconnect/1
]).

%%==================================================================
%% Common Test plumbing
%%==================================================================

all() ->
    [cold_boot_and_meet, kill_detection, pubsub_purge_after_peer_disconnect].

init_per_suite(Config) ->
    %% `peer:start_link/1' with default connection requires Erlang
    %% distribution on the parent VM. Default `rebar3 ct' is
    %% non-distributed; run via `rebar3 ct --name <name>' (or
    %% `scripts/fleet-ct.sh') to exercise the fleet scenarios.
    case needs_distribution() of
        ok         -> prepare_suite(Config);
        {skip, _} = Skip -> Skip
    end.

needs_distribution() ->
    case erlang:is_alive() of
        true  -> ok;
        false -> {skip, "fleet_SUITE requires distributed node — "
                        "run via `rebar3 ct --name <name>' or "
                        "`scripts/fleet-ct.sh'"}
    end.

prepare_suite(Config) ->
    %% `macula_peering' is a MODULE inside the unified `macula'
    %% application, not an application of its own (the SDK folded its
    %% separate apps into one `macula.app.src' — see macula's own
    %% CHANGELOG.md). This suite predates that consolidation.
    {ok, _} = application:ensure_all_started(macula),
    PrivDir = ?config(priv_dir, Config),
    {Cert, Key} = generate_test_cert(PrivDir),
    %% Tight SWIM timings keep the kill-detection bar inside a CT
    %% run's patience budget.
    [{cert, Cert}, {key, Key},
     {swim_opts, #{period_ms          => 400,
                   ping_timeout_ms    => 200,
                   suspect_timeout_ms => 2_000}}
     | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(Case, Config) ->
    Peers = start_two_peers(Case, Config),
    [{peers, Peers}, {case_name, Case} | Config].

end_per_testcase(_Case, Config) ->
    stop_peers(proplists:get_value(peers, Config, [])),
    ok.

%%==================================================================
%% Tests
%%==================================================================

cold_boot_and_meet(Config) ->
    [P1, P2] = ?config(peers, Config),
    connect_meshed(P1, P2),
    Pub1 = pub(P1),
    Pub2 = pub(P2),
    ok = fleet_chaos:wait_until(fun() ->
             dht_contains(P1, Pub2) andalso
             dht_contains(P2, Pub1) andalso
             swim_alive_remote(P1, Pub2) andalso
             swim_alive_remote(P2, Pub1)
         end, 8_000),
    %% Tier=t0 on both sides — that is the observer's direct-peer
    %% spec from Session 8.3.
    ?assertEqual(t0, dht_tier(P1, Pub2)),
    ?assertEqual(t0, dht_tier(P2, Pub1)),
    ok.

kill_detection(Config) ->
    [P1, P2] = ?config(peers, Config),
    connect_meshed(P1, P2),
    Pub2 = pub(P2),
    ok = fleet_chaos:wait_until(
             fun() -> swim_alive_remote(P1, Pub2) end, 5_000),
    %% Stop peer 2's VM outright — the QUIC connection dies, SWIM
    %% pings stop being answered, and P1's failure detector should
    %% transition alive → suspect → confirmed_failed.
    ok = fleet_chaos:stop_peer(maps:get(ctl, P2)),
    ok = fleet_chaos:wait_until(
             fun() -> remote_member_state(P1, Pub2) =:= confirmed_failed end,
             10_000),
    ok.

%% End-to-end proof of `hecate_pubsub_registry:purge_subscriber/2'
%% (macula 10.11.0) wired into `on_disconnected' — real wire-level
%% propagation across two independent BEAM VMs, not a mocked registry
%% or a single-VM unit test. See
%% `plans/DESIGN_SUBSCRIPTION_LIFECYCLE_GC.md'.
pubsub_purge_after_peer_disconnect(Config) ->
    [P1, P2] = ?config(peers, Config),
    connect_meshed(P1, P2),
    Realm = crypto:strong_rand_bytes(32),
    Topic = <<"e2e.pubsub_purge_test.v1">>,

    %% Seed local interest on P2 — the peering router only propagates
    %% topics it has a local reason to want.
    ok = seed_local_interest(P2, Realm, Topic),

    %% The router ticks every 2s (?TICK_MS in
    %% macula_station_peering_router) — wait for it to propagate P2's
    %% fresh local interest to P1 as a real wire SUBSCRIBE.
    ok = fleet_chaos:wait_until(
             fun() -> topic_present(P1, Realm, Topic) end, 10_000),

    %% Kill P2's VM outright — same mechanism as kill_detection above.
    %% P1 observes a real connection loss, not a simulated one.
    ok = fleet_chaos:stop_peer(maps:get(ctl, P2)),

    %% Immediately after: still present. The purge is deferred
    %% (?PUBSUB_PURGE_GRACE_MS on P1), not synchronous with disconnect
    %% — this is what survives the reconnect race (adversary finding
    %% #2 in the design doc).
    ?assert(topic_present(P1, Realm, Topic)),

    %% Once the grace window elapses and isolation is re-confirmed,
    %% P1's copy is gone.
    ok = fleet_chaos:wait_until(
             fun() -> not topic_present(P1, Realm, Topic) end, 15_000),
    ok.

%%==================================================================
%% Peer lifecycle
%%==================================================================

start_two_peers(Case, Config) ->
    Cert = ?config(cert, Config),
    Key  = ?config(key,  Config),
    SwimOpts = ?config(swim_opts, Config),
    [start_one_peer(Case, "a", Cert, Key, SwimOpts),
     start_one_peer(Case, "b", Cert, Key, SwimOpts)].

start_one_peer(Case, Tag, Cert, Key, SwimOpts) ->
    NodeName = iolist_to_binary(io_lib:format(
        "fleet-~s-~s-~B",
        [atom_to_list(Case), Tag,
         erlang:unique_integer([positive])])),
    %% `peer:start_link/1' without `connection' uses Erlang
    %% distribution — requires net_kernel on the parent, which
    %% `rebar3 ct' sets up. Inherit our code path so the peer sees
    %% every app (including test-only modules like
    %% `macula_station_stub_tier').
    {ok, Ctl, Node} = peer:start_link(#{
        name      => binary_to_atom(NodeName, utf8),
        args      => ["-pa" | code:get_path()],
        wait_boot => 10_000
    }),
    Port    = free_port(),
    DataDir = ensure_data_dir(NodeName),
    ok = boot_station(Node, Port, Cert, Key, DataDir, SwimOpts),
    #{ctl => Ctl, node => Node, port => Port, data_dir => DataDir}.

%% Configure + start the application inside the peer via rpc.
boot_station(Node, Port, Cert, Key, DataDir, SwimOpts) ->
    {ok, _} = rpc_call(Node, application, ensure_all_started,
                       [macula]),
    ok = set_station_env(Node, Port, Cert, Key, DataDir, SwimOpts),
    set_stub_bootstrap_env(Node),
    %% Each peer needs its OWN content store — `macula_content' falls
    %% back to a fixed system path (`/var/lib/hecate/content') when
    %% unset, which two peer VMs on the same machine would collide on
    %% (and which a non-root test run can't create in the first
    %% place). This isn't covered by `set_station_env' because
    %% `macula_content' is a separate application, started
    %% transitively as a `macula_station' dependency below.
    ok = rpc_call(Node, application, set_env,
                  [macula_content, store_dir,
                   filename:join(DataDir, "content")]),
    {ok, _Started} = rpc_call(Node, application, ensure_all_started,
                              [macula_station]),
    ok.

set_station_env(Node, Port, Cert, Key, DataDir, _SwimOpts) ->
    Entries = [
        {data_dir, DataDir},
        {bind, "127.0.0.1"},
        {port, Port},
        {certfile, Cert},
        {keyfile,  Key}
    ],
    [ok = rpc_call(Node, application, set_env,
                   [macula_station, K, V]) || {K, V} <- Entries],
    ok.

set_stub_bootstrap_env(Node) ->
    %% Inject a stub discoverer via application env so the cascade
    %% completes with at least one synthetic peer. The test stub
    %% module needs to be loadable from the peer's code path.
    %%
    %% Env key is `discoverers', not `tiers' — `macula_bootstrap:run/0'
    %% (apps/macula_bootstrap/src/macula_bootstrap.erl) reads
    %% `application:get_env(macula_bootstrap, discoverers, [])`; `tiers`
    %% predates that rename and was silently read as unset, always
    %% landing on `{error, no_tiers}` regardless of what this function
    %% set. The value shape (`discoverer_spec() :: {module(), opts()}`)
    %% is unchanged.
    Stub = macula_station_stub_tier,
    _ = ensure_loaded_on_peer(Node, Stub),
    Peer = Stub:stub_peer(<<123:256>>),
    ok = rpc_call(Node, application, set_env,
        [macula_bootstrap, discoverers, [{Stub, #{peers => [Peer]}}]]),
    ok = rpc_call(Node, application, set_env,
        [macula_bootstrap, cascade_opts,
         #{min_peers => 1, timeout_ms => 2_000}]).

ensure_loaded_on_peer(Node, Mod) ->
    %% -pa inherits the test profile's code paths; the stub is in
    %% apps/macula_station/test, which rebar3 includes in the
    %% test code path, so code:get_path() above covers it. Double
    %% check by forcing a load on the peer.
    rpc_call(Node, code, ensure_loaded, [Mod]).

stop_peers([]) -> ok;
stop_peers([#{ctl := Ctl, data_dir := Dir} | Rest]) ->
    _ = fleet_chaos:stop_peer(Ctl),
    rm_rf(Dir),
    stop_peers(Rest).

%%==================================================================
%% Assertion helpers — everything goes through rpc so the test
%% process can observe peer-node state uniformly.
%%==================================================================

pub(#{node := Node}) ->
    {ok, Kp} = rpc_call(Node, macula_station, current_identity, []),
    rpc_call(Node, macula_identity, public, [Kp]).

connect_meshed(P1, #{node := N2, port := Port1_2}) ->
    %% N2 dials the ephemeral port learned above; the observer on
    %% each side handles the handshake and drives DHT+SWIM.
    #{port := Port1} = P1,
    {ok, _Conn} = rpc_call(N2, macula_station, connect_to,
        [#{host => "127.0.0.1", port => Port1, timeout_ms => 3_000}]),
    _ = Port1_2,
    ok.

dht_contains(#{node := Node}, NodeId) ->
    {ok, Dht} = rpc_call(Node, macula_station, dht, []),
    rpc_call(Node, macula_dht, contains, [Dht, NodeId]).

dht_tier(#{node := Node}, NodeId) ->
    {ok, Dht}  = rpc_call(Node, macula_station, dht, []),
    {ok, Entry} = rpc_call(Node, macula_dht, find, [Dht, NodeId]),
    rpc_call(Node, macula_dht_entry, tier, [Entry]).

swim_alive_remote(#{node := Node}, NodeId) ->
    remote_member_state(#{node => Node}, NodeId) =:= alive.

%% Cross-node version of `fleet_chaos:member_state/2'. The chaos
%% helper is a LOCAL call; fleet tests live across peer nodes so
%% we wrap it in `rpc:call'.
remote_member_state(#{node := Node}, NodeId) ->
    {ok, Swim} = rpc_call(Node, macula_station, swim, []),
    rpc_call(Node, fleet_chaos, member_state, [Swim, NodeId]).

%% Register a realm + subscribe a fresh synthetic identity directly
%% against the peer's own pubsub registry — the same two calls
%% `hecate_pubsub_registry_tests.erl' uses, just across an RPC
%% boundary. This is what gives the peer a real reason for its own
%% `macula_station_peering_router' to propagate the topic outward;
%% the synthetic identity itself is never inspected by the test, only
%% (Realm, Topic)'s presence on the OTHER peer.
seed_local_interest(#{node := Node}, Realm, Topic) ->
    Reg = pubsub_registry_of(Node),
    Kp  = rpc_call(Node, macula_identity, generate, []),
    Sub = rpc_call(Node, macula_identity, public, [Kp]),
    {ok, Server} = rpc_call(Node, hecate_pubsub_registry, register,
                            [Reg, Realm, Kp]),
    ok = rpc_call(Node, hecate_pubsub_server, subscribe,
                  [Server, Topic, Sub]),
    ok.

%% Whether Node's own pubsub registry currently has ANY subscriber for
%% (Realm, Topic) — true both for the seeding peer's own local
%% interest and for a peer that learned it via wire-level SUBSCRIBE
%% propagation.
topic_present(#{node := Node}, Realm, Topic) ->
    Reg = pubsub_registry_of(Node),
    topic_present_for(rpc_call(Node, hecate_pubsub_registry, lookup, [Reg, Realm]),
                      Node, Topic).

topic_present_for({error, not_found}, _Node, _Topic) ->
    false;
topic_present_for({ok, Server}, Node, Topic) ->
    lists:member(Topic, rpc_call(Node, hecate_pubsub_server, topics, [Server])).

%% The registry has no registered name (Phase 2 multi-identity —
%% every identity owns its own anonymous registry pid), so the only
%% way to reach it is through the peer observer's own state, exactly
%% as `macula_station_peer_observer_tests.erl' does locally. Index 5
%% is `pubsub_registry' in `#state{}' — see that module's own record
%% definition; `dht'=2, `swim'=3, `handler_registry'=4 precede it.
pubsub_registry_of(Node) ->
    {ok, ObsPid} = rpc_call(Node, macula_station, observer, []),
    State = rpc_call(Node, sys, get_state, [ObsPid]),
    element(5, State).

%%==================================================================
%% Generic utilities
%%==================================================================

rpc_call(Node, M, F, A) ->
    rpc:call(Node, M, F, A, 20_000).

free_port() ->
    {ok, S} = gen_udp:open(0, [{reuseaddr, true}]),
    {ok, P} = inet:port(S),
    ok = gen_udp:close(S),
    P.

ensure_data_dir(Name) ->
    Dir = filename:join(["/tmp", "macula-station-fleet",
                         binary_to_list(Name)]),
    ok = filelib:ensure_dir(filename:join(Dir, "placeholder")),
    Dir.

rm_rf(Dir) -> _ = os:cmd("rm -rf " ++ Dir), ok.

generate_test_cert(Dir) ->
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Cert = filename:join(Dir, "cert.pem"),
    Key  = filename:join(Dir, "key.pem"),
    Cmd  = lists:flatten(io_lib:format(
        "openssl req -x509 -newkey rsa:2048 -nodes "
        "-keyout ~s -out ~s -days 1 -subj /CN=localhost 2>&1",
        [Key, Cert])),
    Out  = os:cmd(Cmd),
    true = filelib:is_regular(Cert) orelse error({openssl_failed, Out}),
    {Cert, Key}.
