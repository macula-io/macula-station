%% @doc Multi-process integration test harness for macula_station.
%%
%% Phase 1 (PLAN_PHASE_1_MULTI_PROCESS_CT_HARNESS.md). Spawns isolated
%% BEAM nodes via OTP `peer', each running a full `macula_station'
%% instance with its own QUIC listener bound to ::1 + ephemeral port.
%% Stations communicate over real QUIC; the test driver coordinates via
%% `erpc'. Replaces the in-VM 50-station synthetic-router pattern that
%% allowed the DHT no_transport regression to ship.
%%
%% Step 1 of 11: module exists with a real working helper
%% (`allocate_data_dir/1') and the `station_handle' type plus accessors.
%% `spawn_cluster/2' is introduced in step 2.
-module(macula_station_test_cluster).

-export([
    %% Cluster lifecycle:
    spawn_cluster/2,
    stop_cluster/1,

    %% Inter-station connectivity:
    dial/2,
    wait_for_handshakes/3,

    %% Run a function on the peer node:
    rpc/4,
    rpc/5,

    %% Helpers (real working code, used by spawn_cluster):
    allocate_data_dir/1,
    new_handle/5,

    %% Type accessors:
    peer_node/1,
    pubkey/1,
    listen_addr/1,
    data_dir/1,
    peer_pid/1,

    %% Internal — invoked via peer:call to run on the peer node.
    %% Public so the peer can resolve the function reference.
    on_peer_boot_station/0,
    on_peer_count_peers/0
]).

-export_type([station_handle/0]).

%%------------------------------------------------------------------
%% Types
%%------------------------------------------------------------------

%% A handle to one isolated test station. Constructed by
%% `new_handle/5' (internal); inspected via the accessor functions.
-type station_handle() :: #{
    peer_node    := node(),
    pubkey       := <<_:256>>,
    listen_addr  := {inet:ip6_address(), inet:port_number()},
    data_dir     := file:filename(),
    peer_pid     := pid()
}.

%%------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------

%% @doc Create a unique temporary data directory for one station.
%%
%% Each call produces a fresh path under `BaseDir' (typically
%% `?config(priv_dir, Config)' from a CT testcase). The directory is
%% created on disk; cleanup is the caller's responsibility (typically
%% via `stop_cluster/1' once that exists, or implicit in CT's
%% per-suite priv_dir teardown).
-spec allocate_data_dir(BaseDir :: file:filename()) -> file:filename().
allocate_data_dir(BaseDir) ->
    Suffix = erlang:integer_to_list(erlang:unique_integer([positive])),
    Dir = filename:join(BaseDir, "macula_test_station_" ++ Suffix),
    %% ensure_dir/1 wants a sentinel filename; the dir itself is
    %% the parent of that sentinel.
    ok = filelib:ensure_dir(filename:join(Dir, ".keep")),
    Dir.

%% @doc Construct a station_handle. Called internally by
%% `spawn_cluster/2' (step 2); exposed here so tests can build
%% synthetic handles for accessor coverage in step 1.
-spec new_handle(node(), <<_:256>>,
                 {inet:ip6_address(), inet:port_number()},
                 file:filename(), pid()) -> station_handle().
new_handle(PeerNode, Pubkey, ListenAddr, DataDir, PeerPid)
  when is_atom(PeerNode), is_binary(Pubkey), byte_size(Pubkey) =:= 32,
       is_tuple(ListenAddr), is_pid(PeerPid) ->
    #{
        peer_node   => PeerNode,
        pubkey      => Pubkey,
        listen_addr => ListenAddr,
        data_dir    => DataDir,
        peer_pid    => PeerPid
    }.

%%------------------------------------------------------------------
%% Accessors
%%------------------------------------------------------------------

-spec peer_node(station_handle()) -> node().
peer_node(#{peer_node := N}) -> N.

-spec pubkey(station_handle()) -> <<_:256>>.
pubkey(#{pubkey := K}) -> K.

-spec listen_addr(station_handle()) -> {inet:ip6_address(), inet:port_number()}.
listen_addr(#{listen_addr := A}) -> A.

-spec data_dir(station_handle()) -> file:filename().
data_dir(#{data_dir := D}) -> D.

-spec peer_pid(station_handle()) -> pid().
peer_pid(#{peer_pid := P}) -> P.

%%------------------------------------------------------------------
%% Cluster lifecycle
%%------------------------------------------------------------------

%% @doc Spawn N isolated stations sequentially.
%%
%% Each station gets:
%%   * a fresh Ed25519 keypair (saved to data_dir/identity.key)
%%   * its own temp data_dir under `Opts#{base_dir}` (default /tmp)
%%   * a self-signed cert (openssl on PATH; CI has it)
%%   * a separate BEAM peer node booted via OTP `peer'
%%   * a QUIC listener on ::1 + ephemeral port
%%
%% Sequential boot keeps `peer:start_link' linkage clean (peer is
%% linked to the test-driver process, not to a transient spawn).
%% Boot time is ~1s per station; N=3 runs in ~3s. Parallelism is
%% available as a follow-up if test latency becomes an issue, but
%% requires splitting setup-vs-boot to preserve link semantics.
%%
%% On failure of any station's boot, rolls back all already-spawned
%% stations (peer nodes stopped, data dirs removed) and re-raises.
%%
%% Peers are started without link (deterministic teardown is the
%% caller's responsibility via `stop_cluster/1'). On test crashes,
%% peer leaks recover when the host VM exits; CT's per-suite
%% `priv_dir' cleanup catches the data dirs.
-spec spawn_cluster(N :: pos_integer(), Opts :: map()) -> [station_handle()].
spawn_cluster(N, Opts) when is_integer(N), N >= 1, is_map(Opts) ->
    spawn_cluster_loop(N, 1, Opts, []).

spawn_cluster_loop(N, I, _Opts, Acc) when I > N ->
    lists:reverse(Acc);
spawn_cluster_loop(N, I, Opts, Acc) ->
    try spawn_one_station(I, Opts) of
        Handle -> spawn_cluster_loop(N, I + 1, Opts, [Handle | Acc])
    catch
        Class:Reason:Stack ->
            lists:foreach(fun stop_one/1, Acc),
            erlang:raise(Class, Reason, Stack)
    end.

%% @doc Tear down stations: stop peer nodes, remove data dirs.
%% Idempotent: tolerant of already-dead peers and missing dirs.
-spec stop_cluster([station_handle()]) -> ok.
stop_cluster(Handles) when is_list(Handles) ->
    lists:foreach(fun stop_one/1, Handles),
    ok.

%%------------------------------------------------------------------
%% Inter-station connectivity
%%------------------------------------------------------------------

%% @doc From station `From', open an outbound QUIC connection to
%% station `To'. Synchronous: returns when the CONNECT/HELLO
%% handshake completes (or fails / times out). The handshake is
%% considered complete when `connect_to/1' returns; bidirectional
%% peer registration may follow asynchronously (use
%% `wait_for_handshakes/3' to assert the symmetric peer view).
-spec dial(From :: station_handle(), To :: station_handle()) ->
    ok | {error, term()}.
dial(From, To) ->
    {ToHost, ToPort} = listen_addr(To),
    Target = #{
        host       => host_to_str(ToHost),
        port       => ToPort,
        timeout_ms => 5_000
    },
    dial_result(peer:call(peer_pid(From), macula_station, connect_to, [Target])).

dial_result({ok, _ConnPid}) -> ok;
dial_result({error, _} = E) -> E.

%% @doc Block until station `Handle' has at least `MinPeers' verified
%% peers, or until `TimeoutMs' elapses. Polls
%% `macula_station_peer_links:verified_peers/0' on the peer every
%% 100ms. Returns ok on success; `{error, {timeout, ActualCount}}'
%% on deadline.
-spec wait_for_handshakes(station_handle(), non_neg_integer(),
                          pos_integer()) ->
    ok | {error, {timeout, non_neg_integer()}}.
wait_for_handshakes(Handle, MinPeers, TimeoutMs)
  when is_integer(MinPeers), MinPeers >= 0,
       is_integer(TimeoutMs), TimeoutMs > 0 ->
    wait_for_handshakes_loop(peer_pid(Handle), MinPeers, TimeoutMs).

wait_for_handshakes_loop(PeerPid, _MinPeers, Remaining) when Remaining =< 0 ->
    {error, {timeout, verified_peer_count(PeerPid)}};
wait_for_handshakes_loop(PeerPid, MinPeers, Remaining) ->
    handshake_check(verified_peer_count(PeerPid), MinPeers,
                    PeerPid, Remaining).

handshake_check(N, MinPeers, _PeerPid, _Remaining) when N >= MinPeers ->
    ok;
handshake_check(_N, MinPeers, PeerPid, Remaining) ->
    timer:sleep(100),
    wait_for_handshakes_loop(PeerPid, MinPeers, Remaining - 100).

verified_peer_count(PeerPid) ->
    %% on_peer_count_peers runs on the peer and counts peer_observer's
    %% view (includes both inbound + outbound). peer_links would only
    %% see outbound dials, missing the dialed-to side of a handshake.
    peer:call(PeerPid, ?MODULE, on_peer_count_peers, []).

%%------------------------------------------------------------------
%% RPC into the peer node
%%------------------------------------------------------------------

%% @doc Run M:F(Args) on the peer node, returning the result.
%% Wraps `peer:call/4'. Default 30s timeout (peer:call's own 5s
%% default is too short for any operation that blocks on station
%% subsystems; tests that need longer should use `rpc/5').
-spec rpc(station_handle(), module(), atom(), [term()]) -> term().
rpc(Handle, Module, Function, Args) ->
    rpc(Handle, Module, Function, Args, 30_000).

-spec rpc(station_handle(), module(), atom(), [term()],
          pos_integer()) -> term().
rpc(Handle, Module, Function, Args, TimeoutMs) ->
    peer:call(peer_pid(Handle), Module, Function, Args, TimeoutMs).

%%------------------------------------------------------------------
%% Internal — spawning
%%------------------------------------------------------------------

spawn_one_station(I, Opts) ->
    BaseDir   = maps:get(base_dir, Opts, default_base_dir()),
    DataDir   = allocate_data_dir(BaseDir),
    KeyPair   = macula_identity:generate(),
    KeyFile   = filename:join(DataDir, "identity.key"),
    ok        = macula_identity:save(KeyFile, KeyPair),
    %% Share one self-signed cert across all stations in the test run.
    %% openssl shell-out is the expensive bit (~200ms each); without
    %% sharing, an N-station test pays N × openssl. Cert validity
    %% doesn't matter — TLS is loopback-only and tests don't verify.
    {Cert, K} = shared_test_cert(),
    Port      = free_loopback_port(),
    PeerName  = peer_name(I),
    %% peer:start (NOT peer:start_link). The link variant ties the
    %% peer's lifetime to the calling process; under eunit, the per-
    %% test process exits at test end and the linked peer is killed
    %% mid-cleanup, leaving stdio ports/sockets in a transient state
    %% that races the next test's setup. peer:start gives us
    %% deterministic teardown via stop_cluster/1 and decouples test-
    %% process lifecycle from peer lifecycle.
    {ok, PeerPid, PeerNode} = peer:start(#{
        name      => PeerName,
        args      => code_path_args(),
        connection => standard_io
    }),
    %% Boot the station inside the peer. On failure, tear the peer
    %% down so we don't leak a dangling BEAM.
    try boot_station_on_peer(PeerPid, DataDir, KeyFile, Cert, K, Port) of
        {ok, ListenAddr} ->
            new_handle(PeerNode, macula_identity:public(KeyPair),
                       ListenAddr, DataDir, PeerPid)
    catch
        Class:Reason:Stack ->
            catch peer:stop(PeerPid),
            catch file:del_dir_r(DataDir),
            erlang:raise(Class, Reason, Stack)
    end.

%% Talks to the peer via its controller (standard_io connection),
%% NOT via Erlang distribution. Lets the test driver stay
%% non-distributed (no net_kernel needed; no node-name collisions
%% between parallel test runs).
boot_station_on_peer(PeerPid, DataDir, KeyFile, Cert, KeyP, Port) ->
    %% Mirror the existing in-VM test pattern (macula_station_app_tests):
    %% start the macula SDK only, set env, then call macula_station_app
    %% start/2 directly. ensure_all_started(macula_station) would also
    %% start macula_content which has external boot requirements not
    %% met in the test environment.
    {ok, _} = peer:call(PeerPid, application, ensure_all_started, [macula]),
    Set = fun(App, K, V) ->
        ok = peer:call(PeerPid, application, set_env, [App, K, V])
    end,
    Set(macula_station, data_dir,      DataDir),
    Set(macula_station, identity_file, KeyFile),
    Set(macula_station, bind,          "::1"),
    Set(macula_station, port,          Port),
    Set(macula_station, certfile,      Cert),
    Set(macula_station, keyfile,       KeyP),
    %% Stub bootstrap so cascade satisfies min_peers without needing
    %% a real network. macula_station_stub_tier lives in the same test
    %% dir as this module; it is on the peer's code path because we
    %% pass code:get_path() to the peer.
    StubPeers = peer:call(PeerPid, macula_station_stub_tier, stub_peers, [1]),
    Set(macula_bootstrap, discoverers,
        [{macula_station_stub_tier, #{peers => StubPeers}}]),
    Set(macula_bootstrap, cascade_opts,
        #{min_peers => 1, timeout_ms => 5000}),
    %% Boot via a guardian process on the peer (NOT directly via
    %% peer:call to macula_station_app:start). peer:call routes through
    %% a transient request-handler process; if we called start there,
    %% supervisor:start_link would link macula_station_sup to that
    %% transient process, and the sup would die when the transient
    %% process exited (which it does as soon as peer:call returns its
    %% result). The guardian is a long-lived process registered on the
    %% peer; it owns the link, keeping the sup alive for the test's
    %% duration. stop_cluster/1 lets the peer shut down naturally.
    {ok, _Sup} = peer:call(PeerPid, ?MODULE, on_peer_boot_station, []),
    %% Block until macula_station's full subsystem is operational
    %% (sup + listener bound + DHT + SWIM + observer all alive).
    %% wait_until_ready/1 returns the listen_addr ATOMICALLY with the
    %% ready verdict — no race window between "ready" and a follow-up
    %% `listen_addr/0' call. On timeout, returns rich diagnostics
    %% (sup_children, running_apps, per-subsystem state) so failures
    %% are self-explaining without needing to re-run.
    %% peer:call has its own (5s) default timeout; pass explicit
    %% buffer above the inner wait_until_ready deadline so the outer
    %% RPC doesn't time out before the inner readiness loop does.
    case peer:call(PeerPid, macula_station, wait_until_ready,
                   [120_000], 130_000) of
        {ok, {Host, ActualPort}} ->
            HostTuple = host_to_tuple(Host),
            {ok, {HostTuple, ActualPort}};
        {error, Diagnostics} ->
            error({station_not_ready, Diagnostics})
    end.

%%------------------------------------------------------------------
%% Internal — guardian (runs on the peer)
%%------------------------------------------------------------------

%% @doc Spawn a long-lived guardian process on the peer that calls
%% `macula_station_app:start/2' and then sits in a receive loop.
%% Returns `{ok, Sup}' to the test driver. The guardian holds the
%% link to `macula_station_sup', keeping the supervision tree alive
%% beyond the lifetime of the peer:call request handler that
%% triggered the boot.
%%
%% Called via `peer:call(PeerPid, ?MODULE, on_peer_boot_station, [])'.
%% Public for that resolution; not part of the user-facing API.
on_peer_boot_station() ->
    Caller = self(),
    Pid = spawn(fun() ->
        Result = (catch macula_station_app:start(normal, [])),
        Caller ! {boot_result, self(), Result},
        guardian_loop()
    end),
    receive
        {boot_result, Pid, Result} -> Result
    after 60_000 ->
        catch exit(Pid, kill),
        {error, boot_timeout}
    end.

guardian_loop() ->
    receive
        stop -> ok;
        _Any -> guardian_loop()
    end.

%% @doc Count peers visible to the station's observer (both inbound
%% and outbound connections). Runs on the peer via peer:call.
%% Returns 0 if the observer isn't started yet.
on_peer_count_peers() ->
    peer_count_for(macula_station:observer()).

peer_count_for({ok, ObsPid}) -> length(macula_station_peer_observer:peers(ObsPid));
peer_count_for(_)            -> 0.

%%------------------------------------------------------------------
%% Internal — teardown
%%------------------------------------------------------------------

stop_one(Handle) ->
    catch peer:stop(peer_pid(Handle)),
    catch file:del_dir_r(data_dir(Handle)),
    ok.

%%------------------------------------------------------------------
%% Internal — peer node setup
%%------------------------------------------------------------------

peer_name(I) ->
    Suffix = erlang:integer_to_list(erlang:unique_integer([positive])),
    list_to_atom("macula_test_station_" ++ erlang:integer_to_list(I) ++
                 "_" ++ Suffix).

code_path_args() ->
    %% Each path becomes a `-pa <path>' arg pair. Use flatmap (NOT
    %% lists:flatten) — the latter recurses into strings and produces
    %% a single charlist, which `peer:verify_args/1' rejects with
    %% `{invalid_arg, 45}' (the ASCII for "-" at the head of the
    %% accidentally-flattened "-pa").
    lists:flatmap(fun(P) -> ["-pa", P] end, code:get_path()).

%%------------------------------------------------------------------
%% Internal — port + cert + paths
%%------------------------------------------------------------------

%% Pre-allocate a free UDP port on IPv6 loopback. Has the standard
%% TOCTOU window — between close and rebind, the kernel could reassign
%% the port to another process. For test runs this is rare enough to
%% accept. (Alternative: pass port=0 to the listener and read it back,
%% but the V2 listener config does not currently support port=0.)
%%
%% Bound to ::1 specifically (not the IPv4-mapped any) so the port
%% claim matches the actual listener bind family.
free_loopback_port() ->
    {ok, S}    = gen_udp:open(0, [inet6, {reuseaddr, true}]),
    {ok, Port} = inet:port(S),
    ok         = gen_udp:close(S),
    Port.

generate_test_cert(Dir) ->
    CertPath = filename:join(Dir, "cert.pem"),
    KeyPath  = filename:join(Dir, "key.pem"),
    Cmd = lists:flatten(io_lib:format(
        "openssl req -x509 -newkey rsa:2048 -nodes "
        "-keyout ~s -out ~s -days 1 -subj /CN=localhost 2>&1",
        [KeyPath, CertPath])),
    Out = os:cmd(Cmd),
    case filelib:is_regular(CertPath) of
        true  -> {CertPath, KeyPath};
        false -> error({openssl_failed, Out})
    end.

%% One self-signed cert shared by every station in the test run.
%% Stored under a stable path so a stale cert from a prior crashed
%% run is reused (still valid for 1 day after generation).
shared_test_cert() ->
    case persistent_term:get({?MODULE, shared_cert}, undefined) of
        {Cert, Key} ->
            case filelib:is_regular(Cert) andalso filelib:is_regular(Key) of
                true  -> {Cert, Key};
                false -> generate_and_cache_cert()
            end;
        undefined ->
            generate_and_cache_cert()
    end.

generate_and_cache_cert() ->
    Dir = filename:join(default_base_dir(), "shared_cert"),
    ok  = filelib:ensure_dir(filename:join(Dir, ".keep")),
    Pair = generate_test_cert(Dir),
    persistent_term:put({?MODULE, shared_cert}, Pair),
    Pair.

default_base_dir() ->
    Tmp = case os:getenv("TMPDIR") of
        false -> "/tmp";
        T     -> T
    end,
    filename:join(Tmp, "macula_station_test_cluster").

host_to_tuple(Host) when is_list(Host) ->
    {ok, Tuple} = inet:parse_address(Host),
    Tuple;
host_to_tuple(Tuple) when is_tuple(Tuple) -> Tuple.

host_to_str(Host) when is_list(Host) -> Host;
host_to_str(Tuple) when is_tuple(Tuple) ->
    inet:ntoa(Tuple).
