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
    %% Helpers (real working code, used by future spawn_cluster):
    allocate_data_dir/1,
    new_handle/5,

    %% Type accessors:
    peer_node/1,
    pubkey/1,
    listen_addr/1,
    data_dir/1,
    peer_pid/1
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
