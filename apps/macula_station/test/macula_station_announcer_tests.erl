%% @doc Eunit suite for the per-identity station announcer.
%%
%% Spins up a real per-identity macula_dht and verifies:
%%   * On init, the announcer puts a signed node_record into the DHT
%%   * The record's `key' is the identity's pubkey, `type' is 0x01,
%%     and `verify/1' succeeds
%%   * On graceful stop, a tombstone (type 0x0C) lands at the same key
%%   * Crash exit (`exit(Pid, kill)') does NOT publish a tombstone
-module(macula_station_announcer_tests).
-include_lib("eunit/include/eunit.hrl").

-define(TYPE_NODE,       16#01).
-define(TYPE_TOMBSTONE,  16#0C).

%%%===================================================================
%%% Fixture
%%%===================================================================

setup() ->
    process_flag(trap_exit, true),
    Kp = macula_identity:generate(),
    {ok, Dht} = macula_dht:start_link(
                  #{self_id => macula_identity:public(Kp)}),
    unlink(Dht),
    #{kp => Kp, dht => Dht}.

cleanup(#{dht := Dht}) ->
    catch macula_dht:stop(Dht),
    drain_exits(),
    ok.

drain_exits() ->
    receive {'EXIT', _, _} -> drain_exits()
    after 0 -> ok end.

start_announcer(#{dht := Dht, kp := Kp}) ->
    {ok, Pid} = macula_station_announcer:start_link(#{
        dht          => Dht,
        identity     => Kp,
        realms       => [],
        capabilities => 16#FF,
        display_name => <<"test-station">>,
        ttl_ms       => 600_000
    }),
    %% Decouple from the test process so its exit doesn't propagate
    %% back as a stray EXIT signal.
    unlink(Pid),
    Pid.

%%%===================================================================
%%% Tests
%%%===================================================================

put_node_record_on_init_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Ctx) ->
        ?_test(begin
            _Pid = start_announcer(Ctx),
            #{dht := Dht, kp := Kp} = Ctx,
            Pub = macula_identity:public(Kp),
            wait_until(fun() ->
                macula_dht:find_local_record(Dht, Pub) =/= []
            end, 1_000),
            [Record] = macula_dht:find_local_record(Dht, Pub),
            ?assertEqual(?TYPE_NODE, macula_record:type(Record)),
            ?assertEqual(Pub, macula_record:key(Record)),
            ?assertMatch({ok, _}, macula_record:verify(Record))
        end)
    end}.

graceful_stop_publishes_tombstone_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Ctx) ->
        ?_test(begin
            Pid = start_announcer(Ctx),
            #{dht := Dht, kp := Kp} = Ctx,
            Pub = macula_identity:public(Kp),
            wait_until(fun() ->
                macula_dht:find_local_record(Dht, Pub) =/= []
            end, 1_000),

            ok = macula_station_announcer:stop(Pid),

            wait_until(fun() ->
                lists:any(fun(R) ->
                    macula_record:type(R) =:= ?TYPE_TOMBSTONE
                end, macula_dht:find_local_record(Dht, Pub))
            end, 1_000),
            Records = macula_dht:find_local_record(Dht, Pub),
            Tombstones = [R || R <- Records,
                               macula_record:type(R) =:= ?TYPE_TOMBSTONE],
            ?assertMatch([_ | _], Tombstones)
        end)
    end}.

peer_observer_pubkeys_land_in_record_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Ctx) ->
        ?_test(begin
            #{dht := Dht, kp := Kp} = Ctx,
            %% Plant station + daemon node_records for the candidate
            %% peers so the announcer's `station_peer_hostname/2'
            %% lookup finds them in the local DHT. Two named stations
            %% + one daemon — daemon must be filtered out, station
            %% hostnames must land in caps_hint as `peers=...'.
            StationKp1 = macula_identity:generate(),
            StationKp2 = macula_identity:generate(),
            DaemonKp   = macula_identity:generate(),
            StationPub1 = macula_identity:public(StationKp1),
            DaemonPub   = macula_identity:public(DaemonKp),
            put_named_station_record(Dht, StationKp1, <<"relay-de-foo.example">>),
            put_named_station_record(Dht, StationKp2, <<"relay-de-bar.example">>),
            put_daemon_record(Dht, DaemonKp),

            ObsPid = fake_peer_observer(
                       [{self(), StationPub1},
                        {self(), DaemonPub},        %% filtered out
                        {self(), macula_identity:public(StationKp2)}]),

            {ok, Pid} = macula_station_announcer:start_link(#{
                dht           => Dht,
                identity      => Kp,
                ttl_ms        => 600_000,
                peer_observer => ObsPid
            }),
            unlink(Pid),
            Pub = macula_identity:public(Kp),
            wait_until(fun() ->
                lists:any(fun(R) ->
                    macula_record:type(R) =:= ?TYPE_NODE
                end, macula_dht:find_local_record(Dht, Pub))
            end, 1_000),
            [Record] = [R || R <- macula_dht:find_local_record(Dht, Pub),
                             macula_record:type(R) =:= ?TYPE_NODE],
            Payload  = macula_record:payload(Record),
            %% Hostnames flow as caps_hint = "peers=h1,h2" (sorted).
            CapsHint = maps:get({text, <<"caps_hint">>}, Payload),
            ?assertEqual({text, <<"peers=relay-de-bar.example,relay-de-foo.example">>},
                         CapsHint),
            catch macula_station_announcer:stop(Pid),
            exit(ObsPid, normal)
        end)
    end}.

crash_exit_does_not_tombstone_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Ctx) ->
        ?_test(begin
            process_flag(trap_exit, true),
            Pid = start_announcer(Ctx),
            #{dht := Dht, kp := Kp} = Ctx,
            Pub = macula_identity:public(Kp),
            wait_until(fun() ->
                macula_dht:find_local_record(Dht, Pub) =/= []
            end, 1_000),

            unlink(Pid),
            exit(Pid, kill),
            Ref = erlang:monitor(process, Pid),
            receive {'DOWN', Ref, process, Pid, _} -> ok
            after 1_000 -> ?assert(false) end,

            %% Settle: tombstone (if we wrongly published one) would
            %% have arrived by now since DHT puts are synchronous.
            timer:sleep(50),
            Records = macula_dht:find_local_record(Dht, Pub),
            Tombstones = [R || R <- Records,
                               macula_record:type(R) =:= ?TYPE_TOMBSTONE],
            ?assertEqual([], Tombstones)
        end)
    end}.

%% A wedged DHT must degrade the announcer, not crash it. Before the 2026-07-25
%% hardening, `publish_node_record' made an uncaught 5s `gen_server:call' to
%% `put_record' straight from init/1 (the exact call that timed out in the
%% 2026-07-24 incident), so a starved DHT crashed the announcer, the supervisor
%% restarted it, and init re-ran into the same wedge — a crash loop that also
%% blocked the supervisor inside each synchronous start.
%%
%% With no hostname or peer_observer in these opts, the enrichment lookups
%% short-circuit without touching the DHT, so this exercises the `put_record'
%% leg specifically — which is both the incident's crash and the one init
%% blocks on. A dead pid raises `noproc' immediately, the same exit class the
%% enrich/put catches handle, without the 5s wait.
survives_wedged_dht_test_() ->
    {timeout, 10, ?_test(begin
        process_flag(trap_exit, true),
        Kp = macula_identity:generate(),
        DeadDht = spawn(fun() -> ok end),
        wait_dead(DeadDht),

        {ok, Pid} = macula_station_announcer:start_link(#{
            dht          => DeadDht,
            identity     => Kp,
            realms       => [],
            capabilities => 16#FF,
            display_name => <<"wedged-dht-station">>,
            ttl_ms       => 600_000
        }),
        unlink(Pid),

        %% init returned a pid (it did not crash on the broken DHT), and the
        %% announcer stays up rather than crash-looping to death. The failed
        %% publish re-arms a 30s retry, well outside this window.
        timer:sleep(100),
        ?assert(is_process_alive(Pid)),

        exit(Pid, kill)
    end)}.

%%%===================================================================
%%% Helpers
%%%===================================================================

wait_dead(P) ->
    Ref = erlang:monitor(process, P),
    receive {'DOWN', Ref, process, P, _} -> ok
    after 1_000 -> ok end.

wait_until(_F, Budget) when Budget =< 0 ->
    erlang:error(wait_until_timeout);
wait_until(F, Budget) ->
    case F() of
        true  -> ok;
        false -> timer:sleep(20), wait_until(F, Budget - 20)
    end.

%% Tiny stand-in for `macula_station_peer_observer'. Replies to the
%% `peers' gen_server call with a canned `[{Pid, Pubkey}]' list — the
%% announcer pulls just the pubkeys, so Pid is irrelevant here.
fake_peer_observer(Peers) ->
    spawn_link(fun() -> fake_peer_observer_loop(Peers) end).

fake_peer_observer_loop(Peers) ->
    receive
        {'$gen_call', From, peers} ->
            gen_server:reply(From, Peers),
            fake_peer_observer_loop(Peers);
        stop ->
            ok
    end.

put_named_station_record(Dht, Kp, Hostname) ->
    Pub      = macula_identity:public(Kp),
    Unsigned = macula_record:node_record(Pub, [], 0,
                                          #{kind => <<"station">>,
                                            hostname => Hostname}),
    Signed   = macula_record:sign(Unsigned, Kp),
    ok = macula_dht:put_record(Dht, Signed).

put_daemon_record(Dht, Kp) ->
    Pub      = macula_identity:public(Kp),
    Unsigned = macula_record:node_record(Pub, [], 0, #{kind => <<"daemon">>}),
    Signed   = macula_record:sign(Unsigned, Kp),
    ok = macula_dht:put_record(Dht, Signed).
