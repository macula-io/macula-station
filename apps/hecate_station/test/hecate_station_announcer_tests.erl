%% @doc Eunit suite for the per-identity station announcer.
%%
%% Spins up a real per-identity hecate_dht and verifies:
%%   * On init, the announcer puts a signed node_record into the DHT
%%   * The record's `key' is the identity's pubkey, `type' is 0x01,
%%     and `verify/1' succeeds
%%   * On graceful stop, a tombstone (type 0x0C) lands at the same key
%%   * Crash exit (`exit(Pid, kill)') does NOT publish a tombstone
-module(hecate_station_announcer_tests).
-include_lib("eunit/include/eunit.hrl").

-define(TYPE_NODE,       16#01).
-define(TYPE_TOMBSTONE,  16#0C).

%%%===================================================================
%%% Fixture
%%%===================================================================

setup() ->
    process_flag(trap_exit, true),
    Kp = macula_identity:generate(),
    {ok, Dht} = hecate_dht:start_link(
                  #{self_id => macula_identity:public(Kp)}),
    unlink(Dht),
    #{kp => Kp, dht => Dht}.

cleanup(#{dht := Dht}) ->
    catch hecate_dht:stop(Dht),
    drain_exits(),
    ok.

drain_exits() ->
    receive {'EXIT', _, _} -> drain_exits()
    after 0 -> ok end.

start_announcer(#{dht := Dht, kp := Kp}) ->
    {ok, Pid} = hecate_station_announcer:start_link(#{
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
                hecate_dht:find_local_record(Dht, Pub) =/= []
            end, 1_000),
            [Record] = hecate_dht:find_local_record(Dht, Pub),
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
                hecate_dht:find_local_record(Dht, Pub) =/= []
            end, 1_000),

            ok = hecate_station_announcer:stop(Pid),

            wait_until(fun() ->
                lists:any(fun(R) ->
                    macula_record:type(R) =:= ?TYPE_TOMBSTONE
                end, hecate_dht:find_local_record(Dht, Pub))
            end, 1_000),
            Records = hecate_dht:find_local_record(Dht, Pub),
            Tombstones = [R || R <- Records,
                               macula_record:type(R) =:= ?TYPE_TOMBSTONE],
            ?assertMatch([_ | _], Tombstones)
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
                hecate_dht:find_local_record(Dht, Pub) =/= []
            end, 1_000),

            unlink(Pid),
            exit(Pid, kill),
            Ref = erlang:monitor(process, Pid),
            receive {'DOWN', Ref, process, Pid, _} -> ok
            after 1_000 -> ?assert(false) end,

            %% Settle: tombstone (if we wrongly published one) would
            %% have arrived by now since DHT puts are synchronous.
            timer:sleep(50),
            Records = hecate_dht:find_local_record(Dht, Pub),
            Tombstones = [R || R <- Records,
                               macula_record:type(R) =:= ?TYPE_TOMBSTONE],
            ?assertEqual([], Tombstones)
        end)
    end}.

%%%===================================================================
%%% Helpers
%%%===================================================================

wait_until(_F, Budget) when Budget =< 0 ->
    erlang:error(wait_until_timeout);
wait_until(F, Budget) ->
    case F() of
        true  -> ok;
        false -> timer:sleep(20), wait_until(F, Budget - 20)
    end.
