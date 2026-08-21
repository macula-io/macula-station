%% @doc A connection this dialer establishes must stay a full peering
%% link, not a write-only one.
%%
%% `ensure_dialed/3' registers the freshly-dialled connection with
%% `macula_station_peer_observer' (`connected_outbound', same shape
%% `macula_station_outbound_link' uses) so `conn_for/2' resolves it for
%% future sends. But this module also stays the QUIC `controlling_pid'
%% for that connection (set at dial time, in `dial_opts_with_self/0'),
%% and its own moduledoc claims frames are "fast-pathed straight past
%% this process" — a claim the code never implements. Every later
%% frame on the connection — CALL, RESULT, PUBLISH, anything — lands
%% in `handle_info({macula_peering, frame, ...})' and, pre-fix, was
%% dropped with only a `?LOG_DEBUG' line. That makes any connection
%% this module dials write-only from the dialling station's own
%% perspective: it can relay a CALL out (`macula_station_peer_observer'
%% sends over the raw ConnPid via `on_remote_lookup'), but can never
%% receive one back, since nothing else owns this mailbox.
%%
%% Found live on the fleet 2026-08-21: the DHT walk's on-demand dial
%% (added 2026-08-20) created a genuine direct helsinki<->nuremberg
%% connection where none existed before. Calls FROM the dialling
%% station arrived and were forwarded correctly (sending doesn't care
%% who owns the connection); calls TO it timed out 100% of the time,
%% deterministically, with the caller-side station showing a tracked
%% `forwarded' entry and every other station — including the true
%% recipient — showing none. No log signal beyond the debug line.
-module(macula_station_dht_dialer_tests).
-include_lib("eunit/include/eunit.hrl").

-define(M, macula_station_dht_dialer).

frame_on_a_dialed_connection_reaches_the_observer_test_() ->
    {setup, fun setup/0, fun teardown/1, fun(Ctx) ->
        %% EUnit's OWN default per-test timeout is 5s and wraps the
        %% whole test fun regardless of any internal `receive ... after'
        %% -- a plain `wait_for_observer_message(10_000)' below got
        %% killed by THAT outer timeout before it could ever return,
        %% reported as "cancelled", not pass or fail. `{timeout, 20, _}'
        %% raises the outer ceiling so the internal wait actually gets
        %% to run to completion.
        {timeout, 20, fun() ->
            #{dialer := Dialer} = Ctx,
            %% `forward_to_observer/1' hardcodes
            %% `whereis(macula_station_peer_observer)' -- register THIS
            %% test process (the one about to receive) under that name
            %% directly, rather than a separate helper process. EUnit's
            %% `{setup, ...}' runs the setup fun and the instantiated
            %% test body in different processes, so registering in
            %% `setup/0' would stand in for the wrong mailbox.
            true = register(macula_station_peer_observer, self()),
            ConnPid = spawn_dummy(),
            Frame   = {call, <<"irrelevant-for-this-test">>},
            Dialer ! {macula_peering, frame, ConnPid, Frame},
            %% Generous, not tight: a heavily loaded VM (this repo's
            %% full suite is 1000+ tests) can genuinely push local
            %% message delivery out several seconds under scheduler
            %% contention -- not a defect in what this test checks.
            ?assertEqual({macula_peering, frame, ConnPid, Frame},
                        wait_for_observer_message(15_000))
        end}
    end}.

setup() ->
    application:ensure_all_started(crypto),
    %% NOT `?M:start_link()' -- that registers `{local, ?MODULE}',
    %% making this instance addressable by `ensure_dialed/3' from ANY
    %% other code in the same VM for as long as this test holds the
    %% name. This test only needs to exercise one instance directly, so
    %% start it anonymously and keep it reachable solely via the
    %% `Dialer' pid -- avoids handing a real, globally-named gen_server
    %% to a shared-VM full-suite run for no reason this test needs.
    {ok, Dialer} = gen_server:start_link(?M, [], []),
    #{dialer => Dialer}.

teardown(#{dialer := Dialer}) ->
    catch unregister(macula_station_peer_observer),
    _ = catch gen_server:stop(Dialer),
    ok.

wait_for_observer_message(Ms) ->
    receive
        Msg -> Msg
    after Ms ->
        timeout
    end.

spawn_dummy() ->
    spawn(fun() -> receive stop -> ok end end).
