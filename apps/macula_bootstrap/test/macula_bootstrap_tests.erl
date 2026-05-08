%% EUnit tests for the cascade orchestrator.
-module(macula_bootstrap_tests).

-include_lib("eunit/include/eunit.hrl").

%% A test strategy that returns the peers it was configured with after
%% sleeping for `delay' milliseconds.
-define(FAKE, macula_bootstrap_test_strategy).

%%------------------------------------------------------------------
%% Empty cascade
%%------------------------------------------------------------------

empty_cascade_fails_test() ->
    ?assertEqual({error, cascade_failed},
                 macula_bootstrap:cascade([])).

%%------------------------------------------------------------------
%% via_operator_paste — single-tier success
%%------------------------------------------------------------------

tier_e_single_cascade_succeeds_test() ->
    Urls = [url() || _ <- lists:seq(1, 3)],
    Tiers = [{macula_bootstrap_via_operator_paste, #{peer_urls => Urls}}],
    {ok, Peers} = macula_bootstrap:cascade(Tiers, #{min_peers => 3}),
    ?assertEqual(3, length(Peers)).

tier_e_below_min_peers_fails_test() ->
    %% Only one verified peer but min_peers=3.
    Tiers = [{macula_bootstrap_via_operator_paste, #{peer_urls => [url()]}}],
    ?assertEqual({error, cascade_failed},
                 macula_bootstrap:cascade(Tiers,
                     #{min_peers => 3, timeout_ms => 500})).

tier_e_no_urls_fails_test() ->
    Tiers = [{macula_bootstrap_via_operator_paste, #{}}],
    ?assertEqual({error, cascade_failed},
                 macula_bootstrap:cascade(Tiers,
                     #{min_peers => 3, timeout_ms => 500})).

%%------------------------------------------------------------------
%% Multi-tier — fast probe wins
%%------------------------------------------------------------------

fast_probe_preempts_slow_probe_test() ->
    FastUrls = [url() || _ <- lists:seq(1, 3)],
    SlowUrls = [url() || _ <- lists:seq(1, 5)],
    %% Slow tier runs after a 500ms stagger and yields 5 peers.
    %% Fast tier runs immediately and yields 3 peers.
    %% Fast tier must win because it finishes first.
    register_fake_tier(slow_tier, 500, {ok, fake_peers(SlowUrls)}),
    try
        Tiers = [{slow_tier, #{}},
                 {macula_bootstrap_via_operator_paste, #{peer_urls => FastUrls}}],
        Start = erlang:monotonic_time(millisecond),
        {ok, Peers} = macula_bootstrap:cascade(Tiers, #{min_peers => 3}),
        Elapsed = erlang:monotonic_time(millisecond) - Start,
        ?assertEqual(3, length(Peers)),
        ?assert(Elapsed < 300)  %% well under the slow probe's stagger
    after
        unregister_fake_tier(slow_tier)
    end.

failing_probes_fall_through_to_working_probe_test() ->
    Urls = [url() || _ <- lists:seq(1, 3)],
    register_fake_tier(broken_a, 0, {error, network_unreachable}),
    register_fake_tier(broken_b, 50, {error, dns_failure}),
    try
        Tiers = [
            {broken_a, #{}},
            {broken_b, #{}},
            {macula_bootstrap_via_operator_paste, #{peer_urls => Urls}}
        ],
        {ok, Peers} = macula_bootstrap:cascade(Tiers,
                          #{min_peers => 3, timeout_ms => 1000}),
        ?assertEqual(3, length(Peers))
    after
        unregister_fake_tier(broken_a),
        unregister_fake_tier(broken_b)
    end.

%%------------------------------------------------------------------
%% Timeout
%%------------------------------------------------------------------

cascade_times_out_test() ->
    register_fake_tier(stuck, 5000, {ok, []}),
    try
        Tiers = [{stuck, #{}}],
        ?assertEqual({error, timeout},
                     macula_bootstrap:cascade(Tiers,
                         #{min_peers => 3, timeout_ms => 100}))
    after
        unregister_fake_tier(stuck)
    end.

%%------------------------------------------------------------------
%% Fake tier: built at runtime via persistent_term so the behaviour
%% callbacks can be redirected per test.
%%------------------------------------------------------------------

register_fake_tier(Name, StaggerMs, ProbeResult) ->
    Forms = [
        {attribute, 1, module, Name},
        {attribute, 1, behaviour, macula_bootstrap_peer_discoverer},
        {attribute, 1, export,
         [{strategy, 0}, {stagger_ms, 0}, {discover, 1}]},
        {function, 1, strategy, 0,
         [{clause, 1, [], [], [{atom, 1, Name}]}]},
        {function, 1, stagger_ms, 0,
         [{clause, 1, [], [],
           [{integer, 1, StaggerMs}]}]},
        {function, 1, discover, 1,
         [{clause, 1, [{var, 1, '_'}], [],
           [term_to_expr(ProbeResult)]}]}
    ],
    {ok, Name, Bin} = compile:forms(Forms, [return_errors]),
    {module, Name} = code:load_binary(Name, atom_to_list(Name) ++ ".erl", Bin),
    ok.

unregister_fake_tier(Name) ->
    code:purge(Name),
    code:delete(Name),
    code:purge(Name),
    ok.

term_to_expr(T) ->
    erl_parse:abstract(T).

%%------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------

url() ->
    Kp = macula_identity:generate(),
    Record = macula_record:sign(
        macula_record:node_record(macula_identity:public(Kp), [], 0),
        Kp),
    macula_bootstrap_via_operator_paste_peer_url:encode(Record, []).

fake_peers(Urls) ->
    [fake_peer(U) || U <- Urls].

fake_peer(Url) ->
    {ok, Record, Addrs} = macula_bootstrap_via_operator_paste_peer_url:decode(Url),
    #{
        node_id   => macula_record:key(Record),
        record    => Record,
        addresses => Addrs,
        strategy  => x,
        via       => fake
    }.
