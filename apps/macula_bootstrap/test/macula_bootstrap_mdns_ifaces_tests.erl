-module(macula_bootstrap_mdns_ifaces_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% Filtering — which interfaces carry mDNS traffic.
%%==================================================================

loopback_is_ineligible_test() ->
    Props = [{flags, [up, loopback, running]},
             {addr,  {0, 0, 0, 0, 0, 0, 0, 1}}],
    ?assertNot(macula_bootstrap_mdns_ifaces:eligible(Props)).

non_multicast_is_ineligible_test() ->
    Props = [{flags, [up, broadcast, running]},  %% no multicast
             {addr,  {16#fe80, 0, 0, 0, 1, 2, 3, 4}}],
    ?assertNot(macula_bootstrap_mdns_ifaces:eligible(Props)).

down_is_ineligible_test() ->
    Props = [{flags, [broadcast, multicast]},    %% no up
             {addr,  {16#fe80, 0, 0, 0, 1, 2, 3, 4}}],
    ?assertNot(macula_bootstrap_mdns_ifaces:eligible(Props)).

ipv4_only_is_ineligible_test() ->
    Props = [{flags, [up, broadcast, multicast, running]},
             {addr,  {192, 168, 1, 1}}],
    ?assertNot(macula_bootstrap_mdns_ifaces:eligible(Props)).

up_multicast_with_ipv6_is_eligible_test() ->
    Props = [{flags, [up, broadcast, running, multicast]},
             {addr,  {192, 168, 1, 1}},
             {addr,  {16#fe80, 0, 0, 0, 1, 2, 3, 4}}],
    ?assert(macula_bootstrap_mdns_ifaces:eligible(Props)).

%%==================================================================
%% describe/2 — shape of the returned map.
%%==================================================================

describe_picks_first_link_local_test() ->
    Props = [{flags, [up, multicast]},
             {addr,  {16#2001, 16#db8, 1, 0, 0, 0, 0, 1}},         %% GUA
             {addr,  {16#fe80, 0, 0, 0, 1, 2, 3, 4}},              %% LL #1
             {addr,  {16#fe80, 0, 0, 0, 5, 6, 7, 8}}],             %% LL #2
    I = macula_bootstrap_mdns_ifaces:describe("ifake0", Props),
    ?assertEqual("ifake0", maps:get(name, I)),
    ?assertEqual({16#fe80, 0, 0, 0, 1, 2, 3, 4}, maps:get(link_local, I)),
    ?assertEqual(3,
                 length(maps:get(ipv6, I))).

describe_no_link_local_is_undefined_test() ->
    Props = [{flags, [up, multicast]},
             {addr,  {16#2001, 16#db8, 1, 0, 0, 0, 0, 1}}],
    I = macula_bootstrap_mdns_ifaces:describe("ifake1", Props),
    ?assertEqual(undefined, maps:get(link_local, I)),
    ?assertMatch([{16#2001, 16#db8, 1, 0, 0, 0, 0, 1}], maps:get(ipv6, I)).

describe_recognises_all_link_local_prefixes_test() ->
    %% fe80::/10 spans 0xfe80..0xfebf in the top 16-bit group.
    [begin
         Props = [{flags, [up, multicast]}, {addr, A}],
         I = macula_bootstrap_mdns_ifaces:describe("if", Props),
         ?assertEqual(A, maps:get(link_local, I))
     end || A <- [{16#fe80, 0, 0, 0, 0, 0, 0, 1},
                  {16#fea0, 0, 0, 0, 0, 0, 0, 1},
                  {16#febf, 0, 0, 0, 0, 0, 0, 1}]].

%%==================================================================
%% list/1 — full pipeline with a canned source.
%%==================================================================

list_with_canned_source_filters_and_describes_test() ->
    Source = fun() -> {ok, canned_ifaddrs()} end,
    Ifaces = macula_bootstrap_mdns_ifaces:list(Source),
    Names  = [maps:get(name, I) || I <- Ifaces],
    %% lo dropped (loopback). docker0 has no IPv6 → dropped.
    %% enp5s0 + wlan0 kept.
    ?assertEqual(["enp5s0", "wlan0"], lists:sort(Names)).

list_with_source_error_returns_empty_test() ->
    Source = fun() -> {error, enetdown} end,
    ?assertEqual([], macula_bootstrap_mdns_ifaces:list(Source)).

list_with_empty_source_returns_empty_test() ->
    Source = fun() -> {ok, []} end,
    ?assertEqual([], macula_bootstrap_mdns_ifaces:list(Source)).

%%==================================================================
%% Fixture — matches the shape `inet:getifaddrs/0' returns.
%%==================================================================

canned_ifaddrs() ->
    [
        {"lo",
         [{flags, [up, loopback, running]},
          {addr,  {127, 0, 0, 1}},
          {addr,  {0, 0, 0, 0, 0, 0, 0, 1}}]},
        {"enp5s0",
         [{flags, [up, broadcast, running, multicast]},
          {addr,  {192, 168, 1, 3}},
          {addr,  {16#2001, 16#db8, 1, 0, 0, 0, 0, 1}},
          {addr,  {16#fe80, 0, 0, 0, 1, 2, 3, 4}}]},
        {"docker0",
         [{flags, [up, broadcast, running, multicast]},
          {addr,  {172, 17, 0, 1}}]},                %% IPv4 only → dropped
        {"wlan0",
         [{flags, [up, broadcast, running, multicast]},
          {addr,  {16#fe80, 0, 0, 0, 9, 8, 7, 6}}]}
    ].
