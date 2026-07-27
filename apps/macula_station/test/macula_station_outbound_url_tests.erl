%% URL parsing for outbound links, including the IPv6 form that used to crash.
%%
%% `parse_url/1' split on the first colon, which is correct for `host:port' and
%% fatal for `::1:5000': the host is itself full of colons, so the old code fed
%% `<<":1:5000">>' to binary_to_integer and died with a badarg. Production dials
%% DNS names so it never bit there, but every station in
%% `macula_station_test_cluster' binds to ::1, which made an outbound link
%% impossible to build in any in-repo test.
-module(macula_station_outbound_url_tests).

-include_lib("eunit/include/eunit.hrl").

bracketed_ipv6_with_port_test() ->
    ?assertEqual({<<"::1">>, 5000}, parse(<<"[::1]:5000">>)).

bracketed_ipv6_default_port_test() ->
    ?assertEqual({<<"::1">>, 4433}, parse(<<"[::1]">>)).

bracketed_ipv6_with_scheme_test() ->
    ?assertEqual({<<"2001:db8::1">>, 4433},
                 parse(<<"quic://[2001:db8::1]:4433">>)).

hostname_still_works_test() ->
    ?assertEqual({<<"relay-de-berlin.macula.io">>, 4433},
                 parse(<<"relay-de-berlin.macula.io:4433">>)).

hostname_default_port_test() ->
    ?assertEqual({<<"example.com">>, 4433}, parse(<<"https://example.com">>)).

ipv4_still_works_test() ->
    ?assertEqual({<<"127.0.0.1">>, 5000}, parse(<<"127.0.0.1:5000">>)).

%% A BARE IPv6 literal is ambiguous by definition — there is no way to tell the
%% last group from a port. It must not crash the link; it degrades to the
%% default port.
bare_ipv6_does_not_crash_test() ->
    ?assertMatch({_, 4433}, parse(<<"::1">>)).

parse(Url) ->
    macula_station_outbound_link:parse_url(Url).
