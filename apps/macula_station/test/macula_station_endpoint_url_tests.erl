%%% @doc `content_announcement''s `endpoint' URL construction.
%%%
%%% Regression cover for a real bug found via a live-fleet probe of
%%% content direct-dial (2026-08-20): `endpoint_url/2' built
%%% `quic://Host:Port' with no bracket-awareness at all. For an IPv6
%%% bind address this produces an unparseable URL -- the trailing
%%% `:Port' is ambiguous with the address's own colons, and
%%% `macula_station_link:parse_seed/1' on the SDK side correctly
%%% rejects it as `{invalid_seed_url, _}'. Confirmed live: nuremberg
%%% announced content, a helsinki-only pool resolved the announcement
%%% and tried to dial `"quic://2a01:4f8:1c1f:8ab8::be:02:4433"' --
%%% no brackets -- and the dial crashed before ever reaching the
%%% network. `station_endpoint' (the RPC/content-upload direct-dial
%%% path) never hit this because it stores the raw host and lets the
%%% SDK's `macula_direct_dial:build_dial_url/2' add brackets when
%%% building the full URL; `content_announcement''s `endpoint' is a
%%% complete URL built once, here, with nothing downstream to add them
%%% later.
-module(macula_station_endpoint_url_tests).

-include_lib("eunit/include/eunit.hrl").

ipv6_tuple_gets_bracketed_test() ->
    Ipv6 = {16#2a01, 16#4f8, 16#1c1f, 16#8ab8, 0, 0, 16#be, 16#02},
    ?assertEqual(<<"quic://[2a01:4f8:1c1f:8ab8::be:2]:4433">>,
                 macula_station_app:endpoint_url(Ipv6, 4433)).

ipv4_tuple_is_not_bracketed_test() ->
    Ipv4 = {192, 168, 1, 10},
    ?assertEqual(<<"quic://192.168.1.10:4433">>,
                 macula_station_app:endpoint_url(Ipv4, 4433)).

hostname_string_is_not_bracketed_test() ->
    ?assertEqual(<<"quic://station-de-nuremberg.macula.io:4433">>,
                 macula_station_app:endpoint_url(
                   "station-de-nuremberg.macula.io", 4433)).

ipv6_literal_string_is_bracketed_test() ->
    %% `Bind' arrives as a plain string in some configs, not always a
    %% tuple -- the colon check must work on that shape too.
    ?assertEqual(<<"quic://[2a01:4f8:1c1f:8ab8::be:2]:4433">>,
                 macula_station_app:endpoint_url(
                   "2a01:4f8:1c1f:8ab8::be:2", 4433)).
