-module(hecate_bootstrap_doh_tests).
-include_lib("eunit/include/eunit.hrl").

-define(DOH_URL, <<"https://dns.example/dns-query">>).

%%==================================================================
%% query_domain
%%==================================================================

query_domain_zero_key_test() ->
    Pub = <<0:256>>,
    Expected = "_pkarr." ++ string:copies("a", 52) ++ ".macula.io",
    ?assertEqual(Expected,
                 hecate_bootstrap_doh:query_domain(Pub, <<"macula.io">>)).

query_domain_random_key_shape_test() ->
    Pub = crypto:strong_rand_bytes(32),
    Domain = hecate_bootstrap_doh:query_domain(Pub, <<"macula.eu">>),
    ["_pkarr", Label, "macula", "eu"] = string:split(Domain, ".", all),
    ?assertEqual(52, length(Label)),
    {ok, Decoded} = hecate_bootstrap_base32:decode(
                      iolist_to_binary(Label)),
    ?assertEqual(Pub, Decoded).

query_domain_accepts_string_zone_test() ->
    Pub = <<0:256>>,
    ?assertEqual("_pkarr." ++ string:copies("a", 52) ++ ".example.org",
                 hecate_bootstrap_doh:query_domain(Pub, "example.org")).

%%==================================================================
%% build_query
%%==================================================================

build_query_encodes_txt_in_question_test() ->
    {Id, Bin} = hecate_bootstrap_doh:build_query("example.com", 7),
    ?assertEqual(7, Id),
    {ok, {dns_rec, {dns_header, 7, false, query, _, _, true, _, _, 0},
                   [{dns_query, "example.com", txt, in, _}],
                   [], [], []}} = inet_dns:decode(Bin).

build_query_random_id_in_range_test_() ->
    [begin
        {Id, _Bin} = hecate_bootstrap_doh:build_query("example.com"),
        ?_assert(Id >= 0 andalso Id =< 65535)
     end || _ <- lists:seq(1, 10)].

%%==================================================================
%% parse_response
%%==================================================================

parse_response_single_string_test() ->
    Bytes = <<1, 2, 3, 4>>,
    Bin = build_answer(42, "example.com", [Bytes]),
    ?assertEqual({ok, Bytes},
                 hecate_bootstrap_doh:parse_response(Bin, 42,
                                                    "example.com")).

parse_response_concatenates_strings_in_one_rr_test() ->
    Bin = build_answer(1, "example.com",
                       [<<"hello">>, <<"world">>]),
    ?assertEqual({ok, <<"helloworld">>},
                 hecate_bootstrap_doh:parse_response(Bin, 1,
                                                    "example.com")).

parse_response_concatenates_multiple_rrs_test() ->
    Bin = build_answer_multi(9, "example.com",
                             [[<<"aaa">>], [<<"bbb">>], [<<"ccc">>]]),
    ?assertEqual({ok, <<"aaabbbccc">>},
                 hecate_bootstrap_doh:parse_response(Bin, 9,
                                                    "example.com")).

parse_response_case_insensitive_name_test() ->
    Bin = build_answer(5, "EXAMPLE.com", [<<"zzz">>]),
    ?assertEqual({ok, <<"zzz">>},
                 hecate_bootstrap_doh:parse_response(Bin, 5,
                                                    "example.com")).

parse_response_id_mismatch_test() ->
    Bin = build_answer(1, "example.com", [<<"x">>]),
    ?assertEqual({error, id_mismatch},
                 hecate_bootstrap_doh:parse_response(Bin, 2,
                                                    "example.com")).

parse_response_not_a_response_test() ->
    Bin = build_question_only(3, "example.com"),
    ?assertEqual({error, not_a_response},
                 hecate_bootstrap_doh:parse_response(Bin, 3,
                                                    "example.com")).

parse_response_rcode_nxdomain_test() ->
    Bin = build_rcode_only(4, "example.com", 3),
    ?assertEqual({error, {rcode, 3}},
                 hecate_bootstrap_doh:parse_response(Bin, 4,
                                                    "example.com")).

parse_response_rcode_refused_test() ->
    Bin = build_rcode_only(11, "example.com", 5),
    ?assertEqual({error, {rcode, 5}},
                 hecate_bootstrap_doh:parse_response(Bin, 11,
                                                    "example.com")).

parse_response_no_txt_answer_test() ->
    Bin = build_rcode_only(6, "example.com", 0),
    ?assertEqual({error, no_txt_answer},
                 hecate_bootstrap_doh:parse_response(Bin, 6,
                                                    "example.com")).

parse_response_wrong_domain_test() ->
    Bin = build_answer(8, "other.example", [<<"data">>]),
    ?assertEqual({error, no_txt_answer},
                 hecate_bootstrap_doh:parse_response(Bin, 8,
                                                    "example.com")).

parse_response_garbage_bytes_test() ->
    ?assertEqual({error, decode_failed},
                 hecate_bootstrap_doh:parse_response(
                   <<"not a dns packet">>, 1, "example.com")).

%%==================================================================
%% resolve/4 — core with canned SendFun
%%==================================================================

resolve_happy_path_test() ->
    Pub = crypto:strong_rand_bytes(32),
    Bytes = <<"hello-record-bytes">>,
    Domain = hecate_bootstrap_doh:query_domain(Pub, <<"macula.io">>),
    Send = fun(Url, QueryBin) ->
                   ?assertEqual(?DOH_URL, Url),
                   {ok, {dns_rec, Hdr, [{dns_query, QDomain, txt, in, _}],
                         _, _, _}} = inet_dns:decode(QueryBin),
                   ?assertEqual(Domain, QDomain),
                   Id = element(2, Hdr),
                   {ok, build_answer(Id, Domain, [Bytes])}
           end,
    ?assertEqual({ok, Bytes},
                 hecate_bootstrap_doh:resolve(
                   ?DOH_URL, Pub, #{}, Send)).

resolve_http_error_wrapped_test() ->
    Pub = crypto:strong_rand_bytes(32),
    Send = fun(_, _) -> {error, econnrefused} end,
    ?assertEqual({error, {http, econnrefused}},
                 hecate_bootstrap_doh:resolve(
                   ?DOH_URL, Pub, #{}, Send)).

resolve_honours_zone_base_test() ->
    Pub = crypto:strong_rand_bytes(32),
    Bytes = <<"eu-zone-bytes">>,
    Send = fun(_, QueryBin) ->
                   {ok, {dns_rec, Hdr,
                         [{dns_query, QDomain, txt, in, _}],
                         _, _, _}} = inet_dns:decode(QueryBin),
                   ?assert(lists:suffix(".macula.eu", QDomain)),
                   Id = element(2, Hdr),
                   {ok, build_answer(Id, QDomain, [Bytes])}
           end,
    ?assertEqual({ok, Bytes},
                 hecate_bootstrap_doh:resolve(
                   ?DOH_URL, Pub,
                   #{zone_base => <<"macula.eu">>}, Send)).

resolve_propagates_parse_errors_test() ->
    Pub = crypto:strong_rand_bytes(32),
    Send = fun(_, _) -> {ok, <<"garbage">>} end,
    ?assertEqual({error, decode_failed},
                 hecate_bootstrap_doh:resolve(
                   ?DOH_URL, Pub, #{}, Send)).

%%==================================================================
%% Response builders
%%==================================================================

build_answer(Id, Domain, Strings) ->
    build_answer_multi(Id, Domain, [Strings]).

build_answer_multi(Id, Domain, ListOfStringLists) ->
    Answers = [make_txt_rr(Domain, S) || S <- ListOfStringLists],
    Msg = inet_dns:make_msg([
        {header, inet_dns:make_header(
                    [{id, Id}, {qr, true}, {opcode, query},
                     {rd, true}, {ra, true}, {rcode, 0}])},
        {qdlist, [inet_dns:make_dns_query(
                    [{domain, Domain}, {type, txt}, {class, in}])]},
        {anlist, Answers}
    ]),
    iolist_to_binary(inet_dns:encode(Msg)).

make_txt_rr(Domain, Strings) ->
    inet_dns:make_rr([
        {domain, Domain}, {type, txt}, {class, in}, {ttl, 60},
        {data, Strings}
    ]).

build_question_only(Id, Domain) ->
    Msg = inet_dns:make_msg([
        {header, inet_dns:make_header(
                    [{id, Id}, {qr, false}, {opcode, query},
                     {rd, true}, {rcode, 0}])},
        {qdlist, [inet_dns:make_dns_query(
                    [{domain, Domain}, {type, txt}, {class, in}])]}
    ]),
    iolist_to_binary(inet_dns:encode(Msg)).

build_rcode_only(Id, Domain, RCode) ->
    Msg = inet_dns:make_msg([
        {header, inet_dns:make_header(
                    [{id, Id}, {qr, true}, {opcode, query},
                     {rd, true}, {ra, true}, {rcode, RCode}])},
        {qdlist, [inet_dns:make_dns_query(
                    [{domain, Domain}, {type, txt}, {class, in}])]}
    ]),
    iolist_to_binary(inet_dns:encode(Msg)).
