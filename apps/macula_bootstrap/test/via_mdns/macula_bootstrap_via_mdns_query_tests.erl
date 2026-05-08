-module(macula_bootstrap_via_mdns_query_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% Constants
%%==================================================================

service_name_and_multicast_test() ->
    ?assertEqual("_macula._udp.local",
                 macula_bootstrap_via_mdns_query:service_name()),
    ?assertEqual({16#FF02, 0, 0, 0, 0, 0, 0, 16#FB},
                 macula_bootstrap_via_mdns_query:multicast_group()),
    ?assertEqual(5353, macula_bootstrap_via_mdns_query:multicast_port()).

%%==================================================================
%% build_query
%%==================================================================

build_query_shape_test() ->
    {Id, Bin} = macula_bootstrap_via_mdns_query:build_query(
                  "_macula._udp.local", 7),
    ?assertEqual(7, Id),
    {ok, {dns_rec, {dns_header, 7, false, query, _, _, false, _, _, 0},
                   [{dns_query, "_macula._udp.local", any, in, _}],
                   [], [], []}} = inet_dns:decode(Bin).

build_query_default_domain_test() ->
    {_Id, Bin} = macula_bootstrap_via_mdns_query:build_query(),
    {ok, {dns_rec, _H, [{dns_query, Domain, any, in, _}], _, _, _}} =
        inet_dns:decode(Bin),
    ?assertEqual("_macula._udp.local", Domain).

%%==================================================================
%% parse_response
%%==================================================================

parse_response_empty_answers_test() ->
    Bin = build_response([]),
    ?assertEqual({ok, []}, macula_bootstrap_via_mdns_query:parse_response(Bin)).

parse_response_returns_answer_maps_test() ->
    Bin = build_response([{"_macula._udp.local", txt,
                           txt_strings(sample_peer(1))}]),
    {ok, [Answer]} = macula_bootstrap_via_mdns_query:parse_response(Bin),
    ?assertMatch(#{name := "_macula._udp.local", type := txt, data := _},
                 Answer).

parse_response_garbage_test() ->
    ?assertEqual({error, decode_failed},
                 macula_bootstrap_via_mdns_query:parse_response(<<"not dns">>)).

%%==================================================================
%% extract_candidates
%%==================================================================

extract_single_txt_test() ->
    Peer = sample_peer(1),
    Answers = [txt_answer(Peer)],
    [C] = macula_bootstrap_via_mdns_query:extract_candidates(Answers),
    ?assertEqual(maps:get(node_id, Peer), maps:get(node_id, C)),
    ?assertEqual(7000, maps:get(port, C)),
    ?assertEqual(0, maps:get(tier, C)).

extract_multiple_txt_records_test() ->
    Peers = [sample_peer(N) || N <- [1, 2, 3]],
    Answers = [txt_answer(P) || P <- Peers],
    Candidates = macula_bootstrap_via_mdns_query:extract_candidates(Answers),
    ?assertEqual(3, length(Candidates)),
    ExpectedIds = lists:sort([maps:get(node_id, P) || P <- Peers]),
    GotIds      = lists:sort([maps:get(node_id, C) || C <- Candidates]),
    ?assertEqual(ExpectedIds, GotIds).

extract_ignores_non_txt_test() ->
    Answers = [
        #{name => "_macula._udp.local", type => aaaa,
          data => {16#fe80, 0, 0, 0, 0, 0, 0, 1}},
        txt_answer(sample_peer(1))
    ],
    ?assertEqual(1, length(macula_bootstrap_via_mdns_query:extract_candidates(Answers))).

extract_ignores_wrong_name_test() ->
    Answers = [
        #{name => "_other._udp.local", type => txt,
          data => txt_strings(sample_peer(1))}
    ],
    ?assertEqual([], macula_bootstrap_via_mdns_query:extract_candidates(Answers)).

extract_is_case_insensitive_for_name_test() ->
    Answers = [
        #{name => "_MACULA._udp.local", type => txt,
          data => txt_strings(sample_peer(1))}
    ],
    ?assertEqual(1, length(macula_bootstrap_via_mdns_query:extract_candidates(Answers))).

extract_drops_missing_node_id_test() ->
    Answers = [malformed_txt(["port=7000", "tier=0"])],
    ?assertEqual([], macula_bootstrap_via_mdns_query:extract_candidates(Answers)).

extract_drops_missing_port_test() ->
    Peer = sample_peer(1),
    Hex = hex(maps:get(node_id, Peer)),
    Answers = [malformed_txt(["node_id=" ++ Hex, "tier=0"])],
    ?assertEqual([], macula_bootstrap_via_mdns_query:extract_candidates(Answers)).

extract_drops_missing_tier_test() ->
    Peer = sample_peer(1),
    Hex = hex(maps:get(node_id, Peer)),
    Answers = [malformed_txt(["node_id=" ++ Hex, "port=7000"])],
    ?assertEqual([], macula_bootstrap_via_mdns_query:extract_candidates(Answers)).

extract_drops_bad_hex_test() ->
    Answers = [malformed_txt(["node_id=not-a-hex",
                              "port=7000", "tier=0"])],
    ?assertEqual([], macula_bootstrap_via_mdns_query:extract_candidates(Answers)).

extract_drops_bad_hex_length_test() ->
    %% Valid hex characters but wrong length.
    Answers = [malformed_txt(["node_id=deadbeef",
                              "port=7000", "tier=0"])],
    ?assertEqual([], macula_bootstrap_via_mdns_query:extract_candidates(Answers)).

extract_drops_port_out_of_range_test_() ->
    Peer = sample_peer(1),
    Hex = hex(maps:get(node_id, Peer)),
    [?_assertEqual(
        [],
        macula_bootstrap_via_mdns_query:extract_candidates(
          [malformed_txt(["node_id=" ++ Hex,
                          "port=" ++ integer_to_list(P),
                          "tier=0"])]))
     || P <- [0, -1, 65536, 100000]].

extract_drops_tier_out_of_range_test_() ->
    Peer = sample_peer(1),
    Hex = hex(maps:get(node_id, Peer)),
    [?_assertEqual(
        [],
        macula_bootstrap_via_mdns_query:extract_candidates(
          [malformed_txt(["node_id=" ++ Hex,
                          "port=7000",
                          "tier=" ++ integer_to_list(T)])]))
     || T <- [-1, 5, 99]].

%%==================================================================
%% Helpers
%%==================================================================

sample_peer(N) ->
    #{node_id => <<N:256>>, port => 7000, tier => 0}.

hex(Bin) ->
    string:lowercase(binary_to_list(binary:encode_hex(Bin))).

txt_answer(Peer) ->
    #{name => "_macula._udp.local",
      type => txt,
      data => txt_strings(Peer)}.

txt_strings(#{node_id := NodeId, port := Port, tier := Tier}) ->
    [lists:flatten(io_lib:format("node_id=~s", [hex(NodeId)])),
     lists:flatten(io_lib:format("port=~p",    [Port])),
     lists:flatten(io_lib:format("tier=~p",    [Tier]))].

malformed_txt(Strings) ->
    #{name => "_macula._udp.local", type => txt, data => Strings}.

build_response(AnswerSpecs) ->
    Answers = [inet_dns:make_rr([
        {domain, Name}, {type, Type}, {class, in}, {ttl, 60},
        {data, Data}
    ]) || {Name, Type, Data} <- AnswerSpecs],
    Msg = inet_dns:make_msg([
        {header, inet_dns:make_header(
                    [{id, 1}, {qr, true}, {opcode, query},
                     {rd, false}, {ra, false}, {rcode, 0}])},
        {anlist, Answers}
    ]),
    iolist_to_binary(inet_dns:encode(Msg)).
