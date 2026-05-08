-module(macula_bootstrap_via_mdns_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% Fixture
%%==================================================================

tier_b_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun happy_path/1,
      fun no_replies/1,
      fun handshake_failure_drops_candidate/1,
      fun node_id_mismatch_drops_candidate/1,
      fun expired_record_dropped/1,
      fun malformed_txt_skipped/1,
      fun deduplicates_by_node_id/1,
      fun missing_handshake_opt_drops_candidate/1
     ]}.

setup() ->
    application:ensure_all_started(crypto),
    macula_bootstrap_via_mdns_fake:init(),
    Peers = [make_peer() || _ <- lists:seq(1, 3)],
    Handshake = registry_handshake(Peers),
    #{peers => Peers, handshake => Handshake}.

cleanup(_Ctx) ->
    macula_bootstrap_via_mdns_fake:reset(),
    ok.

%%==================================================================
%% Cases
%%==================================================================

happy_path(#{peers := Peers, handshake := Handshake}) ->
    fun() ->
        Replies = [peer_to_reply(P) || P <- Peers],
        macula_bootstrap_via_mdns_fake:set_replies(Replies),
        {ok, Got} = probe(Handshake),
        ?assertEqual(length(Peers), length(Got)),
        ExpectedIds = lists:sort([maps:get(pub, P) || P <- Peers]),
        GotIds      = lists:sort([maps:get(node_id, G) || G <- Got]),
        ?assertEqual(ExpectedIds, GotIds),
        [?assertEqual(via_mdns, maps:get(strategy, G)) || G <- Got],
        [?assertEqual(macula_bootstrap_via_mdns, maps:get(via, G))
         || G <- Got]
    end.

no_replies(#{handshake := Handshake}) ->
    fun() ->
        macula_bootstrap_via_mdns_fake:set_replies([]),
        ?assertEqual({ok, []}, probe(Handshake))
    end.

handshake_failure_drops_candidate(#{peers := [P1 | _]}) ->
    fun() ->
        macula_bootstrap_via_mdns_fake:set_replies([peer_to_reply(P1)]),
        Handshake = fun(_, _, _) -> {error, quic_timeout} end,
        ?assertEqual({ok, []}, probe(Handshake))
    end.

node_id_mismatch_drops_candidate(#{peers := [P1 | _]}) ->
    fun() ->
        %% Handshake returns a record for a DIFFERENT NodeId than the
        %% one the TXT advertised. Must be dropped.
        Other = make_peer(),
        Handshake = fun(_, _, _) ->
                            {ok, maps:get(signed_record, Other)}
                    end,
        macula_bootstrap_via_mdns_fake:set_replies([peer_to_reply(P1)]),
        ?assertEqual({ok, []}, probe(Handshake))
    end.

expired_record_dropped(_Ctx) ->
    fun() ->
        Expired = make_expired_peer(),
        Handshake = fun(_, _, _) ->
                            {ok, maps:get(signed_record, Expired)}
                    end,
        macula_bootstrap_via_mdns_fake:set_replies(
          [peer_to_reply(Expired)]),
        ?assertEqual({ok, []}, probe(Handshake))
    end.

malformed_txt_skipped(#{peers := [P1 | _], handshake := Handshake}) ->
    fun() ->
        Good = peer_to_reply(P1),
        Bad = {addr(99), build_response(
                          [{"_macula._udp.local", txt,
                            ["broken_txt"]}])},
        macula_bootstrap_via_mdns_fake:set_replies([Bad, Good]),
        {ok, Got} = probe(Handshake),
        ?assertEqual(1, length(Got)),
        ?assertEqual(maps:get(pub, P1),
                     maps:get(node_id, hd(Got)))
    end.

deduplicates_by_node_id(#{peers := [P1 | _], handshake := Handshake}) ->
    fun() ->
        Reply1 = peer_to_reply(P1),
        Reply2 = {addr(77), element(2, Reply1)},
        macula_bootstrap_via_mdns_fake:set_replies([Reply1, Reply2]),
        {ok, Got} = probe(Handshake),
        ?assertEqual(1, length(Got))
    end.

missing_handshake_opt_drops_candidate(#{peers := [P1 | _]}) ->
    fun() ->
        macula_bootstrap_via_mdns_fake:set_replies([peer_to_reply(P1)]),
        {ok, Got} = macula_bootstrap_via_mdns:discover(
                      #{udp_transport => macula_bootstrap_via_mdns_fake,
                        timeout_ms    => 500}),
        ?assertEqual([], Got)
    end.

%%==================================================================
%% Shared probe invocation
%%==================================================================

probe(Handshake) ->
    macula_bootstrap_via_mdns:discover(
      #{udp_transport => macula_bootstrap_via_mdns_fake,
        handshake_fun => Handshake,
        timeout_ms    => 500}).

%%==================================================================
%% Peer + packet helpers
%%==================================================================

make_peer() ->
    Kp  = macula_identity:generate(),
    Pub = macula_identity:public(Kp),
    Rec = macula_record:sign(
            macula_record:node_record(Pub, [], 0), Kp),
    #{kp => Kp, pub => Pub, signed_record => Rec,
      port => 7000, tier => 0, addr => rand_addr()}.

make_expired_peer() ->
    Kp  = macula_identity:generate(),
    Pub = macula_identity:public(Kp),
    Rec = macula_record:sign(
            macula_record:node_record(Pub, [], 0, #{ttl_ms => 1}), Kp),
    timer:sleep(5),
    #{kp => Kp, pub => Pub, signed_record => Rec,
      port => 7000, tier => 0, addr => rand_addr()}.

peer_to_reply(#{addr := Addr} = Peer) ->
    {Addr, build_response(
             [{"_macula._udp.local", txt, txt_strings(Peer)}])}.

txt_strings(#{pub := NodeId, port := Port, tier := Tier}) ->
    [lists:flatten(io_lib:format("node_id=~s", [hex(NodeId)])),
     lists:flatten(io_lib:format("port=~p",    [Port])),
     lists:flatten(io_lib:format("tier=~p",    [Tier]))].

registry_handshake(Peers) ->
    Index = maps:from_list(
              [{maps:get(pub, P), maps:get(signed_record, P)} || P <- Peers]),
    fun(_Addr, _Port, NodeId) ->
            case maps:find(NodeId, Index) of
                {ok, Rec} -> {ok, Rec};
                error     -> {error, not_registered}
            end
    end.

hex(Bin) ->
    string:lowercase(binary_to_list(binary:encode_hex(Bin))).

rand_addr() ->
    {16#fe80, 0, 0, 0,
     rand:uniform(65536) - 1, rand:uniform(65536) - 1,
     rand:uniform(65536) - 1, rand:uniform(65536) - 1}.

addr(N) -> {16#fe80, 0, 0, 0, 0, 0, 0, N}.

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
