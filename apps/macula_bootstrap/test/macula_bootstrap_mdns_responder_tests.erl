-module(macula_bootstrap_mdns_responder_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% build_advertisement (pure)
%%==================================================================

advertisement_happy_path_test() ->
    Info = sample_info(),
    Query = build_query("_macula._udp.local", any),
    {ok, RespBin} = macula_bootstrap_mdns:build_advertisement(Query, Info),
    {ok, {dns_rec, Hdr, _Qs, [Answer], _Ns, _Ar}} = inet_dns:decode(RespBin),
    {dns_header, _, true, _, true, _, _, _, _, 0} = Hdr,
    {dns_rr, "_macula._udp.local", txt, in, _, _TTL, Strings, _, _, _} = Answer,
    Map = strings_to_map(Strings),
    ?assertEqual(hex(maps:get(node_id, Info)),
                 binary_to_list(maps:get(<<"node_id">>, Map))),
    ?assertEqual(integer_to_list(maps:get(port, Info)),
                 binary_to_list(maps:get(<<"port">>, Map))),
    ?assertEqual(integer_to_list(maps:get(tier, Info)),
                 binary_to_list(maps:get(<<"tier">>, Map))).

advertisement_ignores_response_packet_test() ->
    %% Another responder's response leaks in — we must not echo it.
    Info = sample_info(),
    Response = build_response_packet("_macula._udp.local"),
    ?assertEqual(ignore,
                 macula_bootstrap_mdns:build_advertisement(Response, Info)).

advertisement_ignores_other_service_test() ->
    Info = sample_info(),
    Query = build_query("_other._udp.local", any),
    ?assertEqual(ignore,
                 macula_bootstrap_mdns:build_advertisement(Query, Info)).

advertisement_ignores_unsupported_type_test() ->
    Info = sample_info(),
    Query = build_query("_macula._udp.local", srv),
    ?assertEqual(ignore,
                 macula_bootstrap_mdns:build_advertisement(Query, Info)).

advertisement_serves_txt_test() ->
    Info = sample_info(),
    Query = build_query("_macula._udp.local", txt),
    ?assertMatch({ok, _},
                 macula_bootstrap_mdns:build_advertisement(Query, Info)).

advertisement_serves_ptr_test() ->
    Info = sample_info(),
    Query = build_query("_macula._udp.local", ptr),
    ?assertMatch({ok, _},
                 macula_bootstrap_mdns:build_advertisement(Query, Info)).

advertisement_ignores_garbage_test() ->
    Info = sample_info(),
    ?assertEqual(ignore,
                 macula_bootstrap_mdns:build_advertisement(
                   <<"not a dns packet">>, Info)).

advertisement_echoes_query_id_test() ->
    Info = sample_info(),
    Query = build_query("_macula._udp.local", any, 4242),
    {ok, RespBin} = macula_bootstrap_mdns:build_advertisement(Query, Info),
    {ok, {dns_rec, Hdr, _, _, _, _}} = inet_dns:decode(RespBin),
    ?assertEqual(4242, element(2, Hdr)).

%%==================================================================
%% Responder gen_server lifecycle + packet round-trip
%%==================================================================

responder_loopback_test_() ->
    {timeout, 5,
     fun() ->
        Info = sample_info(),
        {ok, Pid} = macula_bootstrap_mdns_responder:start_link(
                      Info#{socket_opener => loopback_opener()}),
        try
            Port = macula_bootstrap_mdns_responder:port(Pid),
            ?assert(Port > 0),
            Resp = ask_responder(Port,
                                 build_query("_macula._udp.local", any,
                                             17)),
            {ok, {dns_rec, Hdr, _, [Answer], _, _}} = inet_dns:decode(Resp),
            ?assertEqual(17, element(2, Hdr)),
            ?assertMatch({dns_rr, "_macula._udp.local", txt, in, _, _,
                          _, _, _, _}, Answer)
        after
            macula_bootstrap_mdns_responder:stop(Pid)
        end
     end}.

responder_silent_drops_query_test_() ->
    {timeout, 5,
     fun() ->
        Info = sample_info(),
        {ok, Pid} = macula_bootstrap_mdns_responder:start_link(
                      Info#{silent        => true,
                            socket_opener => loopback_opener()}),
        try
            Port = macula_bootstrap_mdns_responder:port(Pid),
            Query = build_query("_macula._udp.local", any, 7),
            ?assertEqual(timeout, ask_responder_expect_silence(Port, Query, 200))
        after
            macula_bootstrap_mdns_responder:stop(Pid)
        end
     end}.

responder_set_silent_toggles_test_() ->
    {timeout, 5,
     fun() ->
        Info = sample_info(),
        {ok, Pid} = macula_bootstrap_mdns_responder:start_link(
                      Info#{socket_opener => loopback_opener()}),
        try
            Port = macula_bootstrap_mdns_responder:port(Pid),
            Query = build_query("_macula._udp.local", any, 11),
            ?assertMatch(<<_/binary>>, ask_responder(Port, Query)),
            ok = macula_bootstrap_mdns_responder:set_silent(Pid, true),
            ?assertEqual(timeout,
                         ask_responder_expect_silence(Port, Query, 200)),
            ok = macula_bootstrap_mdns_responder:set_silent(Pid, false),
            ?assertMatch(<<_/binary>>, ask_responder(Port, Query))
        after
            macula_bootstrap_mdns_responder:stop(Pid)
        end
     end}.

responder_ignores_non_service_query_test_() ->
    {timeout, 5,
     fun() ->
        Info = sample_info(),
        {ok, Pid} = macula_bootstrap_mdns_responder:start_link(
                      Info#{socket_opener => loopback_opener()}),
        try
            Port = macula_bootstrap_mdns_responder:port(Pid),
            Query = build_query("_other._udp.local", any, 3),
            ?assertEqual(timeout,
                         ask_responder_expect_silence(Port, Query, 200))
        after
            macula_bootstrap_mdns_responder:stop(Pid)
        end
     end}.

responder_open_failure_stops_init_test() ->
    Info = sample_info(),
    Opts = Info#{socket_opener =>
                     fun() -> {error, eaddrinuse} end},
    Prev = process_flag(trap_exit, true),
    try
        ?assertMatch({error, {open_failed, eaddrinuse}},
                     macula_bootstrap_mdns_responder:start_link(Opts)),
        drain_exits()
    after
        process_flag(trap_exit, Prev)
    end.

drain_exits() ->
    receive {'EXIT', _, _} -> drain_exits() after 0 -> ok end.

%%==================================================================
%% Helpers
%%==================================================================

sample_info() ->
    #{node_id => crypto:strong_rand_bytes(32),
      port    => 7000,
      tier    => 0}.

loopback_opener() ->
    fun() ->
        gen_udp:open(0, [inet6, binary, {active, once},
                         {ip, {0, 0, 0, 0, 0, 0, 0, 1}}])
    end.

ask_responder(Port, Query) ->
    {ok, ClientSock} = gen_udp:open(0, [inet6, binary, {active, false}]),
    try
        ok = gen_udp:send(ClientSock,
                          {0, 0, 0, 0, 0, 0, 0, 1}, Port, Query),
        {ok, {_Addr, _SPort, Bin}} = gen_udp:recv(ClientSock, 65535, 1000),
        Bin
    after
        gen_udp:close(ClientSock)
    end.

ask_responder_expect_silence(Port, Query, Timeout) ->
    {ok, ClientSock} = gen_udp:open(0, [inet6, binary, {active, false}]),
    try
        ok = gen_udp:send(ClientSock,
                          {0, 0, 0, 0, 0, 0, 0, 1}, Port, Query),
        case gen_udp:recv(ClientSock, 65535, Timeout) of
            {ok, _}         -> received;
            {error, timeout} -> timeout
        end
    after
        gen_udp:close(ClientSock)
    end.

build_query(Domain, Type) ->
    build_query(Domain, Type, 1).

build_query(Domain, Type, Id) ->
    Msg = inet_dns:make_msg([
        {header, inet_dns:make_header(
                    [{id, Id}, {qr, false}, {opcode, query},
                     {rd, false}])},
        {qdlist, [inet_dns:make_dns_query(
                    [{domain, Domain}, {type, Type}, {class, in}])]}
    ]),
    iolist_to_binary(inet_dns:encode(Msg)).

build_response_packet(Domain) ->
    RR = inet_dns:make_rr([
        {domain, Domain}, {type, txt}, {class, in}, {ttl, 60},
        {data, ["placeholder"]}
    ]),
    Msg = inet_dns:make_msg([
        {header, inet_dns:make_header(
                    [{id, 2}, {qr, true}, {opcode, query},
                     {rd, false}, {ra, false}, {rcode, 0}])},
        {anlist, [RR]}
    ]),
    iolist_to_binary(inet_dns:encode(Msg)).

strings_to_map(Strings) ->
    maps:from_list([split_pair(iolist_to_binary(S)) || S <- Strings]).

split_pair(Bin) ->
    case binary:split(Bin, <<"=">>) of
        [K, V] -> {K, V};
        [K]    -> {K, <<>>}
    end.

hex(Bin) ->
    string:lowercase(binary_to_list(binary:encode_hex(Bin))).
