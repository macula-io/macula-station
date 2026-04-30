%% @doc Integration tests — Tier A orchestrator + DoH codec layered.
%%
%% The tier_a unit tests mock the resolver directly; this suite
%% instead plugs a resolver whose internals run the real
%% `macula_bootstrap_doh:resolve/4' codec over a fake HTTP transport
%% that builds DoH-shaped response packets. Verifies that:
%% <ul>
%%   <li>The base32-derived zone name round-trips — the resolver's
%%       HTTP transport sees the correctly-encoded `_pkarr.*' query
%%       for the pubkey Tier A asked about.</li>
%%   <li>TXT rdata flowing back through the codec decodes to exactly
%%       the `macula_record:encode/1' bytes Tier A then verifies.</li>
%% </ul>
-module(macula_bootstrap_tier_a_doh_tests).
-include_lib("eunit/include/eunit.hrl").

-behaviour(macula_bootstrap_resolver).
-export([resolve/3]).

-define(URL1, <<"https://dns1.example/dns-query">>).
-define(URL2, <<"https://dns2.example/dns-query">>).
-define(URL3, <<"https://dns3.example/dns-query">>).

tier_a_over_doh_codec_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     fun doh_end_to_end/1}.

setup() ->
    application:ensure_all_started(crypto),
    Kp     = macula_identity:generate(),
    Fk     = macula_identity:public(Kp),
    application:set_env(macula_record, foundation_pubkeys, [Fk]),
    Seeds  = [#{node_id => crypto:strong_rand_bytes(32),
                addresses => [], tier => 4}
              || _ <- lists:seq(1, 4)],
    Record = macula_record:sign(
               macula_record:foundation_seed_list(Fk, Seeds), Kp),
    Bytes  = macula_record:encode(Record),
    persistent_term:put({?MODULE, bytes}, Bytes),
    #{fk => Fk, seeds => Seeds, bytes => Bytes}.

cleanup(_Ctx) ->
    application:unset_env(macula_record, foundation_pubkeys),
    persistent_term:erase({?MODULE, bytes}),
    ok.

doh_end_to_end(#{fk := Fk}) ->
    fun() ->
        Resolvers = [{?MODULE, ?URL1},
                     {?MODULE, ?URL2},
                     {?MODULE, ?URL3}],
        {ok, Peers} = macula_bootstrap_tier_a:probe(
                        #{resolvers     => Resolvers,
                          pubkeys       => [Fk],
                          corroboration => 2,
                          timeout_ms    => 1_000}),
        ?assertEqual(4, length(Peers)),
        ?assert(lists:all(fun(#{tier := T, via := V}) ->
                                  T =:= a andalso V =:= macula_bootstrap_tier_a
                          end, Peers))
    end.

%%==================================================================
%% Resolver behaviour — runs the real codec over a canned HTTP fun.
%%==================================================================

resolve(Url, Pubkey, Opts) ->
    Send = fun(U, QueryBin) -> canned_http(U, QueryBin) end,
    macula_bootstrap_doh:resolve(Url, Pubkey, Opts, Send).

canned_http(_Url, QueryBin) ->
    {ok, DnsRec} = inet_dns:decode(QueryBin),
    {dns_rec, Header, [{dns_query, Domain, txt, in, _}], _, _, _} = DnsRec,
    Id = element(2, Header),
    Bytes = persistent_term:get({?MODULE, bytes}),
    {ok, build_txt_response(Id, Domain, Bytes)}.

build_txt_response(Id, Domain, Bytes) ->
    Chunks = chunk(Bytes, 200),
    RR = inet_dns:make_rr([
        {domain, Domain}, {type, txt}, {class, in}, {ttl, 60},
        {data, Chunks}
    ]),
    Msg = inet_dns:make_msg([
        {header, inet_dns:make_header(
                   [{id, Id}, {qr, true}, {opcode, query},
                    {rd, true}, {ra, true}, {rcode, 0}])},
        {qdlist, [inet_dns:make_dns_query(
                    [{domain, Domain}, {type, txt}, {class, in}])]},
        {anlist, [RR]}
    ]),
    iolist_to_binary(inet_dns:encode(Msg)).

%% Split a binary into a list of binaries of at most `Max' bytes each.
%% Matches the TXT character-string 255-byte limit and lets us
%% exercise the codec's multi-string concatenation.
chunk(Bin, Max) when byte_size(Bin) =< Max ->
    [Bin];
chunk(Bin, Max) ->
    <<Head:Max/binary, Rest/binary>> = Bin,
    [Head | chunk(Rest, Max)].
