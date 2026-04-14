%% @doc mDNS codec for Tier B bootstrap.
%%
%% Tier B (Part 5 §5) discovers nearby stations via IPv6 link-local
%% mDNS (`ff02::fb` port 5353). The advertised service is a single
%% record-set at the flat name `_macula._udp.local' carrying TXT
%% fields:
%%
%% ```
%% _macula._udp.local  IN TXT
%%   "node_id=<64 hex>"
%%   "port=<1..65535>"
%%   "tier=<0..4>"
%% '''
%%
%% This module is the <b>pure codec</b> half — building queries,
%% parsing response packets, and mapping TXT rdata to candidate
%% peers. Per Part 5 §5.1 the TXT values alone are NOT trusted; a
%% QUIC handshake against the candidate's source address is what
%% upgrades a candidate into a `verified_peer()'. That handshake
%% lives in `hecate_bootstrap_tier_b'.
%%
%% The wire format is standard DNS, so we reuse OTP's
%% `inet_dns'. mDNS-specific framing quirks (unicast-response bit,
%% cache-flush bit) are omitted — we query `class=in' and parse
%% whatever the responder sends back.
-module(hecate_bootstrap_mdns).

-export([
    service_name/0,
    multicast_group/0,
    multicast_port/0,
    build_query/0, build_query/1, build_query/2,
    parse_response/1,
    extract_candidates/1
]).

-export_type([candidate/0, extract_error/0, parse_error/0, query_id/0]).

-define(SERVICE_NAME,    "_macula._udp.local").
-define(MULTICAST_GROUP, {16#FF02, 0, 0, 0, 0, 0, 0, 16#FB}).
-define(MULTICAST_PORT,  5353).

-type query_id() :: 0..65535.

-type candidate() :: #{
    node_id := macula_identity:pubkey(),
    port    := 1..65535,
    tier    := 0..4
}.

-type extract_error() ::
        missing_node_id
      | missing_port
      | missing_tier
      | bad_node_id
      | bad_port
      | bad_tier.

-type parse_error() :: decode_failed.

%%==================================================================
%% Well-known constants
%%==================================================================

-spec service_name() -> string().
service_name() -> ?SERVICE_NAME.

-spec multicast_group() -> inet:ip6_address().
multicast_group() -> ?MULTICAST_GROUP.

-spec multicast_port() -> 1..65535.
multicast_port() -> ?MULTICAST_PORT.

%%==================================================================
%% Query construction
%%==================================================================

-spec build_query() -> {query_id(), binary()}.
build_query() -> build_query(?SERVICE_NAME).

-spec build_query(string()) -> {query_id(), binary()}.
build_query(Domain) -> build_query(Domain, rand:uniform(65536) - 1).

-spec build_query(string(), query_id()) -> {query_id(), binary()}.
build_query(Domain, Id) when is_integer(Id), Id >= 0, Id =< 65535 ->
    Msg = inet_dns:make_msg([
        {header, inet_dns:make_header(
                    [{id, Id}, {qr, false}, {opcode, query},
                     {rd, false}])},
        {qdlist, [inet_dns:make_dns_query(
                    [{domain, Domain}, {type, any}, {class, in}])]}
    ]),
    {Id, iolist_to_binary(inet_dns:encode(Msg))}.

%%==================================================================
%% Response parsing
%%==================================================================

%% @doc Decode an mDNS response packet. Returns a list of answer
%% records as `#{name, type, data}' maps. Caller decides which are
%% interesting (typically TXT records at the service name).
-spec parse_response(binary()) ->
          {ok, [#{name := string(), type := atom(), data := term()}]}
        | {error, parse_error()}.
parse_response(Bin) when is_binary(Bin) ->
    decode(inet_dns:decode(Bin)).

decode({ok, {dns_rec, _H, _Qs, Answers, _Ns, _Ar}}) ->
    {ok, [answer_map(A) || A <- Answers]};
decode({error, _}) ->
    {error, decode_failed}.

answer_map({dns_rr, Name, Type, _Class, _Cnt, _TTL, Data, _Tm, _Bm, _F}) ->
    #{name => Name, type => Type, data => Data}.

%%==================================================================
%% Candidate extraction
%%==================================================================

%% @doc Scan a list of parsed answers for TXT records at our service
%% name and turn each into a `candidate()'. Malformed TXT entries
%% (missing keys, bad hex, out-of-range values) are silently dropped.
-spec extract_candidates([#{name := string(), type := atom(),
                            data := term()}]) -> [candidate()].
extract_candidates(Answers) ->
    [C || A <- Answers,
          is_txt_for_service(A),
          {ok, C} <- [candidate_from(A)]].

is_txt_for_service(#{name := Name, type := txt}) ->
    string:equal(Name, ?SERVICE_NAME, true);
is_txt_for_service(_) ->
    false.

candidate_from(#{data := Strings}) ->
    build_candidate(txt_to_kv(Strings)).

txt_to_kv(Strings) ->
    maps:from_list([parse_pair(iolist_to_binary(S)) || S <- Strings]).

parse_pair(Bin) ->
    split_pair(binary:split(Bin, <<"=">>)).

split_pair([K, V])  -> {K, V};
split_pair([K])     -> {K, <<>>}.

build_candidate(KV) ->
    chain([
        fun() -> need(<<"node_id">>, KV, missing_node_id) end,
        fun(Hex) -> decode_node_id(Hex) end,
        fun(NodeId) -> need_pair(KV, NodeId) end,
        fun({NodeId, Port, Tier}) ->
                {ok, #{node_id => NodeId, port => Port, tier => Tier}}
        end
    ]).

need(Key, KV, Err) ->
    case maps:find(Key, KV) of
        {ok, V} -> {ok, V};
        error   -> {error, Err}
    end.

need_pair(KV, NodeId) ->
    chain([
        fun() -> need(<<"port">>, KV, missing_port) end,
        fun(P) -> parse_port(P) end,
        fun(Port) -> join_tier(KV, NodeId, Port) end
    ]).

join_tier(KV, NodeId, Port) ->
    chain([
        fun() -> need(<<"tier">>, KV, missing_tier) end,
        fun(T) -> parse_tier(T) end,
        fun(Tier) -> {ok, {NodeId, Port, Tier}} end
    ]).

decode_node_id(Hex) when byte_size(Hex) =:= 64 ->
    try binary:decode_hex(Hex) of
        Bin when byte_size(Bin) =:= 32 -> {ok, Bin};
        _                              -> {error, bad_node_id}
    catch
        _:_ -> {error, bad_node_id}
    end;
decode_node_id(_) ->
    {error, bad_node_id}.

parse_port(Bin) ->
    parse_int(Bin, 1, 65535, bad_port).

parse_tier(Bin) ->
    parse_int(Bin, 0, 4, bad_tier).

parse_int(Bin, Lo, Hi, Err) ->
    try binary_to_integer(Bin) of
        N when N >= Lo, N =< Hi -> {ok, N};
        _                       -> {error, Err}
    catch
        _:_ -> {error, Err}
    end.

%% Sequential chain over `{ok, V} | {error, _}'. First step is arity
%% 0; subsequent steps take the previous step's unwrapped value.
chain([Step | Rest]) -> chain(Step(), Rest).

chain({error, _} = E, _Rest)         -> E;
chain({ok, V},        [])            -> {ok, V};
chain({ok, V},        [Step | Rest]) -> chain(Step(V), Rest).
