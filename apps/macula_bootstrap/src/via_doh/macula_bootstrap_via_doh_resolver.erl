%% @doc RFC 8484 DNS-over-HTTPS request/response codec + resolver core.
%%
%% via_doh (Part 5 §4.1) fetches foundation-signed `foundation_seed_list'
%% records via DoH. The well-known zone name for a foundation pubkey
%% is built from a base32 encoding of the pubkey; the record bytes
%% are published as one or more TXT character-strings which this
%% module concatenates verbatim.
%%
%% == Layering ==
%%
%% Two responsibilities, two entry points:
%% <ul>
%%   <li><b>Pure codec</b> — `query_domain/2', `build_query/1,2',
%%       `parse_response/3'. All deterministic; tested without any
%%       network I/O.</li>
%%   <li><b>Resolver core</b> — `resolve/4'. Composes the pure codec
%%       with a caller-supplied HTTP `SendFun'. Concrete DoH
%%       behaviours (e.g. `macula_bootstrap_via_doh_http') plug a real
%%       `inets:httpc' send function in here; tests plug a canned
%%       response function.</li>
%% </ul>
%%
%% Wire format is handled by the undocumented-but-stable `inet_dns'
%% OTP module. No DNSSEC — trust is earned at the record layer
%% (`macula_foundation:verify_record/1') and corroboration layer
%% (`macula_bootstrap_via_doh'), not at the DNS layer.
-module(macula_bootstrap_via_doh_resolver).

-export([
    query_domain/2,
    build_query/1, build_query/2,
    parse_response/3,
    resolve/4
]).

-export_type([
    query_id/0,
    send_fun/0,
    resolve_opts/0,
    parse_error/0,
    resolve_error/0
]).

-type query_id()   :: 0..65535.
-type send_fun()   :: fun((Url :: macula_bootstrap_via_doh_resolver_behaviour:url(),
                            Body :: binary()) ->
                         {ok, binary()} | {error, term()}).

-type resolve_opts() :: #{
    zone_base => binary() | string(),
    timeout_ms => pos_integer()
}.

-type parse_error() ::
        decode_failed
      | not_a_response
      | id_mismatch
      | {rcode, 0..15}
      | no_txt_answer
      | empty_rdata.

-type resolve_error() :: parse_error() | {http, term()}.

-define(DEFAULT_ZONE_BASE, <<"macula.io">>).

%%==================================================================
%% Pure codec
%%==================================================================

%% @doc Build the DoH query name for `Pubkey' under `ZoneBase'.
%% Returns a plain Erlang string (what `inet_dns' wants for domains).
-spec query_domain(macula_identity:pubkey(), binary() | string()) ->
          string().
query_domain(Pubkey, ZoneBase)
  when is_binary(Pubkey), byte_size(Pubkey) =:= 32 ->
    Label = macula_bootstrap_via_doh_base32:encode(Pubkey),
    Zone  = iolist_to_binary(ZoneBase),
    binary_to_list(<<"_pkarr.", Label/binary, ".", Zone/binary>>).

%% @doc Build a DoH TXT query for `Domain' with a random transaction
%% id.
-spec build_query(string()) -> {query_id(), binary()}.
build_query(Domain) ->
    build_query(Domain, rand:uniform(65536) - 1).

%% @doc Build a DoH TXT query for `Domain' using the caller-supplied
%% transaction id. Useful for tests that want deterministic packets.
-spec build_query(string(), query_id()) -> {query_id(), binary()}.
build_query(Domain, Id) when is_integer(Id), Id >= 0, Id =< 65535 ->
    Msg = inet_dns:make_msg([
        {header, inet_dns:make_header(
                    [{id, Id}, {qr, false}, {opcode, query},
                     {rd, true}])},
        {qdlist, [inet_dns:make_dns_query(
                    [{domain, Domain}, {type, txt}, {class, in}])]}
    ]),
    {Id, iolist_to_binary(inet_dns:encode(Msg))}.

%% @doc Parse a DoH response: verify the id, check the RCODE, and
%% return the concatenated TXT rdata for answers matching `Domain'.
-spec parse_response(binary(), query_id(), string()) ->
          {ok, binary()} | {error, parse_error()}.
parse_response(Bin, ExpectedId, Domain) when is_binary(Bin) ->
    decode_and_check(inet_dns:decode(Bin), ExpectedId, Domain).

decode_and_check({ok, DnsRec}, ExpectedId, Domain) ->
    check_header(DnsRec, ExpectedId, Domain);
decode_and_check({error, _}, _Id, _Domain) ->
    {error, decode_failed}.

check_header({dns_rec, Header, _Qs, Answers, _Ns, _Ar} = _Rec,
             ExpectedId, Domain) ->
    {dns_header, Id, Qr, _Op, _Aa, _Tc, _Rd, _Ra, _Pr, RCode} = Header,
    header_ok(Id, Qr, RCode, ExpectedId, Answers, Domain).

header_ok(Id, _Qr, _RCode, ExpectedId, _Answers, _Domain)
  when Id =/= ExpectedId ->
    {error, id_mismatch};
header_ok(_Id, false, _RCode, _ExpectedId, _Answers, _Domain) ->
    {error, not_a_response};
header_ok(_Id, true, 0, _ExpectedId, Answers, Domain) ->
    extract_txt(Answers, Domain);
header_ok(_Id, true, RCode, _ExpectedId, _Answers, _Domain) ->
    {error, {rcode, RCode}}.

extract_txt(Answers, Domain) ->
    Rdata = [Strings || {dns_rr, Name, txt, _Class, _Cnt, _TTL,
                         Strings, _Tm, _Bm, _Func} <- Answers,
                        names_equal(Name, Domain)],
    join_rdata(Rdata).

join_rdata([])   -> {error, no_txt_answer};
join_rdata(Lists) ->
    Bin = iolist_to_binary(Lists),
    case Bin of
        <<>> -> {error, empty_rdata};
        _    -> {ok, Bin}
    end.

names_equal(A, B) when is_list(A), is_list(B) ->
    string:equal(A, B, true).

%%==================================================================
%% Resolver core
%%==================================================================

%% @doc High-level resolve: build the DoH query, ship it via
%% `SendFun', parse the response, and return the foundation record
%% bytes.
-spec resolve(macula_bootstrap_via_doh_resolver_behaviour:url(),
              macula_identity:pubkey(),
              resolve_opts(),
              send_fun()) -> {ok, binary()} | {error, resolve_error()}.
resolve(Url, Pubkey, Opts, SendFun)
  when is_binary(Pubkey), byte_size(Pubkey) =:= 32,
       is_function(SendFun, 2) ->
    ZoneBase = maps:get(zone_base, Opts, ?DEFAULT_ZONE_BASE),
    Domain   = query_domain(Pubkey, ZoneBase),
    {Id, Query} = build_query(Domain),
    dispatch(SendFun(Url, Query), Id, Domain).

dispatch({ok, RespBin}, Id, Domain) -> parse_response(RespBin, Id, Domain);
dispatch({error, _} = E, _Id, _Domain) -> wrap_http_error(E).

wrap_http_error({error, R}) -> {error, {http, R}}.
