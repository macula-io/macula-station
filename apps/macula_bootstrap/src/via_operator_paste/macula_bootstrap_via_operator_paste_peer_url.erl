%% @doc Tier E signed peer-URL codec.
%%
%% The "social / out-of-band" tier (Part 5 §8) lets an operator paste
%% a single URL into the station to add a known peer when all
%% automated discovery paths have failed. The URL wraps a signed
%% `node_record' so the receiving station can verify the peer's
%% NodeId without consulting any external infrastructure.
%%
%% == Format ==
%%
%% ```
%% macula-peer:<base64url(cbor_map)>
%% '''
%%
%% where the CBOR map is:
%%
%% ```cbor
%% {
%%   "r": h'<CBOR-encoded signed node_record>',
%%   "a": [ ip_address_rec(), ... ]    ; hints, unsigned
%% }
%% '''
%%
%% The signed record is the trust anchor. The `a' field carries
%% transport hints (addresses, ports) that the caller uses to reach
%% the peer but which are NOT covered by the record signature —
%% callers must corroborate the hint via the QUIC handshake
%% (the peer proves possession of the NodeId private key there).
%%
%% Reference: plans/PLAN_MACULA_V2_PART5_BOOTSTRAP.md §8.
-module(macula_bootstrap_via_operator_paste_peer_url).

-export([encode/2, decode/1]).

-export_type([peer_url/0, decode_error/0]).

-define(SCHEME, "macula-peer:").
-define(B64_OPTS, #{mode => urlsafe, padding => false}).

-type peer_url() :: binary().

-type decode_error() ::
        bad_scheme
      | bad_base64
      | bad_cbor
      | bad_shape
      | missing_record
      | bad_addresses
      | bad_record
      | signature_invalid
      | expired
      | wrong_type.

%% @doc Build a peer URL from a signed `node_record' and a list of
%% address hints. The record MUST already be signed
%% (`macula_record:sign/2').
-spec encode(macula_record:record(), [map()]) -> peer_url().
encode(Record, Addresses)
  when is_map(Record), is_list(Addresses) ->
    RecBin = macula_record:encode(Record),
    Payload = #{
        {text, <<"r">>} => RecBin,
        {text, <<"a">>} => Addresses
    },
    CborBin = macula_record_cbor:encode(Payload),
    B64 = base64:encode(CborBin, ?B64_OPTS),
    <<?SCHEME, B64/binary>>.

%% @doc Parse and verify a peer URL.
%%
%% On success returns `{ok, Record, Addresses}' where `Record' is the
%% decoded, signature-verified, non-expired `node_record' and
%% `Addresses' is the hint list (may be empty).
-spec decode(binary()) ->
        {ok, macula_record:record(), [map()]} | {error, decode_error()}.
decode(Bin) when is_binary(Bin) ->
    case split_scheme(Bin) of
        {ok, B64}   -> decode_b64(B64);
        {error, R}  -> {error, R}
    end.

split_scheme(<<?SCHEME, Rest/binary>>) -> {ok, Rest};
split_scheme(_)                        -> {error, bad_scheme}.

decode_b64(B64) ->
    %% base64:decode crashes on malformed input; catch at this
    %% trust boundary (operator-supplied string).
    try base64:decode(B64, ?B64_OPTS) of
        Cbor -> decode_cbor(Cbor)
    catch
        _:_ -> {error, bad_base64}
    end.

decode_cbor(Bin) ->
    try macula_record_cbor:decode(Bin) of
        Map when is_map(Map) -> decode_envelope(Map);
        _                    -> {error, bad_cbor}
    catch
        _:_ -> {error, bad_cbor}
    end.

decode_envelope(Map) ->
    RecBin = maps:get({text, <<"r">>}, Map, undefined),
    Addrs  = maps:get({text, <<"a">>}, Map, []),
    validate_shape(RecBin, Addrs).

validate_shape(undefined, _)                         -> {error, missing_record};
validate_shape(Bin, _Addrs) when not is_binary(Bin)  -> {error, bad_shape};
validate_shape(_Bin, Addrs) when not is_list(Addrs)  -> {error, bad_addresses};
validate_shape(Bin, Addrs)                           -> decode_record(Bin, Addrs).

decode_record(Bin, Addrs) ->
    verify_record(macula_record:decode(Bin), Addrs).

verify_record({error, _}, _Addrs)  -> {error, bad_record};
verify_record({ok, Record}, Addrs) ->
    check_type(macula_record:type(Record), Record, Addrs).

check_type(16#01, Record, Addrs) ->
    check_verify(macula_record:verify(Record), Addrs);
check_type(_, _Record, _Addrs) ->
    {error, wrong_type}.

check_verify({ok, Record}, Addrs)    -> {ok, Record, Addrs};
check_verify({error, _} = E, _Addrs) -> E.
