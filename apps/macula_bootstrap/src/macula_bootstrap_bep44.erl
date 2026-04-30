%% @doc BEP 44 mutable-item envelope.
%%
%% Tier C bootstraps via Mainline DHT. Mainline BEP 44 defines how
%% to store and retrieve Ed25519-signed mutable data items at a
%% `target' id (SHA-1 of the pubkey + optional salt). The PKARR
%% layer (`pkarr.org') repurposes BEP 44 to publish signed DNS
%% packets addressable by pubkey alone — which is exactly the
%% vehicle we want for the foundation's seed list.
%%
%% This module is the <b>signature-layer codec</b>. It has no
%% knowledge of the DHT RPCs; consumers compose it with a DHT
%% transport.
%%
%% == Signed payload shape (BEP 44 §1) ==
%%
%% Without salt:
%% ```
%% 3:seqi<seq>e1:v<bencoded-value>
%% '''
%% With salt:
%% ```
%% 4:salt<len>:<salt>3:seqi<seq>e1:v<bencoded-value>
%% '''
%%
%% The payload is NOT itself wrapped in `d...e'; it is a flat
%% concatenation of bencoded dict entries in a fixed order. The
%% `v' value is bencoded separately — for a binary value (our
%% usual case) this is `<len>:<bytes>'.
%%
%% == Target id (BEP 44 §2) ==
%%
%% ```
%% target = SHA-1(pubkey)              (no salt)
%% target = SHA-1(pubkey || salt)      (with salt)
%% '''
-module(macula_bootstrap_bep44).

-export([target_id/1, target_id/2,
         signed_payload/2, signed_payload/3,
         sign/3, sign/4,
         verify/1]).

-export_type([item/0, verify_error/0]).

-type item() :: #{
    pubkey := macula_identity:pubkey(),
    seq    := non_neg_integer(),
    value  := binary(),
    sig    := <<_:512>>,
    salt   => binary()
}.

-type verify_error() ::
        signature_invalid
      | bad_pubkey
      | bad_sig
      | bad_seq.

%%==================================================================
%% Target id
%%==================================================================

-spec target_id(macula_identity:pubkey()) -> <<_:160>>.
target_id(Pubkey) when is_binary(Pubkey), byte_size(Pubkey) =:= 32 ->
    crypto:hash(sha, Pubkey).

-spec target_id(macula_identity:pubkey(), binary()) -> <<_:160>>.
target_id(Pubkey, Salt)
  when is_binary(Pubkey), byte_size(Pubkey) =:= 32,
       is_binary(Salt) ->
    crypto:hash(sha, <<Pubkey/binary, Salt/binary>>).

%%==================================================================
%% Signed payload
%%==================================================================

-spec signed_payload(non_neg_integer(), binary()) -> binary().
signed_payload(Seq, Value)
  when is_integer(Seq), Seq >= 0, is_binary(Value) ->
    <<"3:seqi", (integer_to_binary(Seq))/binary, "e",
      "1:v", (bencode_binary(Value))/binary>>.

-spec signed_payload(non_neg_integer(), binary(), binary()) -> binary().
signed_payload(Seq, Value, Salt)
  when is_binary(Salt) ->
    <<"4:salt", (integer_to_binary(byte_size(Salt)))/binary, ":",
      Salt/binary,
      (signed_payload(Seq, Value))/binary>>.

bencode_binary(Bin) ->
    <<(integer_to_binary(byte_size(Bin)))/binary, ":", Bin/binary>>.

%%==================================================================
%% Sign (test / tooling convenience — production signing uses FROST)
%%==================================================================

-spec sign(non_neg_integer(), binary(), macula_identity:key_pair()) ->
          item().
sign(Seq, Value, KeyPair) ->
    Payload = signed_payload(Seq, Value),
    Sig     = macula_identity:sign(Payload, KeyPair),
    #{pubkey => macula_identity:public(KeyPair),
      seq    => Seq,
      value  => Value,
      sig    => Sig}.

-spec sign(non_neg_integer(), binary(), binary(),
           macula_identity:key_pair()) -> item().
sign(Seq, Value, Salt, KeyPair) ->
    Payload = signed_payload(Seq, Value, Salt),
    Sig     = macula_identity:sign(Payload, KeyPair),
    #{pubkey => macula_identity:public(KeyPair),
      seq    => Seq,
      value  => Value,
      salt   => Salt,
      sig    => Sig}.

%%==================================================================
%% Verify
%%==================================================================

-spec verify(item()) -> ok | {error, verify_error()}.
verify(Item) ->
    classify(shape(Item), Item).

classify(ok, Item)                 -> do_verify(Item);
classify({error, _} = E, _Item)    -> E.

shape(#{pubkey := Pk, seq := S, value := V, sig := Sig})
  when is_binary(Pk), byte_size(Pk)  =:= 32,
       is_integer(S), S >= 0,
       is_binary(V),
       is_binary(Sig), byte_size(Sig) =:= 64 ->
    ok;
shape(#{pubkey := Pk}) when not is_binary(Pk) orelse byte_size(Pk) =/= 32 ->
    {error, bad_pubkey};
shape(#{sig := Sig}) when not is_binary(Sig) orelse byte_size(Sig) =/= 64 ->
    {error, bad_sig};
shape(#{seq := S}) when not is_integer(S); S < 0 ->
    {error, bad_seq};
shape(_) ->
    {error, bad_sig}.

do_verify(#{pubkey := Pk, seq := S, value := V, sig := Sig,
            salt := Salt}) ->
    check(macula_identity:verify(
            signed_payload(S, V, Salt), Sig, Pk));
do_verify(#{pubkey := Pk, seq := S, value := V, sig := Sig}) ->
    check(macula_identity:verify(
            signed_payload(S, V), Sig, Pk)).

check(true)  -> ok;
check(false) -> {error, signature_invalid}.
