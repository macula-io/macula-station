%% @doc Pure DHT protocol helpers — frame construction, verification,
%% and routing-table-entry to station_ref conversion.
%%
%% This module is a thin glue layer between the DHT state
%% (`macula_dht_routing_table' / `macula_dht_entry') and the
%% wire-format constructors exposed by `macula_frame'. It owns
%% <em>no</em> state — everything is deterministic from its arguments.
%%
%% Split out from `macula_dht_server' so wire-level operations can be
%% unit-tested without a gen_server, and so future sessions (3.7
%% FIND_VALUE, 3.8 STORE, 3.9 REPLICATE) can add builders without
%% growing the server module further.
%%
%% Reference: plans/PLAN_MACULA_V2_PART6_PROTOCOL.md §7,
%% plans/PLAN_PHASE_3_BREAKDOWN.md Session 3.5.
-module(macula_dht_protocol).

-export([
    build_ping/1,
    build_pong/2,
    build_find_node/4,
    build_nodes_reply/3,
    build_find_value/3,
    build_value_reply/3,
    build_store/2,
    build_store_ack/4,
    verify/2,
    entry_to_station_ref/1,
    entry_to_station_ref/2,
    tier_to_int/1,
    int_to_tier/1,
    new_nonce/0
]).

-export_type([nonce/0]).

-define(NONCE_BYTES, 16).

-type nonce() :: <<_:128>>.

%%=====================================================================
%% Frame builders
%%=====================================================================

%% @doc Build a signed PING frame and return the fresh nonce alongside
%% it. The nonce is what the caller uses to correlate the incoming
%% PONG.
-spec build_ping(macula_identity:key_pair()) ->
        {macula_frame:frame(), nonce()}.
build_ping(Identity) ->
    Nonce = new_nonce(),
    Frame = macula_frame:sign(macula_frame:ping(#{nonce => Nonce}), Identity),
    {Frame, Nonce}.

%% @doc Build a signed PONG echoing the nonce from an incoming PING.
-spec build_pong(nonce(), macula_identity:key_pair()) -> macula_frame:frame().
build_pong(Nonce, Identity) when is_binary(Nonce), byte_size(Nonce) =:= ?NONCE_BYTES ->
    macula_frame:sign(macula_frame:pong(#{nonce => Nonce}), Identity).

%% @doc Build a signed FIND_NODE request.
-spec build_find_node(Key :: macula_dht_xor:id(),
                      Origin :: macula_identity:pubkey(),
                      Depth :: non_neg_integer(),
                      macula_identity:key_pair()) -> macula_frame:frame().
build_find_node(<<_:256>> = Key, <<_:256>> = Origin, Depth, Identity)
  when is_integer(Depth), Depth >= 0 ->
    macula_frame:sign(
      macula_frame:find_node(#{key => Key, origin => Origin, depth => Depth}),
      Identity).

%% @doc Build a signed NODES response from a list of routing-table
%% entries. Converts each entry to a `station_ref()' inline.
-spec build_nodes_reply(Key :: macula_dht_xor:id(),
                        [macula_dht_entry:entry()],
                        macula_identity:key_pair()) -> macula_frame:frame().
build_nodes_reply(<<_:256>> = Key, Entries, Identity) ->
    Refs = [entry_to_station_ref(E) || E <- Entries],
    macula_frame:sign(
      macula_frame:nodes(#{key => Key, nodes => Refs}),
      Identity).

%% @doc Build a signed FIND_VALUE request.
-spec build_find_value(Key :: macula_dht_xor:id(),
                       Origin :: macula_identity:pubkey(),
                       macula_identity:key_pair()) -> macula_frame:frame().
build_find_value(<<_:256>> = Key, <<_:256>> = Origin, Identity) ->
    macula_frame:sign(
      macula_frame:find_value(#{key => Key, origin => Origin}),
      Identity).

%% @doc Build a signed VALUE response carrying one or more records.
-spec build_value_reply(Key :: macula_dht_xor:id(),
                        [macula_record:record()],
                        macula_identity:key_pair()) -> macula_frame:frame().
build_value_reply(<<_:256>> = Key, Records, Identity) when is_list(Records) ->
    macula_frame:sign(
      macula_frame:value(#{key => Key, records => Records}),
      Identity).

%% @doc Build a signed STORE request carrying a record to persist.
-spec build_store(macula_record:record(),
                  macula_identity:key_pair()) -> macula_frame:frame().
build_store(Record, Identity) when is_map(Record) ->
    macula_frame:sign(macula_frame:store(#{record => Record}), Identity).

%% @doc Build a signed STORE_ACK for a given storage key.
-spec build_store_ack(macula_dht_xor:id(), boolean(),
                      atom() | undefined,
                      macula_identity:key_pair()) -> macula_frame:frame().
build_store_ack(<<_:256>> = Key, Stored, Reason, Identity)
  when is_boolean(Stored) ->
    Spec = #{key => Key, stored => Stored, reason => Reason},
    macula_frame:sign(macula_frame:store_ack(Spec), Identity).

%%=====================================================================
%% Verification
%%=====================================================================

%% @doc Verify an incoming DHT frame against the claimed sender's
%% NodeId. In V2 every NodeId is itself an Ed25519 pubkey, so the
%% NodeId is the verification key.
-spec verify(macula_frame:frame(), macula_identity:pubkey()) ->
        {ok, macula_frame:frame()} | {error, term()}.
verify(Frame, <<_:256>> = FromNodeId) ->
    macula_frame:verify(Frame, FromNodeId).

%%=====================================================================
%% Entry → station_ref conversion
%%=====================================================================

%% @doc Convert a routing-table entry to a wire-format `station_ref()'.
%% Uses `erlang:system_time(millisecond)' as the wall-clock reference
%% and derives the entry's `last_seen_at' in wall-clock ms from its
%% monotonic timestamp.
-spec entry_to_station_ref(macula_dht_entry:entry()) ->
        macula_frame:station_ref().
entry_to_station_ref(Entry) ->
    entry_to_station_ref(Entry, erlang:monotonic_time(millisecond)).

-spec entry_to_station_ref(macula_dht_entry:entry(), integer()) ->
        macula_frame:station_ref().
entry_to_station_ref(Entry, NowMono) ->
    WallNow = erlang:system_time(millisecond),
    LastSeenMono = macula_dht_entry:last_seen(Entry),
    LastSeenWall = WallNow - (NowMono - LastSeenMono),
    macula_frame:station_ref(#{
        node_id      => macula_dht_entry:node_id(Entry),
        station_id   => macula_dht_entry:node_id(Entry),
        %% Publish the entry's real endpoints. This was hardcoded `[]', which
        %% made every ref in every NODES reply address-less and therefore
        %% undialable: a receiver could add the node to its routing table and
        %% still never reach it, because
        %% `macula_station_dht_transport:send_frame/2' resolves only through an
        %% EXISTING connection. An entry with no endpoints still publishes
        %% none, which is the honest answer for an inbound-only peer whose
        %% listening address we do not know.
        %% Guarded by macula_dht_endpoint_propagation_tests, which asserts the
        %% address survives a real CBOR encode/decode rather than only the
        %% call site.
        addresses    => macula_dht_entry:endpoints(Entry),
        tier         => tier_to_int(macula_dht_entry:tier(Entry)),
        asn          => macula_dht_entry:asn(Entry),
        country      => macula_dht_entry:country(Entry),
        last_seen_at => max(1, LastSeenWall)
    }).

%%=====================================================================
%% Tier mapping
%%=====================================================================

-spec tier_to_int(macula_dht_entry:tier()) -> 0..4.
tier_to_int(t0) -> 0;
tier_to_int(t1) -> 1;
tier_to_int(t2) -> 2;
tier_to_int(t3) -> 3.

-spec int_to_tier(0..4) -> macula_dht_entry:tier().
int_to_tier(0) -> t0;
int_to_tier(1) -> t1;
int_to_tier(2) -> t2;
int_to_tier(3) -> t3;
int_to_tier(4) -> t3.

%%=====================================================================
%% Nonce
%%=====================================================================

-spec new_nonce() -> nonce().
new_nonce() ->
    crypto:strong_rand_bytes(?NONCE_BYTES).
