%% Does a dialable address actually survive from one station's routing
%% table into another station's routing table?
%%
%% WHY THIS FILE EXISTS. Nothing in the tree asserted that a routing-table
%% entry ever carries a non-empty endpoint list, and for a long time none
%% did: `macula_dht_protocol:entry_to_station_ref/2' published
%% `addresses => []' unconditionally, and
%% `macula_station_peer_observer:direct_peer_spec/1' observed peers with
%% `endpoints => []'. Every entry on the fleet was therefore address-less,
%% and `macula_station_dht_transport:send_frame/2' — which resolves only
%% through an EXISTING connection — could reach nothing it was not already
%% talking to. Measured consequence on a degree-1 leaf: 5 of 6 chosen
%% custodians unroutable on a single record store.
%%
%% The trap this guards is subtler than the original bug. `macula_frame:encode/1'
%% is generic CBOR over the frame map, and this codebase has repeatedly been
%% bitten by CBOR turning map keys into `{text, Bin}' tuples, so a change that
%% "publishes the endpoints" can look correct at the call site and still arrive
%% empty. The existing `macula_dht_wire_tests' cannot catch that: its harness
%% routes frames as raw Erlang terms and never calls encode/decode at all.
%%
%% So these tests go THROUGH the real codec, and assert on the receiving side.
-module(macula_dht_endpoint_propagation_tests).

-include_lib("eunit/include/eunit.hrl").

-define(EP, #{host => <<"station-fi-helsinki.macula.io">>,
              port => 4433,
              transport => quic}).

%%---------------------------------------------------------------------
%% The wire itself: can a station_ref carry an address at all?
%%---------------------------------------------------------------------

station_ref_addresses_survive_cbor_roundtrip_test() ->
    Ref = macula_frame:station_ref(#{
        node_id      => <<1:256>>,
        station_id   => <<1:256>>,
        addresses    => [?EP],
        tier         => 0,
        asn          => 12345,
        country      => <<"FI">>,
        last_seen_at => 1
    }),
    Decoded = roundtrip_nodes([Ref]),
    ?assertMatch([_], Decoded),
    [Got] = Decoded,
    Addrs = addresses_of(Got),
    ?assertNotEqual([], Addrs),
    ?assertEqual(1, length(Addrs)).

%%---------------------------------------------------------------------
%% The producer: does an entry's endpoint reach the wire?
%%---------------------------------------------------------------------

entry_endpoints_reach_the_wire_test() ->
    Entry = macula_dht_entry:new(#{
        node_id   => <<2:256>>,
        endpoints => [?EP],
        asn       => 12345,
        country   => <<"FI">>,
        tier      => t0
    }, 0),
    Ref = macula_dht_protocol:entry_to_station_ref(Entry),
    ?assertNotEqual([], maps:get(addresses, Ref)),

    [Got] = roundtrip_nodes([Ref]),
    ?assertNotEqual([], addresses_of(Got)).

%% An entry with no endpoints must stay empty rather than acquiring a
%% fabricated one — an address that cannot be dialled is worse than none,
%% because it converts an honest `no_route' into a connection attempt.
entry_without_endpoints_stays_empty_test() ->
    Entry = macula_dht_entry:new(#{
        node_id   => <<3:256>>,
        endpoints => [],
        asn       => 0,
        country   => <<"??">>,
        tier      => t0
    }, 0),
    Ref = macula_dht_protocol:entry_to_station_ref(Entry),
    ?assertEqual([], maps:get(addresses, Ref)).

%%---------------------------------------------------------------------
%% Helpers
%%---------------------------------------------------------------------

%% Build a real NODES frame, sign it, push it through encode/decode, and
%% hand back the refs as the receiver sees them.
roundtrip_nodes(Refs) ->
    Kp    = macula_identity:generate(),
    Frame = macula_frame:sign(
              macula_frame:nodes(#{key => <<0:256>>, nodes => Refs}), Kp),
    Bytes = macula_frame:encode(Frame),
    {ok, Decoded, <<>>} = macula_frame:decode(Bytes),
    nodes_of(Decoded).

%% CBOR may hand back either atom keys or `{text, Bin}' keys depending on
%% how the field was encoded. Read every shape rather than assuming one:
%% assuming a single shape is how a dropped field reads as "empty list"
%% instead of as a failure.
nodes_of(#{nodes := Ns})                  -> Ns;
nodes_of(#{{text, <<"nodes">>} := Ns})    -> Ns;
nodes_of(#{<<"nodes">> := Ns})            -> Ns;
nodes_of(Other)                           -> erlang:error({no_nodes_field, Other}).

addresses_of(#{addresses := A})               -> A;
addresses_of(#{{text, <<"addresses">>} := A}) -> A;
addresses_of(#{<<"addresses">> := A})         -> A;
addresses_of(_)                               -> [].
