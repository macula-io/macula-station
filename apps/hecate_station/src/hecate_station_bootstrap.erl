%% @doc Bootstrap → DHT routing-table ingestion.
%%
%% Bridges the output of `hecate_bootstrap:run/0,1' (a list of
%% foundation-verified peers from the five-tier cascade) into the
%% station's local Kademlia routing table via `hecate_dht:observe/2'.
%% The cascade finds peers; this module hands them to the DHT so
%% subsequent lookup walks start from a seeded table.
%%
%% == Design notes ==
%%
%% <ul>
%%   <li><b>Adapter, not a codec.</b> The mapping from a cascade
%%       `verified_peer()' to a `hecate_dht_entry:spec()' is pure
%%       (`to_entry_spec/1'); the side-effectful step
%%       (`ingest/2') iterates and counts outcomes.</li>
%%   <li><b>Metadata defaults.</b> The cascade doesn't always know
%%       the peer's ASN or country — neither foundation seeds nor
%%       mDNS advertisements carry them. We default to `asn => 0'
%%       + `country => &lt;&lt;"??"&gt;&gt;'. The DHT's PING /
%%       FIND_NODE paths later learn the real values and the
%%       routing-table entry gets refined via subsequent observes.
%%       </li>
%%   <li><b>Gateway tier.</b> Foundation seeds carry a `tier' field
%%       (3 or 4 per Part 2 §8); we surface it as
%%       `gateway_tier' on the verified_peer and translate it to the
%%       DHT's atom form (`t3'). Non-foundation peers
%%       (Tier B mDNS, Tier E operator paste) default to `t0'.</li>
%%   <li><b>Location.</b> Lives in `hecate_station' rather than
%%       `hecate_bootstrap' because it bridges two apps and its home
%%       is the orchestration layer that already depends on both.</li>
%% </ul>
-module(hecate_station_bootstrap).

-export([to_entry_spec/1, ingest/2]).

-export_type([ingest_summary/0]).

-type ingest_summary() :: #{
    observed  := non_neg_integer(),
    admitted  := non_neg_integer(),
    touched   := non_neg_integer(),
    replaced  := non_neg_integer(),
    rejected  := non_neg_integer()
}.

-define(DEFAULT_COUNTRY, <<"??">>).
-define(DEFAULT_ASN,     0).

%%==================================================================
%% Pure adapter
%%==================================================================

%% @doc Convert a cascade `verified_peer()' into a
%% `hecate_dht_entry:spec()' with sensible defaults for metadata the
%% cascade does not carry.
-spec to_entry_spec(hecate_bootstrap_tier:verified_peer()) ->
          hecate_dht_entry:spec().
to_entry_spec(#{node_id := NodeId} = Peer) ->
    #{
        node_id   => NodeId,
        endpoints => maps:get(addresses, Peer, []),
        asn       => ?DEFAULT_ASN,
        country   => ?DEFAULT_COUNTRY,
        tier      => dht_tier(Peer)
    }.

dht_tier(#{gateway_tier := 4}) -> t3;   %% DHT entry tier caps at t3
dht_tier(#{gateway_tier := 3}) -> t3;
dht_tier(#{gateway_tier := _}) -> t0;
dht_tier(_)                    -> t0.

%%==================================================================
%% Side-effectful ingest
%%==================================================================

%% @doc Observe every cascade-verified peer into the DHT and return
%% a summary of outcomes. Safe to call on an empty peer list
%% (summary is all-zero).
-spec ingest(hecate_dht:dht(),
             [hecate_bootstrap_tier:verified_peer()]) ->
          ingest_summary().
ingest(Dht, Peers) when is_list(Peers) ->
    lists:foldl(fun(P, Acc) -> absorb(observe(Dht, P), Acc) end,
                zero_summary(),
                Peers).

observe(Dht, Peer) ->
    hecate_dht:observe(Dht, to_entry_spec(Peer)).

absorb(admitted,         S) -> bump(admitted, S);
absorb(touched,          S) -> bump(touched, S);
absorb({replaced, _},    S) -> bump(replaced, S);
absorb(rejected,         S) -> bump(rejected, S).

bump(Key, S) ->
    S#{observed := maps:get(observed, S) + 1,
       Key     := maps:get(Key, S) + 1}.

zero_summary() ->
    #{observed => 0, admitted => 0, touched => 0,
      replaced => 0, rejected => 0}.
