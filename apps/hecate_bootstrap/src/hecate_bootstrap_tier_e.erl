%% @doc Tier E — social / out-of-band peer ingestion.
%%
%% The last-resort cascade tier (Part 5 §8). Operator supplies one or
%% more signed peer URLs (see `hecate_bootstrap_peer_url'); each URL
%% is decoded and signature-verified. Verified peers are returned to
%% the orchestrator.
%%
%% Tier E is instantaneous — it does no network I/O — so it runs with
%% zero stagger. When URLs are provided it typically wins the cascade
%% before any other tier gets a chance. When no URLs are provided it
%% returns `{error, no_urls}' and the orchestrator falls through.
%%
%% Probe options:
%% <ul>
%%   <li>`peer_urls' :: [binary()] — operator-supplied URLs</li>
%% </ul>
%%
%% Reference: plans/PLAN_MACULA_V2_PART5_BOOTSTRAP.md §8.
-module(hecate_bootstrap_tier_e).
-behaviour(hecate_bootstrap_tier).

-export([tier/0, stagger_ms/0, probe/1]).

tier() -> e.

stagger_ms() -> 0.

probe(Opts) ->
    collect(maps:get(peer_urls, Opts, [])).

collect([])   -> {error, no_urls};
collect(Urls) -> collect(Urls, []).

collect([], [])    -> {error, all_urls_invalid};
collect([], Acc)   -> {ok, lists:reverse(Acc)};
collect([U | Rest], Acc) ->
    accumulate(hecate_bootstrap_peer_url:decode(U), Rest, Acc).

accumulate({ok, Record, Addrs}, Rest, Acc) ->
    Peer = #{
        node_id   => macula_record:key(Record),
        record    => Record,
        addresses => Addrs,
        tier      => e,
        via       => ?MODULE
    },
    collect(Rest, [Peer | Acc]);
accumulate({error, _Reason}, Rest, Acc) ->
    %% Tolerant: a bad URL in the paste shouldn't discard the rest.
    collect(Rest, Acc).
