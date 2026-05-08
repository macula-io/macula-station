%% @doc via_operator_paste — social / out-of-band peer ingestion.
%%
%% The last-resort cascade strategy (Part 5 §8). Operator supplies one or
%% more signed peer URLs (see `macula_bootstrap_via_operator_paste_peer_url'); each URL
%% is decoded and signature-verified. Verified peers are returned to
%% the orchestrator.
%%
%% via_operator_paste is instantaneous — it does no network I/O — so it runs with
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
-module(macula_bootstrap_via_operator_paste).
-behaviour(macula_bootstrap_peer_discoverer).

-export([strategy/0, stagger_ms/0, discover/1]).

strategy() -> via_operator_paste.

stagger_ms() -> 0.

discover(Opts) ->
    collect(maps:get(peer_urls, Opts, [])).

collect([])   -> {error, no_urls};
collect(Urls) -> collect(Urls, []).

collect([], [])    -> {error, all_urls_invalid};
collect([], Acc)   -> {ok, lists:reverse(Acc)};
collect([U | Rest], Acc) ->
    accumulate(macula_bootstrap_via_operator_paste_peer_url:decode(U), Rest, Acc).

accumulate({ok, Record, Addrs}, Rest, Acc) ->
    Peer = #{
        node_id   => macula_record:key(Record),
        record    => Record,
        addresses => Addrs,
        strategy  => via_operator_paste,
        via       => ?MODULE
    },
    collect(Rest, [Peer | Acc]);
accumulate({error, _Reason}, Rest, Acc) ->
    %% Tolerant: a bad URL in the paste shouldn't discard the rest.
    collect(Rest, Acc).
