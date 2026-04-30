%% @doc Tier B — link-local mDNS discovery (Part 5 §5).
%%
%% Tier B sends a single IPv6 mDNS query to the link-local service
%% name `_macula._udp.local' and collects TXT advertisements from
%% nearby stations. TXT values alone are <b>not</b> trusted — an
%% adversary on the same LAN could spoof any advertisement. For each
%% mDNS candidate Tier B runs a per-peer QUIC handshake via a
%% caller-supplied function; the peer proves possession of the
%% private key behind the advertised NodeId, hands back its signed
%% `node_record', and only then becomes a `verified_peer()'.
%%
%% == Probe options ==
%%
%% <ul>
%%   <li>`udp_transport' :: module() — implementation of
%%       `macula_bootstrap_mdns_transport'. Defaults to
%%       `macula_bootstrap_mdns_udp' (real multicast UDP).</li>
%%   <li>`handshake_fun' :: `handshake_fun()' — the QUIC
%%       corroboration step. Takes the mDNS source address, the
%%       advertised port, and the advertised NodeId, and returns
%%       the peer's signed `node_record'. Default returns
%%       `{error, handshake_not_configured}' so a misconfigured
%%       station yields zero peers rather than trusting TXT blindly.
%%   </li>
%%   <li>`timeout_ms' :: pos_integer() — total budget (default
%%       2000 ms).</li>
%% </ul>
%%
%% == Stagger ==
%%
%% Tier B enters the cascade 200 ms after Tier A per Part 5 §3 —
%% Tier A gets a head start (it's typically fastest), but if it
%% hasn't returned in 200 ms, B begins in parallel rather than
%% blocking on A's timeout.
-module(macula_bootstrap_tier_b).
-behaviour(macula_bootstrap_tier).

-export([tier/0, stagger_ms/0, probe/1]).

-export_type([handshake_fun/0, probe_opts/0]).

-type handshake_fun() ::
        fun((SrcAddr :: inet:ip6_address(),
             Port    :: 1..65535,
             ExpectedNodeId :: macula_identity:pubkey()) ->
              {ok, macula_record:record()} | {error, term()}).

-type probe_opts() :: #{
    udp_transport => module(),
    handshake_fun => handshake_fun(),
    timeout_ms    => pos_integer()
}.

-define(DEFAULT_TIMEOUT, 2_000).

tier()       -> b.
stagger_ms() -> 200.

-spec probe(probe_opts()) -> macula_bootstrap_tier:probe_result().
probe(Opts) ->
    UdpMod     = maps:get(udp_transport, Opts, macula_bootstrap_mdns_udp),
    Handshake  = maps:get(handshake_fun, Opts, fun not_configured/3),
    Timeout    = maps:get(timeout_ms,    Opts, ?DEFAULT_TIMEOUT),
    run(UdpMod, Handshake, Timeout).

run(UdpMod, Handshake, Timeout) ->
    {_Id, Query} = macula_bootstrap_mdns:build_query(),
    Replies      = UdpMod:query(Query, Timeout),
    Candidates   = candidates(Replies),
    deduplicate(verify_all(Candidates, Handshake)).

%%------------------------------------------------------------------
%% Candidate assembly
%%------------------------------------------------------------------

candidates(Replies) ->
    lists:flatten([answers_to_candidates(Src, Bin) || {Src, Bin} <- Replies]).

answers_to_candidates(Src, Bin) ->
    case macula_bootstrap_mdns:parse_response(Bin) of
        {ok, Answers} ->
            [C#{src_addr => Src}
             || C <- macula_bootstrap_mdns:extract_candidates(Answers)];
        {error, _} ->
            []
    end.

%%------------------------------------------------------------------
%% Verification step (handshake + identity match)
%%------------------------------------------------------------------

verify_all(Candidates, Handshake) ->
    lists:filtermap(
      fun(C) -> verify_one(C, Handshake) end,
      Candidates).

verify_one(#{src_addr := Src, port := Port, node_id := NodeId} = C,
           Handshake) ->
    classify(Handshake(Src, Port, NodeId), C).

classify({ok, Record}, C)     -> match_identity(Record, C);
classify({error, _Reason}, _C) -> false.

match_identity(Record, #{node_id := Expected} = C) ->
    check_key(macula_record:key(Record), Expected, Record, C).

check_key(Expected, Expected, Record, C) ->
    verified(macula_record:verify(Record), Record, C);
check_key(_Other, _Expected, _Record, _C) ->
    false.

verified({ok, _}, Record, C)  -> {true, to_peer(Record, C)};
verified({error, _}, _Record, _C) -> false.

to_peer(Record, #{src_addr := Src, port := Port}) ->
    #{
        node_id   => macula_record:key(Record),
        record    => Record,
        addresses => [#{ {text, <<"ip">>}   => {text, ip_to_text(Src)},
                         {text, <<"port">>} => Port }],
        tier      => b,
        via       => ?MODULE
    }.

ip_to_text(Addr) ->
    iolist_to_binary(inet:ntoa(Addr)).

%%------------------------------------------------------------------
%% Deduplication — keep one peer per NodeId, preserving first
%% observation (the cascade already tolerates duplicates downstream,
%% but deduping here keeps the routing-table seed tight).
%%------------------------------------------------------------------

deduplicate(Peers) ->
    {ok, element(1, lists:foldr(fun dedup/2, {[], #{}}, Peers))}.

dedup(#{node_id := Id} = P, {Acc, Seen}) ->
    case maps:is_key(Id, Seen) of
        true  -> {Acc, Seen};
        false -> {[P | Acc], Seen#{Id => true}}
    end.

%%------------------------------------------------------------------
%% Default handshake — refuse. A production station MUST install a
%% real QUIC handshake via the `handshake_fun' opt.
%%------------------------------------------------------------------

not_configured(_Src, _Port, _NodeId) ->
    {error, handshake_not_configured}.
