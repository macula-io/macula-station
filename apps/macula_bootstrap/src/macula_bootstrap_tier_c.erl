%% @doc Tier C — Mainline DHT bridge (Part 5 §6).
%%
%% Retrieves the foundation's BEP 44 mutable item (a PKARR DNS
%% packet) from the public Mainline DHT at `SHA-1(foundation_pubkey)'.
%% Treats the DHT as a bootstrap <em>signal</em> only: every returned
%% item is verified against a firmware-embedded foundation pubkey
%% and its wrapped record is re-checked by `macula_foundation' — the
%% DHT never becomes a trust anchor.
%%
%% == Pipeline ==
%%
%% For each `FoundationPubkey':
%% <ol>
%%   <li>`macula_bootstrap_bep44:target_id/1' — derive SHA-1 target.</li>
%%   <li>`DhtMod:get_mutable/2' — fetch signed item (may block up to
%%       `timeout_ms').</li>
%%   <li>Item pubkey must match the one we asked about.</li>
%%   <li>`macula_bootstrap_bep44:verify/1' — Ed25519 over BEP 44
%%       payload shape.</li>
%%   <li>Item's `value' is a DNS packet; concat every TXT
%%       character-string into a binary.</li>
%%   <li>`macula_record:decode/1' + `macula_foundation:verify_record/1'
%%       — the inner Macula record must be a foundation-signed type
%%       and still valid.</li>
%%   <li>Emit one `verified_peer()' per seed.</li>
%% </ol>
%%
%% == Stagger ==
%%
%% 500 ms per Part 5 §3 — the DHT lookup is the slowest tier under
%% normal conditions; we give Tier A and B a head start so they
%% aren't starved by DHT latency.
%%
%% == Transport ==
%%
%% Takes a `dht_transport' module implementing
%% `macula_bootstrap_dht_transport'. No default: a misconfigured
%% station gets `{error, no_transport}' rather than pretending to
%% bootstrap without a DHT client.
-module(macula_bootstrap_tier_c).
-behaviour(macula_bootstrap_peer_discoverer).

-export([strategy/0, stagger_ms/0, discover/1]).

-export_type([discover_opts/0]).

-type discover_opts() :: #{
    dht_transport := module(),
    pubkeys       => [macula_identity:pubkey()],
    timeout_ms    => pos_integer()
}.

-define(DEFAULT_TIMEOUT, 10_000).

strategy()   -> via_mainline_dht.
stagger_ms() -> 500.

-spec discover(discover_opts()) -> macula_bootstrap_peer_discoverer:discover_result().
discover(Opts) ->
    DhtMod  = maps:get(dht_transport, Opts, undefined),
    Pubkeys = maps:get(pubkeys,       Opts, macula_foundation:pubkeys()),
    Timeout = maps:get(timeout_ms,    Opts, ?DEFAULT_TIMEOUT),
    run(DhtMod, Pubkeys, Timeout).

run(undefined, _Pubkeys, _Timeout)    -> {error, no_transport};
run(_Mod,      [], _Timeout)          -> {error, no_pubkeys};
run(Mod, Pubkeys, Timeout) ->
    Tag    = make_ref(),
    Parent = self(),
    lists:foreach(
      fun(Pk) -> spawn_worker(Parent, Tag, Mod, Pk, Timeout) end,
      Pubkeys),
    Deadline = erlang:monotonic_time(millisecond) + Timeout + 500,
    collect(length(Pubkeys), Tag, Deadline).

%%------------------------------------------------------------------
%% Fan-out / collect
%%------------------------------------------------------------------

spawn_worker(Parent, Tag, Mod, Pubkey, Timeout) ->
    spawn(fun() ->
                  Parent ! {Tag, fetch_and_verify(Mod, Pubkey, Timeout)}
          end).

collect(0, _Tag, _Deadline) ->
    {error, all_failed};
collect(N, Tag, Deadline) ->
    Remaining = max(Deadline - erlang:monotonic_time(millisecond), 0),
    receive
        {Tag, {ok, Peers}} when Peers =/= [] ->
            {ok, Peers};
        {Tag, _OkEmptyOrError} ->
            collect(N - 1, Tag, Deadline)
    after Remaining ->
        {error, timeout}
    end.

%%------------------------------------------------------------------
%% Per-pubkey pipeline
%%------------------------------------------------------------------

fetch_and_verify(Mod, Pubkey, Timeout) ->
    Target = macula_bootstrap_bep44:target_id(Pubkey),
    chain([
        fun() -> Mod:get_mutable(Target, Timeout) end,
        fun(Item)   -> check_pubkey(Item, Pubkey) end,
        fun(Item)   -> check_bep44(Item) end,
        fun(Item)   -> extract_record_bytes(Item) end,
        fun(Bytes)  -> macula_bootstrap_foundation:decode_record_bytes(Bytes) end,
        fun(Record) -> macula_foundation:verify_record(Record) end,
        fun(Record) ->
                {ok, macula_bootstrap_foundation:peers_from_record(
                       Record, via_mainline_dht, ?MODULE)}
        end
    ]).

check_pubkey(#{pubkey := Pk} = Item, Pk) -> {ok, Item};
check_pubkey(_Item, _Expected)           -> {error, wrong_pubkey}.

check_bep44(Item) ->
    case macula_bootstrap_bep44:verify(Item) of
        ok              -> {ok, Item};
        {error, _} = E  -> E
    end.

extract_record_bytes(#{value := DnsBytes}) ->
    extract(inet_dns:decode(DnsBytes)).

extract({ok, {dns_rec, _Hdr, _Qs, Answers, _Ns, _Ar}}) ->
    collapse([S || {dns_rr, _Name, txt, _Class, _Cnt, _TTL,
                    Strings, _Tm, _Bm, _F} <- Answers,
                    S <- Strings]);
extract({error, _}) ->
    {error, bad_pkarr_packet}.

collapse([])      -> {error, empty_pkarr};
collapse(Strings) ->
    Bin = iolist_to_binary(Strings),
    nonempty(Bin).

nonempty(<<>>) -> {error, empty_pkarr};
nonempty(Bin)  -> {ok, Bin}.

%%------------------------------------------------------------------
%% Result chain
%%------------------------------------------------------------------

chain([Step | Rest]) -> chain(Step(), Rest).

chain({error, _} = E, _Rest)         -> E;
chain({ok, V},        [])            -> {ok, V};
chain({ok, V},        [Step | Rest]) -> chain(Step(V), Rest).
