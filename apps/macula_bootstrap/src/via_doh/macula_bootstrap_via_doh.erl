%% @doc Tier A — foundation-anchor bootstrap (Part 5 §4).
%%
%% Tier A queries a panel of independent DoH resolvers in parallel
%% for the foundation-signed `foundation_seed_list' record at each
%% firmware-embedded foundation pubkey (`macula_foundation:pubkeys/0').
%% A record is accepted only when at least `corroboration' resolvers
%% return byte-identical payloads — defending against single-resolver
%% hijack (Part 5 §4.3).
%%
%% Per-response verification chain:
%% <ol>
%%   <li>Decode bytes via `macula_record:decode/1'.</li>
%%   <li>Confirm the record's storage key matches the expected
%%       `sha256("foundation_seed_list" || FoundationKey)' (defends
%%       against substitution to a different foundation domain).</li>
%%   <li>Verify type / signer / signature / expiry via
%%       `macula_foundation:verify_record/1'.</li>
%% </ol>
%%
%% Once a record clears corroboration, every seed in its payload is
%% emitted as a `macula_bootstrap_peer_discoverer:verified_peer()' carrying the
%% same signed record as its trust anchor.
%%
%% == Probe options ==
%%
%% <ul>
%%   <li>`resolvers' :: [{module(), url()}] — required. Each tuple is
%%       a `macula_bootstrap_via_doh_resolver_behaviour' implementation paired with the
%%       endpoint it should query. Multiple distinct providers are
%%       required for any meaningful corroboration.</li>
%%   <li>`pubkeys' :: [pubkey()] — defaults to
%%       `macula_foundation:pubkeys/0'.</li>
%%   <li>`corroboration' :: pos_integer() — minimum resolvers that
%%       must return identical bytes for a given pubkey
%%       (default 2).</li>
%%   <li>`timeout_ms' :: pos_integer() — per-resolver deadline
%%       (default 1500).</li>
%% </ul>
%%
%% Tier A runs at zero stagger — it is the cascade's first hop.
-module(macula_bootstrap_via_doh).
-behaviour(macula_bootstrap_peer_discoverer).

-export([strategy/0, stagger_ms/0, discover/1]).

-export_type([resolver_spec/0, discover_opts/0]).

-type resolver_spec() :: {module(), macula_bootstrap_via_doh_resolver_behaviour:url()}.

-type discover_opts() :: #{
    resolvers     := [resolver_spec()],
    pubkeys       => [macula_identity:pubkey()],
    corroboration => pos_integer(),
    timeout_ms    => pos_integer()
}.

-define(DEFAULT_CORROBORATION, 2).
-define(DEFAULT_TIMEOUT_MS,    1500).
-define(STORAGE_DOMAIN_FOUND_SEED, <<"foundation_seed_list">>).

strategy()   -> via_doh.
stagger_ms() -> 0.

-spec discover(discover_opts()) -> macula_bootstrap_peer_discoverer:discover_result().
discover(Opts) ->
    Resolvers = maps:get(resolvers,     Opts, []),
    Pubkeys   = maps:get(pubkeys,       Opts, macula_foundation:pubkeys()),
    Threshold = maps:get(corroboration, Opts, ?DEFAULT_CORROBORATION),
    Timeout   = maps:get(timeout_ms,    Opts, ?DEFAULT_TIMEOUT_MS),
    run(Resolvers, Pubkeys, Threshold, Timeout).

%%------------------------------------------------------------------
%% Top-level dispatch
%%------------------------------------------------------------------

run([],   _Pubkeys, _Threshold, _Timeout) -> {error, no_resolvers};
run(_Rs,  [],       _Threshold, _Timeout) -> {error, no_pubkeys};
run(Rs,   Pubkeys,   Threshold,  Timeout) ->
    ProbeTag = make_ref(),
    Workers  = start_workers(Rs, Pubkeys, Timeout, ProbeTag),
    Deadline = erlang:monotonic_time(millisecond) + Timeout + 250,
    Tally    = collect(ProbeTag, length(Workers), Deadline, #{}),
    select(Tally, Threshold).

%%------------------------------------------------------------------
%% Worker fan-out
%%------------------------------------------------------------------

start_workers(Resolvers, Pubkeys, Timeout, ProbeTag) ->
    [start_worker(R, K, Timeout, ProbeTag)
     || R <- Resolvers, K <- Pubkeys].

start_worker({Mod, Url}, Pubkey, Timeout, ProbeTag) ->
    Parent = self(),
    spawn(
      fun() ->
          Reply = Mod:resolve(Url, Pubkey, #{timeout_ms => Timeout}),
          Parent ! {?MODULE, ProbeTag, Pubkey, Reply}
      end).

%%------------------------------------------------------------------
%% Result collection — tally distinct (Pubkey, Bytes) groups
%%------------------------------------------------------------------

collect(_ProbeTag, 0, _Deadline, Tally) ->
    Tally;
collect(ProbeTag, N, Deadline, Tally) ->
    Remaining = remaining_ms(Deadline),
    receive
        {?MODULE, ProbeTag, Pubkey, Reply} ->
            collect(ProbeTag, N - 1, Deadline,
                    accumulate(Pubkey, Reply, Tally))
    after Remaining ->
        Tally
    end.

accumulate(Pubkey, {ok, Bytes}, Tally) when is_binary(Bytes) ->
    bump(Pubkey, Bytes, Tally);
accumulate(_Pubkey, _Other, Tally) ->
    Tally.

bump(Pubkey, Bytes, Tally) ->
    Key = {Pubkey, Bytes},
    maps:update_with(Key, fun(N) -> N + 1 end, 1, Tally).

%%------------------------------------------------------------------
%% Selection — pick a corroborated group, verify, emit peers
%%------------------------------------------------------------------

select(Tally, Threshold) ->
    Eligible = [{Pk, Bytes} || {{Pk, Bytes}, Count} <- maps:to_list(Tally),
                               Count >= Threshold],
    pick_first(Eligible).

pick_first([])                  -> {error, no_corroboration};
pick_first([{Pk, Bytes} | Rest]) -> verify_and_emit(Pk, Bytes, Rest).

verify_and_emit(Pk, Bytes, Rest) ->
    case decode_and_verify(Pk, Bytes) of
        {ok, Record} -> {ok, peers_from(Record)};
        {error, _}   -> pick_first(Rest)
    end.

decode_and_verify(Pk, Bytes) ->
    chain([
        fun() -> macula_bootstrap_foundation:decode_record_bytes(Bytes) end,
        fun(R) -> check_storage_key(Pk, R) end,
        fun(R) -> macula_foundation:verify_record(R) end
    ]).

%% Sequential `>>=' over `{ok, _} | {error, _}'.
chain([Step | Rest]) -> chain(Step(), Rest).

chain({error, _} = E, _Rest)         -> E;
chain({ok, V},        [])            -> {ok, V};
chain({ok, V},        [Step | Rest]) -> chain(Step(V), Rest).

check_storage_key(Pk, Record) ->
    Expected = crypto:hash(sha256,
                           <<?STORAGE_DOMAIN_FOUND_SEED/binary, Pk/binary>>),
    case macula_record:storage_key(Record) =:= Expected of
        true  -> {ok, Record};
        false -> {error, wrong_storage_key}
    end.

%%------------------------------------------------------------------
%% Peer extraction (delegates to the shared foundation helper)
%%------------------------------------------------------------------

peers_from(Record) ->
    macula_bootstrap_foundation:peers_from_record(Record, via_doh, ?MODULE).

%%------------------------------------------------------------------
%% Time helpers
%%------------------------------------------------------------------

remaining_ms(Deadline) ->
    max(Deadline - erlang:monotonic_time(millisecond), 0).
