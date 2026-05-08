%% @doc Blockchain-anchor transport behaviour (Tier D, Part 5 §7).
%%
%% Hides the per-chain mechanics of reading the foundation's
%% quarterly-refreshed signed anchor from a public blockchain.
%% Concrete adapters (Bitcoin OP_RETURN, Ethereum contract event)
%% implement this callback; Tier D queries each configured chain in
%% parallel and accepts the first valid answer.
%%
%% The anchor bytes MUST be a `macula_record:encode/1' byte string of
%% a foundation-signed record (typically `foundation_seed_list',
%% 0x0D). This keeps Tier D verification identical to Tier A — the
%% chain adapter is responsible solely for locating and returning the
%% bytes.
%%
%% == Why Ed25519 signature checks, not chain consensus ==
%%
%% Even a 51%-attacked chain cannot forge the Ed25519 signature of
%% the foundation's threshold key — the chain gives us <em>where to
%% find</em> the anchor, the Ed25519 signature gives us <em>trust</em>.
%% A rewritten chain history at worst denies service (forces Tier E),
%% it does not inject malicious peers.
-module(macula_bootstrap_via_blockchain_transport).

-export_type([anchor_result/0, chain_opts/0]).

-type chain_opts() :: map().

-type anchor_result() ::
        {ok, AnchorBytes :: binary()}
      | {error, term()}.

-callback latest_anchor(chain_opts(), TimeoutMs :: pos_integer()) ->
            anchor_result().
