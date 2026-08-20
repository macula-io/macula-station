%% @doc via_doh DoH resolver behaviour.
%%
%% via_doh (Part 5 §4) bootstraps from <b>foundation-signed seed
%% lists</b> resolved via DNS-over-HTTPS. The actual transport is
%% pluggable so that:
%% <ul>
%%   <li>The <em>orchestration</em> (parallel queries, K-of-N
%%       corroboration, signature verification) lives in
%%       `macula_bootstrap_via_doh' and is fully unit-testable.</li>
%%   <li>Real DoH I/O, request shaping, and TLS pinning live in
%%       transport-specific modules (e.g. an `httpc'-backed
%%       resolver, a Cloudflare-1.1.1.1 resolver, a Quad9 resolver)
%%       that the operator wires in via station config.</li>
%% </ul>
%%
%% Each implementation is responsible for issuing the lookup, parsing
%% the DoH response, extracting the PKARR/CBOR payload, and returning
%% the raw `macula_record' bytes. Decoding, signature verification,
%% and corroboration are done by the orchestrator — resolvers MUST
%% NOT verify signatures themselves (a single trusted resolver is a
%% trust-anchor failure mode).
%%
%% Reference: plans/PLAN_MACULA_V2_PART5_BOOTSTRAP.md §4.
-module(macula_bootstrap_via_doh_resolver_behaviour).

-export_type([url/0, resolve_opts/0, resolve_result/0]).

-type url() :: binary() | string().

-type resolve_opts() :: #{
    timeout_ms => non_neg_integer()
}.

-type resolve_result() ::
        {ok, RecordBytes :: binary()}
      | {error, term()}.

%% Resolve the foundation seed-list record for `FoundationKey'
%% from the resolver's `Url'. Return raw record bytes for the
%% orchestrator to decode and verify, or an `{error, Reason}'.
-callback resolve(url(), macula_identity:pubkey(), resolve_opts()) ->
            resolve_result().
