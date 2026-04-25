%% @doc DHT integration helpers for content discovery.
%%
%% Pure-function module: builds DHT keys from MCIDs, formats provider
%% info, computes TTL bookkeeping. Does NOT call into a DHT instance
%% — the caller (a process manager, or the station's content-aware
%% glue code) wraps these helpers around `hecate_dht:put_record/2'
%% and `hecate_dht:find_value/3' calls so this module stays free of
%% process state and easy to test.
%%
%% V2 difference from V1: V1 directly called `macula_routing_server'
%% via `whereis/1'. V2 leaves the integration to the caller because
%% hecate-station's DHT API is record-oriented (signed envelopes via
%% `hecate_record') rather than raw key-value, and the appropriate
%% record-type construction is a domain concern, not infrastructure.
-module(hecate_content_dht).

-export([
    dht_key/1,
    create_provider_info/3,
    format_providers/1,
    create_announcement/4,
    create_removal/1,
    default_ttl/0,
    get_ttl/1,
    reannounce_interval/1
]).

-export_type([provider_info/0]).

-type mcid() :: <<_:272>>.

-type provider_info() :: #{
    node_id       := binary(),
    endpoint      := binary(),
    metadata      := map(),
    advertised_at := non_neg_integer(),
    ttl           => pos_integer(),
    removed       => boolean(),
    removed_at    => non_neg_integer()
}.

-define(DEFAULT_TTL,    300).
-define(MIN_REANNOUNCE, 30).

%%====================================================================
%% Key generation
%%====================================================================

%% @doc DHT key for `MCID' — SHA256 of the MCID bytes for uniform
%% keyspace distribution. Independent of the MCID's internal hash
%% algorithm so BLAKE3 and SHA256 content land in the same keyspace.
-spec dht_key(mcid()) -> <<_:256>>.
dht_key(MCID) when is_binary(MCID), byte_size(MCID) =:= 34 ->
    crypto:hash(sha256, MCID).

%%====================================================================
%% Provider info
%%====================================================================

-spec create_provider_info(binary(), binary(), map()) -> provider_info().
create_provider_info(NodeId, Endpoint, Metadata) ->
    #{
        node_id       => NodeId,
        endpoint      => Endpoint,
        metadata      => Metadata,
        advertised_at => erlang:system_time(second)
    }.

%% @doc Normalise a single provider or a list of providers — fills in
%% missing fields with defaults so callers can pattern-match on a
%% stable shape.
-spec format_providers([provider_info()] | provider_info()) ->
        [provider_info()].
format_providers([])                                -> [];
format_providers(Single) when is_map(Single)        -> [normalise(Single)];
format_providers(List)   when is_list(List)         -> [normalise(P) || P <- List].

normalise(P) when is_map(P) ->
    #{
        node_id       => maps:get(node_id, P, <<>>),
        endpoint      => maps:get(endpoint, P, <<>>),
        metadata      => maps:get(metadata, P, #{}),
        advertised_at => maps:get(advertised_at, P, 0)
    }.

%%====================================================================
%% Announcements
%%====================================================================

%% @doc Build a `{Key, Value}' tuple for a content announcement.
%% Caller is responsible for wrapping `Value' in a signed
%% `hecate_record' envelope and feeding it to `hecate_dht:put_record/2'.
-spec create_announcement(mcid(), binary(), binary(), map()) ->
        {<<_:256>>, provider_info()}.
create_announcement(MCID, NodeId, Endpoint, ManifestInfo) ->
    Value = (create_provider_info(NodeId, Endpoint, ManifestInfo))
              #{ttl => ?DEFAULT_TTL},
    {dht_key(MCID), Value}.

%% @doc Build a removal marker — a provider explicitly retracting
%% an earlier announcement.
-spec create_removal(binary()) -> map().
create_removal(NodeId) ->
    #{
        node_id    => NodeId,
        removed    => true,
        removed_at => erlang:system_time(second)
    }.

%%====================================================================
%% TTL helpers
%%====================================================================

-spec default_ttl() -> pos_integer().
default_ttl() -> ?DEFAULT_TTL.

-spec get_ttl(map()) -> pos_integer().
get_ttl(Opts) when is_map(Opts) ->
    maps:get(ttl, Opts, ?DEFAULT_TTL).

%% @doc Suggested re-announcement interval — slightly before TTL
%% expires so coverage doesn't drop. Floors at `?MIN_REANNOUNCE'
%% seconds for very short TTLs.
-spec reannounce_interval(pos_integer()) -> pos_integer().
reannounce_interval(TTL) when TTL > ?MIN_REANNOUNCE + 60 ->
    TTL - 60;
reannounce_interval(_TTL) ->
    ?MIN_REANNOUNCE.
