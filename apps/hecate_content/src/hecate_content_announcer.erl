%% @doc Process manager: turns `manifest_stored' events from
%% `hecate_content_store' into signed `content_announcement' records
%% put into the local DHT instance.
%%
%% Subscribes to the pg group `hecate_content_store:events_group/0',
%% receives `{manifest_stored, MCID, Manifest}' messages, and
%% publishes via `hecate_dht:put_record/2'. Records are signed with
%% the configured station identity so DHT consumers can verify
%% authenticity end-to-end.
%%
%% Configured at start-up with:
%% <ul>
%%   <li>`dht'      — `hecate_dht:dht()' reference (the local DHT
%%       gen_server) to publish records into.</li>
%%   <li>`identity' — `macula_identity:key_pair()' used to sign the
%%       outgoing record.</li>
%%   <li>`station_id' — pubkey identifying the announcing station
%%       (typically `macula_identity:public(Identity)').</li>
%%   <li>`endpoint' — binary endpoint URL ("quic://host:port") that
%%       remote nodes should dial to fetch the content.</li>
%% </ul>
%%
%% If `dht' is `undefined' (unconfigured), the announcer logs and
%% drops events — useful for unit tests of the dispatch pathway
%% without a running DHT.
%%
%% == Multi-identity (PLAN_MULTI_IDENTITY_RELAY §Phase 2) ==
%%
%% Anonymous gen_server. Each identity gets its OWN announcer with
%% its OWN identity / station_id / endpoint. The shared
%% `hecate_content_store' fan-outs `{manifest_stored, ...}' messages
%% via pg, so every announcer subscribed to the events group sees
%% the same store events and publishes a per-identity record into
%% its identity's DHT.
-module(hecate_content_announcer).
-behaviour(gen_server).

-export([
    start_link/1, stop/1,
    announce/2, announce/3
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([opts/0]).

-type opts() :: #{
    dht          := hecate_dht:dht() | undefined,
    identity     := macula_identity:key_pair(),
    station_id   := macula_identity:pubkey(),
    endpoint     := binary(),
    ttl_ms       => pos_integer(),
    %% Phase 6 (operational tooling): when supplied, the announcer
    %% sets `logger:set_process_metadata(#{identity_id =&gt; Key})'
    %% on init so its log lines carry the identity for diagnostics
    %% on a multi-identity box.
    identity_key => term()
}.

-record(state, {
    dht        :: hecate_dht:dht() | undefined,
    identity   :: macula_identity:key_pair(),
    station_id :: macula_identity:pubkey(),
    endpoint   :: binary(),
    ttl_ms     :: pos_integer()
}).

-define(DEFAULT_TTL_MS, 300000).  %% 5 minutes — matches DHT default

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{identity := _, station_id := _, endpoint := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%% @doc Manually announce a manifest by MCID. Used when a caller
%% wants to announce content the announcer didn't observe via the
%% pg event channel (e.g. on station boot, to re-publish previously
%% stored manifests).
-spec announce(pid(), binary()) -> ok | {error, term()}.
announce(Pid, MCID) ->
    gen_server:call(Pid, {announce_mcid, MCID}).

-spec announce(pid(), binary(), map()) -> ok | {error, term()}.
announce(Pid, MCID, Manifest) ->
    gen_server:call(Pid, {announce, MCID, Manifest}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Opts) ->
    ensure_pg_scope(),
    set_logger_identity(Opts),
    ok = pg:join(hecate_content_store:events_group(), self()),
    {ok, #state{
        dht        = maps:get(dht, Opts, undefined),
        identity   = maps:get(identity, Opts),
        station_id = maps:get(station_id, Opts),
        endpoint   = maps:get(endpoint, Opts),
        ttl_ms     = maps:get(ttl_ms, Opts, ?DEFAULT_TTL_MS)
    }}.

set_logger_identity(#{identity_key := Key}) ->
    logger:set_process_metadata(#{identity_id => Key});
set_logger_identity(_) ->
    ok.

handle_call({announce, MCID, Manifest}, _From, S) ->
    {reply, do_announce(MCID, Manifest, S), S};
handle_call({announce_mcid, MCID}, _From, S) ->
    {reply, do_announce_by_mcid(MCID, S), S};
handle_call(_Other, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_, S) -> {noreply, S}.

handle_info({manifest_stored, MCID, Manifest}, S) ->
    _ = do_announce(MCID, Manifest, S),
    {noreply, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, _S) ->
    ok.

%%====================================================================
%% Internal
%%====================================================================

do_announce_by_mcid(MCID, S) ->
    case hecate_content_store:get_manifest(MCID) of
        {ok, Manifest} -> do_announce(MCID, Manifest, S);
        {error, _} = E -> E
    end.

do_announce(_MCID, _Manifest, #state{dht = undefined}) ->
    {error, dht_not_configured};
do_announce(MCID, Manifest, S) ->
    Record = build_record(MCID, Manifest, S),
    Signed = hecate_record:sign(Record, S#state.identity),
    publish(S#state.dht, Signed).

build_record(MCID, Manifest, S) ->
    Opts = #{
        name        => maps:get(name, Manifest, <<>>),
        size        => maps:get(size, Manifest, 0),
        chunk_count => maps:get(chunk_count, Manifest, 0),
        ttl_ms      => S#state.ttl_ms
    },
    hecate_record:content_announcement(
        S#state.station_id, MCID, S#state.endpoint, Opts).

publish(Dht, Record) ->
    case catch hecate_dht:put_record(Dht, Record) of
        ok                  -> ok;
        {'EXIT', Reason}    -> {error, {dht_crashed, Reason}};
        {error, _} = Error  -> Error
    end.

ensure_pg_scope() ->
    %% pg's default scope is started by the kernel application in
    %% OTP 27+. If it isn't already running (older OTP, embedded
    %% setups), start it here. Idempotent.
    case erlang:whereis(pg) of
        undefined -> _ = pg:start(pg), ok;
        _Pid      -> ok
    end.
