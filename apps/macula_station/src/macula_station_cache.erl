%% @doc DHT routing-table cache.
%%
%% Warm-boot optimisation: instead of cascading from scratch on every
%% restart, the station snapshots its DHT routing table to disk at a
%% periodic cadence. On the next boot — before the cascade runs —
%% `load/2' re-injects the cached contacts into a fresh DHT. Stations
%% that just went down for a clean restart therefore come back up
%% with the same peer set they had moments ago, and the cascade only
%% tops it up.
%%
%% The file format is a single `term_to_binary/1' blob:
%%
%% ```
%% {macula_station_cache, 1, [Entry1, Entry2, ...]}
%% '''
%%
%% Version tag is for future schema evolution (new fields, different
%% entry shape). `load/2' refuses to decode other versions and falls
%% back to a clean DHT — better to cascade afresh than load garbage.
%%
%% == Lifecycle ==
%%
%% <ul>
%%   <li>Boot — `macula_station_app:start/2' calls `load/2'
%%       synchronously between DHT start and the cascade.</li>
%%   <li>Steady state — `start_link/1' runs a gen_server that
%%       flushes the current table every `flush_period_ms'. On a
%%       clean `terminate/2' it performs a final flush so the newest
%%       contacts land on disk.</li>
%% </ul>
%%
%% Entries are encoded by their `macula_dht_entry:spec()' shape so
%% that `load/2' can simply iterate and call `macula_dht:observe/2'.
%% Age-based fields (`first_seen', `last_seen', uptime) are dropped on
%% write — they will be recomputed when the entry is re-observed.
-module(macula_station_cache).
-behaviour(gen_server).

-include("macula_station_cfg.hrl").

-export([
    start_link/1, stop/1,
    dump/2,
    load/2,
    flush/1,
    path/1
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0, load_summary/0]).

-define(FILE_NAME, "routing-table.erl.bin").
-define(FORMAT_TAG, macula_station_cache).
-define(FORMAT_VSN, 1).

-type opts() :: #{
    dht   := pid(),
    cache := macula_station_config:cache_cfg()
}.

-type load_summary() :: #{
    loaded   := non_neg_integer(),
    skipped  := non_neg_integer()
}.

-record(state, {
    dht       :: pid(),
    cache_cfg :: macula_station_config:cache_cfg()
}).

%%==================================================================
%% Boot-time synchronous API.
%%==================================================================

%% @doc Canonical file path for a cache dir.
-spec path(file:name_all()) -> file:name_all().
path(Dir) -> filename:join(Dir, ?FILE_NAME).

%% @doc Dump the DHT's current routing table to the cache dir.
%% Creates the directory if needed. Writes atomically via tmp + rename.
-spec dump(pid(), file:name_all()) -> ok | {error, term()}.
dump(Dht, Dir) when is_pid(Dht) ->
    Entries = collect_entries(Dht),
    Blob    = term_to_binary({?FORMAT_TAG, ?FORMAT_VSN, Entries}),
    Path    = path(Dir),
    Tmp     = iolist_to_binary([Path, ".tmp"]),
    ok      = filelib:ensure_dir(Path),
    write_atomic(Tmp, Path, Blob).

%% @doc Load cached entries into the DHT. Missing file returns
%% `{ok, #{loaded => 0, skipped => 0}}' — warm boot succeeds even
%% on first boot. Corrupt or version-mismatched files return
%% `{error, _}' so the boot orchestrator can log + move on.
-spec load(pid(), file:name_all()) ->
    {ok, load_summary()} | {error, term()}.
load(Dht, Dir) when is_pid(Dht) ->
    on_read(file:read_file(path(Dir)), Dht).

%%==================================================================
%% Periodic flush gen_server.
%%==================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%% @doc Force an immediate flush. Used by tests + by graceful shutdown.
-spec flush(pid()) -> ok | {error, term()}.
flush(Pid) -> gen_server:call(Pid, flush).

%%==================================================================
%% Internals — disk I/O
%%==================================================================

collect_entries(Dht) ->
    [E || {_Ix, Bucket} <- macula_dht:populated_buckets(Dht),
          E <- macula_dht_bucket:members(Bucket)].

write_atomic(Tmp, Path, Blob) ->
    on_write(file:write_file(Tmp, Blob, [raw, binary]), Tmp, Path).

on_write(ok, Tmp, Path)                -> file:rename(Tmp, Path);
on_write({error, _} = E, _Tmp, _Path)  -> E.

on_read({ok, Blob}, Dht) ->
    decode_and_ingest(safe_binary_to_term(Blob), Dht);
on_read({error, enoent}, _Dht) ->
    {ok, #{loaded => 0, skipped => 0}};
on_read({error, _} = E, _Dht) ->
    E.

safe_binary_to_term(Blob) ->
    try {ok, binary_to_term(Blob, [safe])}
    catch _:_ -> {error, bad_term}
    end.

decode_and_ingest({ok, {?FORMAT_TAG, ?FORMAT_VSN, Entries}}, Dht)
  when is_list(Entries) ->
    ingest_entries(Entries, Dht, #{loaded => 0, skipped => 0});
decode_and_ingest({ok, _}, _Dht) ->
    {error, bad_format};
decode_and_ingest({error, _} = E, _Dht) ->
    E.

ingest_entries([], _Dht, Acc) ->
    {ok, Acc};
ingest_entries([Entry | Rest], Dht, Acc) ->
    ingest_entries(Rest, Dht, step_ingest(entry_to_spec(Entry), Dht, Acc)).

step_ingest({ok, Spec}, Dht, #{loaded := L} = Acc) ->
    _ = macula_dht:observe(Dht, Spec),
    Acc#{loaded := L + 1};
step_ingest(skip, _Dht, #{skipped := S} = Acc) ->
    Acc#{skipped := S + 1}.

%% Turn a live `macula_dht_entry:entry()' into an `observe' spec,
%% stripping age-based metadata.
entry_to_spec(#{node_id := _,
                asn := _,
                country := _,
                tier := _} = E) ->
    {ok, #{node_id   => maps:get(node_id, E),
           endpoints => maps:get(endpoints, E, []),
           asn       => maps:get(asn, E),
           country   => maps:get(country, E),
           tier      => maps:get(tier, E)}};
entry_to_spec(_) ->
    skip.

%%==================================================================
%% gen_server
%%==================================================================

init(#{dht := Dht, cache := #cache_cfg{} = Cfg}) when is_pid(Dht) ->
    schedule_tick(Cfg),
    {ok, #state{dht = Dht, cache_cfg = Cfg}}.

handle_call(flush, _From, S) ->
    {reply, do_flush(S), S};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) -> {noreply, S}.

handle_info(flush_tick, #state{cache_cfg = Cfg} = S) ->
    _ = do_flush(S),
    schedule_tick(Cfg),
    {noreply, S};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, S) ->
    _ = do_flush(S),
    ok.

code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%==================================================================
%% Internals
%%==================================================================

schedule_tick(#cache_cfg{flush_period_ms = Ms}) ->
    erlang:send_after(Ms, self(), flush_tick).

do_flush(#state{dht = Dht, cache_cfg = #cache_cfg{path = Dir}}) ->
    dump(Dht, Dir).
