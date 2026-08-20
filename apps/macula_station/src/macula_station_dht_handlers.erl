%% @doc Station-level DHT procedure handlers.
%%
%% On init, advertises three procedures against the station's
%% `macula_handler_registry':
%%
%% <ul>
%%   <li>`_dht.put_record' — store a signed `macula_record', validating
%%       the signature first via `macula_record:verify/1'. Maps to
%%       `macula_dht:put_record/2'.</li>
%%   <li>`_dht.find_record' — fetch by `macula_record:storage_key/1'
%%       output. Maps to `macula_dht:find_local_record/2' (returns
%%       the first record at that key, `not_found' if empty).</li>
%%   <li>`_dht.find_records' — fetch EVERY record at
%%       `macula_record:storage_key/1' (the full multi-value set,
%%       e.g. every provider that advertised one procedure_uri).
%%       Maps to the same `macula_dht:find_local_record/2', which
%%       already returns the whole list; `_dht.find_record' just
%%       takes its head. A miss is `{ok, []}', not `not_found'.</li>
%%   <li>`_dht.find_records_by_type' — list every locally-known record
%%       of a given type tag. Filters `macula_dht:list_records/1' by
%%       `macula_record:type/1'. Coverage is the relay's local view
%%       (own puts + replicated copies); cross-station completeness
%%       requires querying multiple relays.</li>
%% </ul>
%%
%% == Lifecycle ==
%%
%% Started under the station's supervision tree AFTER both
%% `macula_handler_registry' and `macula_dht' are up. On any
%% one_for_all restart the registry + DHT come back fresh and this
%% gen_server's `init/1' re-advertises the four procedures against
%% the new registry pid — no manual re-wiring required.
%%
%% No state beyond the references; the gen_server is otherwise idle.
-module(macula_station_dht_handlers).
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-export([next_candidates/3, node_id_of/1, addresses_of/1]).
-endif.

-export_type([opts/0]).

%% Registries are REGISTERED NAMES, resolved by gen_server:call/2 on
%% every use. A pid captured at child-spec time is dead the moment the
%% registry restarts, and supervisors reuse the original child spec, so
%% the stale pid would never be replaced.
-type opts() :: #{
    handler_registry := atom() | pid(),
    dht              := pid()
}.

-record(state, {
    handler_registry :: atom() | pid(),
    dht              :: pid()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{handler_registry := _, dht := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

%%====================================================================
%% gen_server
%%====================================================================

init(#{handler_registry := Registry, dht := Dht}) ->
    advertise_all(Registry, Dht),
    {ok, #state{handler_registry = Registry, dht = Dht}}.

handle_call(_Msg, _From, S) -> {reply, {error, unknown_call}, S}.
handle_cast(_Msg, S)        -> {noreply, S}.
handle_info(_Msg, S)        -> {noreply, S}.
terminate(_Reason, _S)      -> ok.
code_change(_OldVsn, S, _)  -> {ok, S}.

%%====================================================================
%% Procedure advertisement
%%====================================================================

advertise_all(Registry, Dht) ->
    ok = macula_handler_registry:advertise(
           Registry, <<"_dht.put_record">>,
           fun(Args) -> handle_put_record(Dht, Args) end),
    ok = macula_handler_registry:advertise(
           Registry, <<"_dht.find_record">>,
           fun(Args) -> handle_find_record(Dht, Args) end),
    ok = macula_handler_registry:advertise(
           Registry, <<"_dht.find_records">>,
           fun(Args) -> handle_find_records(Dht, Args) end),
    ok = macula_handler_registry:advertise(
           Registry, <<"_dht.find_records_by_type">>,
           fun(Args) -> handle_find_records_by_type(Dht, Args) end),
    ok.

%%====================================================================
%% Handler bodies
%%====================================================================

handle_put_record(Dht, Record) when is_map(Record) ->
    on_verify(Dht, Record, macula_record:verify(Record));
handle_put_record(_Dht, _Other) ->
    {error, bad_request}.

on_verify(Dht, _Record, {ok, Verified}) ->
    ok = macula_dht:put_record(Dht, Verified),
    fire_eager_replication(Verified),
    {ok, ok};
on_verify(_Dht, _Record, {error, _Reason}) ->
    {error, bad_signature}.

%% Eager replication on every put — async cast to the replicator,
%% which spawns one unlinked worker per k-closest custodian. The
%% daemon's `_dht.put_record' RPC returns the moment the local
%% store has succeeded; remote acks happen in the background.
%%
%% Without this a daemon putting on station A and reading from
%% station B saw not_found until the next periodic
%% `macula_dht_replicate' tick (default 1 h). The periodic tick
%% remains the safety net for churn (peers that joined the
%% k-closest set after the original put).
%%
%% Two underlying fixes had to land first:
%%   1. macula-station 7d46867 — peer_observer's conn_for/2 reads
%%      via ETS instead of gen_server:call, so per-store send_frame
%%      doesn't queue against the observer's frame-handling loop.
%%   2. macula 4.2.6 — peering_conn handles send_hello errors
%%      gracefully instead of badmatching, so the burst of new
%%      handshake races during teardown doesn't trip supervisor
%%      restart-intensity.
fire_eager_replication(Record) ->
    case whereis(macula_dht_replicate) of
        undefined -> ok;
        Pid       -> macula_dht_replicate:replicate_one(Pid, Record)
    end.

handle_find_record(Dht, #{key := Key})
  when is_binary(Key), byte_size(Key) =:= 32 ->
    on_local_hit(macula_dht:find_local_record(Dht, Key), Dht, Key);
handle_find_record(_Dht, _Other) ->
    {error, bad_request}.

%% Local hit short-circuits — record is already on this station.
%% Local miss falls back to a one-hop iterative lookup against the
%% k-closest peers in our routing table. Necessary because in the
%% partial-mesh topology not every pair of stations has a direct
%% edge: writer's eager replication lands on writer's k-closest
%% custodians, but the reader (here) might not be in that set —
%% the reader has to walk out one hop to find a custodian that does
%% hold the record.
on_local_hit([Record | _], _Dht, _Key) ->
    {ok, Record};
on_local_hit([], Dht, Key) ->
    on_remote_lookup(remote_find_value(Dht, Key)).

on_remote_lookup({ok, Record}) -> {ok, Record};
on_remote_lookup(not_found)    -> {ok, not_found}.

%% Iterative Kademlia FIND_VALUE walk. Round 0 asks every one of our own
%% k-closest routing-table peers (we hold a connection to all of them, by
%% definition) in parallel; first `{value, ...}' wins. On an all-NODES
%% round, every replying peer's k-closest-to-Key becomes next round's
%% candidate pool — dialled on demand via `macula_station_dht_dialer' for
%% anything we are not already connected to (see its moduledoc for why
%% that dial is trust-checked, not just opened).
%%
%% This used to stop after one hop: "eager replication landed copies on
%% the writer's k-closest, so at least one of OUR peers should hold it."
%% True only when the reader's k-closest and the writer's k-closest
%% overlap — false by construction on a partial-mesh pair with no direct
%% edge between them, which is exactly the case a one-hop fallback cannot
%% reach no matter how the mesh's replication is tuned. Multi-round can.
%%
%% Bounded three ways: `?WALK_MAX_ROUNDS' caps rounds; `Seen' guarantees
%% no NodeId is queried twice, so a cycle in NODES replies cannot loop
%% even before the round cap; and every round only ever walks toward Key
%% (`build_nodes_reply' returns a station's OWN k-closest, which is closer
%% to Key than the querying station or it would not have been offered).
%%
%% Safe to fan a round out in parallel because peer_observer's `conn_for'
%% is an ETS read (commit 7d46867); the per-peer `find_value' wire sends
%% do not serialise against the observer's mailbox.
-define(REMOTE_FIND_PER_PEER_MS, 1_500).
-define(WALK_DIAL_TIMEOUT_MS,    3_000).
-define(WALK_MAX_ROUNDS,             3).
-define(WALK_ROUND_WIDTH,            5).

remote_find_value(Dht, Key) ->
    value_or_not_found(walk_find_value(Dht, Key)).

value_or_not_found({value, [Record | _]}) -> {ok, Record};
value_or_not_found(not_found)             -> not_found.

remote_find_records(Dht, Key) ->
    records_or_empty(walk_find_value(Dht, Key)).

records_or_empty({value, Records}) -> Records;
records_or_empty(not_found)        -> [].

%% The k-closest peers to Key from OUR OWN routing table, excluding self
%% — round 0's candidates. Paired with `[]' addresses: round 0 needs no
%% dial, we are already connected to everything in our own routing table.
walk_find_value(Dht, Key) ->
    walk(Dht, Key, [{Id, []} || Id <- peer_ids(Dht, Key)], sets:new(), 0).

peer_ids(Dht, Key) ->
    Entries = macula_dht:k_closest(Dht, Key, 20),
    SelfId  = macula_dht:self_id(Dht),
    [Id || E <- Entries,
           (Id = macula_dht_entry:node_id(E)) =/= SelfId].

walk(_Dht, _Key, [], _Seen, _Round) ->
    not_found;
walk(_Dht, _Key, _Candidates, _Seen, Round) when Round >= ?WALK_MAX_ROUNDS ->
    not_found;
walk(Dht, Key, Candidates, Seen, Round) ->
    Seen1 = lists:foldl(fun({Id, _Addrs}, Acc) -> sets:add_element(Id, Acc) end,
                        Seen, Candidates),
    settle_round(query_round(Dht, Key, Candidates), Dht, Key, Seen1, Round).

settle_round({value, Records}, _Dht, _Key, _Seen, _Round) ->
    {value, Records};
settle_round({nodes, Refs}, Dht, Key, Seen, Round) ->
    walk(Dht, Key, next_candidates(Key, Refs, Seen), Seen, Round + 1).

%% Query every candidate of this round in parallel. Each spawned worker
%% dials on demand (a no-op if already connected — see
%% `macula_station_dht_dialer:ensure_dialed/3') before issuing FIND_VALUE,
%% so an unroutable candidate costs at most `?WALK_DIAL_TIMEOUT_MS', not a
%% silent hang.
%%
%% `Candidates' is never `[]' here — `walk/5' short-circuits to
%% `not_found' before ever calling this, so an empty-list clause would be
%% dead code (dialyzer correctly flags it as unreachable).
query_round(Dht, Key, Candidates) ->
    Tag    = make_ref(),
    Parent = self(),
    [spawn(fun() -> Parent ! {Tag, query_one(Dht, Key, C)} end)
     || C <- Candidates],
    Budget = ?WALK_DIAL_TIMEOUT_MS + ?REMOTE_FIND_PER_PEER_MS + 200,
    collect_round(Tag, length(Candidates), Budget, []).

query_one(Dht, Key, {PeerId, Addresses}) ->
    dial_then_query(
      macula_station_dht_dialer:ensure_dialed(PeerId, Addresses,
                                              ?WALK_DIAL_TIMEOUT_MS),
      Dht, Key, PeerId).

dial_then_query(ok, Dht, Key, PeerId) ->
    macula_dht:find_value(Dht, Key, PeerId, ?REMOTE_FIND_PER_PEER_MS);
dial_then_query({error, Reason}, _Dht, _Key, _PeerId) ->
    {error, {no_route, Reason}}.

%% Short-circuits on the first VALUE. Otherwise waits out the round to
%% collect every NODES reply — the whole point of a round is to learn as
%% much of the next hop's candidate pool as possible, so stopping at the
%% first NODES reply back would throw away exactly the information a
%% multi-round walk exists to gather.
collect_round(_Tag, 0, _DeadlineMs, RefsAcc) ->
    {nodes, lists:append(RefsAcc)};
collect_round(Tag, Remaining, DeadlineMs, RefsAcc) ->
    Start = erlang:monotonic_time(millisecond),
    receive
        {Tag, {value, [_ | _] = Records}} ->
            {value, Records};
        {Tag, {nodes, Refs}} ->
            Elapsed = erlang:monotonic_time(millisecond) - Start,
            collect_round(Tag, Remaining - 1, max(0, DeadlineMs - Elapsed),
                         [Refs | RefsAcc]);
        {Tag, _Other} ->
            Elapsed = erlang:monotonic_time(millisecond) - Start,
            collect_round(Tag, Remaining - 1, max(0, DeadlineMs - Elapsed),
                         RefsAcc)
    after DeadlineMs ->
        {nodes, lists:append(RefsAcc)}
    end.

%% Fold every ref this round's NODES replies carried into next round's
%% candidate pool: dedupe by node_id, drop anything already queried (in
%% ANY prior round — `Seen' accumulates) or already self, drop anything
%% with no address at all (an honest dead end — see
%% `macula_dht_protocol:entry_to_station_ref/2'), sort by XOR distance to
%% Key, and keep only the closest `?WALK_ROUND_WIDTH' — bounding fan-out
%% growth round over round the same way `k' bounds it in the routing
%% table itself.
next_candidates(Key, Refs, Seen) ->
    %% `Refs' arrives already flat — `collect_round' merges every peer's
    %% NODES reply for this round with `lists:append/1' before handing it
    %% to `settle_round'.
    ById = lists:foldl(fun(R, Acc) -> maps:put(node_id_of(R), R, Acc) end,
                       #{}, Refs),
    Candidates = [R || R <- maps:values(ById),
                        not sets:is_element(node_id_of(R), Seen),
                        addresses_of(R) =/= []],
    Sorted = lists:sort(
               fun(A, B) ->
                   macula_dht_xor:distance_int(Key, node_id_of(A)) =<
                   macula_dht_xor:distance_int(Key, node_id_of(B))
               end, Candidates),
    [{node_id_of(R), addresses_of(R)}
     || R <- lists:sublist(Sorted, ?WALK_ROUND_WIDTH)].

%% `station_ref()' arrives here already decoded off the wire
%% (`macula_frame:decode/1' inside the DHT server's frame ingestion), and
%% this codebase has been repeatedly bitten by CBOR handing back
%% `{text, Bin}' keys instead of atoms for a map that looks correct at
%% the call site — see `macula_dht_endpoint_propagation_tests' for the
%% same trap on the producer side. Read every shape rather than assume
%% one.
node_id_of(#{node_id := V})               -> V;
node_id_of(#{{text, <<"node_id">>} := V}) -> V;
node_id_of(#{<<"node_id">> := V})         -> V.

addresses_of(#{addresses := A})               -> A;
addresses_of(#{{text, <<"addresses">>} := A}) -> A;
addresses_of(#{<<"addresses">> := A})         -> A;
addresses_of(_)                               -> [].

%% Multi-value sibling of `handle_find_record'. Returns EVERY record
%% stored at the key (the full `bag'), not just the first — e.g.
%% every provider that advertised one procedure_uri. Same
%% local-hit-then-one-hop-remote shape; a miss is `{ok, []}', not
%% `{ok, not_found}', because the reply is a list.
handle_find_records(Dht, #{key := Key})
  when is_binary(Key), byte_size(Key) =:= 32 ->
    on_local_records(macula_dht:find_local_record(Dht, Key), Dht, Key);
handle_find_records(_Dht, _Other) ->
    {error, bad_request}.

on_local_records([_ | _] = Records, _Dht, _Key) ->
    {ok, Records};
on_local_records([], Dht, Key) ->
    {ok, remote_find_records(Dht, Key)}.

handle_find_records_by_type(Dht, #{type := Type})
  when is_integer(Type), Type >= 0, Type =< 255 ->
    All = macula_dht:list_records(Dht),
    {ok, [R || R <- All, macula_record:type(R) =:= Type]};
handle_find_records_by_type(_Dht, _Other) ->
    {error, bad_request}.
