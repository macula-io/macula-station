%% @doc Per-identity DHT procedure handlers.
%%
%% On init, advertises three procedures against the per-identity
%% `macula_handler_registry':
%%
%% <ul>
%%   <li>`_dht.put_record' — store a signed `macula_record', validating
%%       the signature first via `macula_record:verify/1'. Maps to
%%       `macula_dht:put_record/2'.</li>
%%   <li>`_dht.find_record' — fetch by `macula_record:storage_key/1'
%%       output. Maps to `macula_dht:find_local_record/2' (returns
%%       the first record at that key, `not_found' if empty).</li>
%%   <li>`_dht.find_records_by_type' — list every locally-known record
%%       of a given type tag. Filters `macula_dht:list_records/1' by
%%       `macula_record:type/1'. Coverage is the relay's local view
%%       (own puts + replicated copies); cross-station completeness
%%       requires querying multiple relays.</li>
%% </ul>
%%
%% == Lifecycle ==
%%
%% Started by `macula_station_identity_sup' as a procedural child
%% AFTER both `macula_handler_registry' and `macula_dht' are up.
%% On any one_for_all restart of the identity sup the registry +
%% DHT come back fresh and this gen_server's `init/1' re-advertises
%% the three procedures against the new registry pid — no manual
%% re-wiring required.
%%
%% No state beyond the references; the gen_server is otherwise idle.
-module(macula_station_dht_handlers).
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0]).

-type opts() :: #{
    handler_registry := pid(),
    dht              := pid(),
    identity_key     => term()
}.

-record(state, {
    handler_registry :: pid(),
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

init(#{handler_registry := Registry, dht := Dht} = Opts) ->
    set_logger_identity(Opts),
    advertise_all(Registry, Dht),
    {ok, #state{handler_registry = Registry, dht = Dht}}.

set_logger_identity(#{identity_key := Key}) ->
    logger:set_process_metadata(#{identity_id => Key});
set_logger_identity(_) ->
    ok.

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
    {ok, ok};
on_verify(_Dht, _Record, {error, _Reason}) ->
    {error, bad_signature}.

handle_find_record(Dht, #{key := Key})
  when is_binary(Key), byte_size(Key) =:= 32 ->
    classify_local_lookup(macula_dht:find_local_record(Dht, Key));
handle_find_record(_Dht, _Other) ->
    {error, bad_request}.

classify_local_lookup([])           -> {ok, not_found};
classify_local_lookup([Record | _]) -> {ok, Record}.

handle_find_records_by_type(Dht, #{type := Type})
  when is_integer(Type), Type >= 0, Type =< 255 ->
    All = macula_dht:list_records(Dht),
    {ok, [R || R <- All, macula_record:type(R) =:= Type]};
handle_find_records_by_type(_Dht, _Other) ->
    {error, bad_request}.
