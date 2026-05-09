%% @doc Station-level content procedure handlers.
%%
%% Advertises four RPC procedures against the station's
%% `macula_handler_registry' so daemons can put and get
%% content-addressed blobs (chunked + Merkle-rooted) over the
%% existing unary CALL infrastructure:
%%
%% <ul>
%%   <li>`_content.put_manifest' — accepts a manifest map, stores it
%%       in the local content store. Manifest = `chunk_size +
%%       chunks (each with hash) + Merkle root + total size'.</li>
%%   <li>`_content.put_block' — accepts `{mcid, payload}', verifies
%%       the payload's BLAKE3 hash matches the MCID, stores the
%%       block locally.</li>
%%   <li>`_content.get_manifest' — accepts `{mcid}', returns the
%%       manifest map or `not_found'. On local miss this is the
%%       point where iterative discovery to other stations could
%%       eventually be wired in (Phase 4+); for now we return
%%       `not_found' and the caller's daemon retries against
%%       other stations on its own.</li>
%%   <li>`_content.get_block' — accepts `{mcid}', returns the
%%       block payload or `not_found'.</li>
%% </ul>
%%
%% This puts content sharing on the same plumbing as `_dht.*' —
%% no new wire frames needed, no new station-to-station relay
%% protocol. Daemon SDK layers an iterate-providers + reassemble
%% loop on top.
-module(macula_station_content_handlers).
-behaviour(gen_server).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([opts/0]).

-type opts() :: #{
    handler_registry := pid()
}.

-record(state, {
    handler_registry :: pid()
}).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{handler_registry := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

%%====================================================================
%% gen_server
%%====================================================================

init(#{handler_registry := Registry}) ->
    advertise_all(Registry),
    {ok, #state{handler_registry = Registry}}.

handle_call(_Msg, _From, S) -> {reply, {error, unknown_call}, S}.
handle_cast(_Msg, S)        -> {noreply, S}.
handle_info(_Msg, S)        -> {noreply, S}.
terminate(_Reason, _S)      -> ok.
code_change(_OldVsn, S, _)  -> {ok, S}.

%%====================================================================
%% Procedure advertisement
%%====================================================================

advertise_all(Registry) ->
    ok = macula_handler_registry:advertise(
           Registry, <<"_content.put_manifest">>,
           fun handle_put_manifest/1),
    ok = macula_handler_registry:advertise(
           Registry, <<"_content.put_block">>,
           fun handle_put_block/1),
    ok = macula_handler_registry:advertise(
           Registry, <<"_content.get_manifest">>,
           fun handle_get_manifest/1),
    ok = macula_handler_registry:advertise(
           Registry, <<"_content.get_block">>,
           fun handle_get_block/1),
    ok.

%%====================================================================
%% Handler bodies
%%====================================================================

handle_put_manifest(#{manifest := Manifest}) when is_map(Manifest) ->
    ok = macula_content_store:put_manifest(Manifest),
    {ok, ok};
handle_put_manifest(_Other) ->
    {error, bad_request}.

%% Block put validates the payload hashes to the MCID before storing
%% (`macula_content_store:put_block/2' performs the BLAKE3 check and
%% returns `{error, hash_mismatch}' on tampered uploads). The CALL
%% relay path doesn't validate content integrity, so this is the
%% chokepoint that keeps the local store consistent.
handle_put_block(#{mcid := MCID, payload := Payload})
  when is_binary(MCID), is_binary(Payload) ->
    classify_put_block(macula_content_store:put_block(MCID, Payload));
handle_put_block(_Other) ->
    {error, bad_request}.

classify_put_block(ok)                       -> {ok, ok};
classify_put_block({error, hash_mismatch})   -> {ok, hash_mismatch};
classify_put_block({error, Other})           -> {ok, {put_block_failed, Other}}.

handle_get_manifest(#{mcid := MCID}) when is_binary(MCID) ->
    classify_get_manifest(macula_content_store:get_manifest(MCID));
handle_get_manifest(_Other) ->
    {error, bad_request}.

classify_get_manifest({ok, Manifest})        -> {ok, Manifest};
classify_get_manifest({error, not_found})    -> {ok, not_found}.

handle_get_block(#{mcid := MCID}) when is_binary(MCID) ->
    classify_get_block(macula_content_store:get_block(MCID));
handle_get_block(_Other) ->
    {error, bad_request}.

classify_get_block({ok, Block})              -> {ok, Block};
classify_get_block({error, not_found})       -> {ok, not_found}.
