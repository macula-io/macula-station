%% @doc Bitswap-style content transfer protocol.
%%
%% Tracks outbound block requests and processes inbound WANT / HAVE /
%% BLOCK / MANIFEST_REQ / MANIFEST_RES / CANCEL frames. All wire
%% framing goes through `macula_frame'; signing is the caller's
%% responsibility (`macula_frame:sign/2') so this module stays free
%% of identity state.
%%
%% This gen_server does not own the network — wire I/O lives in the
%% station listener / handler modules. Inbound frames are dispatched
%% into here via `process_inbound/2'; outbound frames are built by
%% the pure builder functions and returned to the caller for
%% transmission.
%%
%% Named `_bitswap', not `_transfer': the `macula' SDK dependency
%% ships its own unrelated `macula_content_transfer' (client-side
%% put/get, added in 9.9.0) — same name, different app, different
%% purpose. `rebar3 release' hard-fails on duplicate module names
%% across included apps, so this collided fatally the moment `macula'
%% crossed 9.9.0. Renamed this side (the consumer) rather than the SDK.
-module(macula_content_bitswap).
-behaviour(gen_server).

-export([
    start_link/0, start_link/1, stop/0,

    %% Outbound — pure builders; caller signs the result
    build_want/1, build_want/2,
    build_have/1,
    build_manifest_req/1,
    build_cancel/1,

    %% Request tracking (gen_server state)
    request_blocks/2, pending_requests/0,
    complete_request/1, cancel_request/1, request_info/1,

    %% Inbound — process frames received over the wire
    process_inbound/2
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).
-define(DEFAULT_PRIORITY, 128).

-record(state, {
    requests :: ets:tid()
}).

-record(request, {
    mcids       :: [macula_frame:mcid()],
    target_node :: binary(),
    created_at  :: integer(),
    status      :: pending | complete | cancelled
}).

%%====================================================================
%% Lifecycle
%%====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() -> start_link(#{}).

-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, Opts, []).

-spec stop() -> ok.
stop() -> gen_server:stop(?SERVER).

%%====================================================================
%% Outbound frame builders (pure)
%%====================================================================

-spec build_want([macula_frame:mcid() | macula_frame:want_entry()]) ->
        macula_frame:frame().
build_want(McidsOrEntries) ->
    build_want(McidsOrEntries, ?DEFAULT_PRIORITY).

-spec build_want([macula_frame:mcid() | macula_frame:want_entry()],
                 macula_frame:want_priority()) ->
        macula_frame:frame().
build_want(McidsOrEntries, DefaultPrio) ->
    Entries = [normalise_want(E, DefaultPrio) || E <- McidsOrEntries],
    macula_frame:want(#{blocks => Entries}).

-spec build_have([{macula_frame:mcid(), non_neg_integer()}]) ->
        macula_frame:frame().
build_have(Entries) ->
    Blocks = [#{mcid => M, size => S} || {M, S} <- Entries],
    macula_frame:have(#{blocks => Blocks}).

-spec build_manifest_req(macula_frame:mcid()) -> macula_frame:frame().
build_manifest_req(MCID) ->
    macula_frame:manifest_req(#{mcid => MCID}).

-spec build_cancel([macula_frame:mcid()]) -> macula_frame:frame().
build_cancel(MCIDs) ->
    macula_frame:cancel(#{blocks => MCIDs}).

normalise_want(MCID, DefaultPrio) when is_binary(MCID) ->
    #{mcid => MCID, priority => DefaultPrio};
normalise_want(#{mcid := _} = Entry, _DefaultPrio) ->
    Entry.

%%====================================================================
%% Request tracking
%%====================================================================

-spec request_blocks([macula_frame:mcid()], binary()) -> {ok, binary()}.
request_blocks(MCIDs, TargetNode) ->
    gen_server:call(?SERVER, {request_blocks, MCIDs, TargetNode}).

-spec pending_requests() -> [binary()].
pending_requests() ->
    gen_server:call(?SERVER, pending_requests).

-spec complete_request(binary()) -> ok.
complete_request(RequestId) ->
    gen_server:call(?SERVER, {complete_request, RequestId}).

-spec cancel_request(binary()) -> ok.
cancel_request(RequestId) ->
    gen_server:call(?SERVER, {cancel_request, RequestId}).

-spec request_info(binary()) -> {ok, map()} | {error, not_found}.
request_info(RequestId) ->
    gen_server:call(?SERVER, {request_info, RequestId}).

%%====================================================================
%% Inbound dispatch
%%====================================================================

%% @doc Process an inbound content frame. `From' is the verified
%% sender pubkey (caller has already run `macula_frame:verify/2').
%%
%% Returns `{ok, Frame}' for the frame the caller should send back
%% (BLOCK in response to WANT, MANIFEST_RES in response to
%% MANIFEST_REQ), `ok' for fire-and-forget frames (HAVE,
%% MANIFEST_RES, CANCEL), or `{error, Reason}' on failure.
-spec process_inbound(macula_identity:pubkey(), macula_frame:frame()) ->
        ok | {ok, macula_frame:frame()} | {error, term()}.
process_inbound(_From, #{frame_type := want} = Frame) ->
    gen_server:call(?SERVER, {process_want, Frame});
process_inbound(_From, #{frame_type := have}) ->
    %% HAVE is informational — caller's higher-level logic decides
    %% whether to issue a follow-up WANT.
    ok;
process_inbound(_From, #{frame_type := block} = Frame) ->
    gen_server:call(?SERVER, {process_block, Frame});
process_inbound(_From, #{frame_type := manifest_req} = Frame) ->
    gen_server:call(?SERVER, {process_manifest_req, Frame});
process_inbound(_From, #{frame_type := manifest_res}) ->
    %% MANIFEST_RES content is consumed by the requester directly;
    %% no server-side state mutation here.
    ok;
process_inbound(_From, #{frame_type := cancel} = Frame) ->
    gen_server:cast(?SERVER, {process_cancel, Frame}),
    ok.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(_Opts) ->
    {ok, #state{
        requests = ets:new(content_requests, [set, private])
    }}.

handle_call({request_blocks, MCIDs, TargetNode}, _From, S) ->
    RequestId = new_request_id(),
    Req = #request{
        mcids       = MCIDs,
        target_node = TargetNode,
        created_at  = erlang:system_time(millisecond),
        status      = pending
    },
    ets:insert(S#state.requests, {RequestId, Req}),
    {reply, {ok, RequestId}, S};
handle_call(pending_requests, _From, S) ->
    Pending = ets:foldl(
        fun({Id, #request{status = pending}}, Acc) -> [Id | Acc];
           (_, Acc) -> Acc end,
        [], S#state.requests),
    {reply, Pending, S};
handle_call({complete_request, Id}, _From, S) ->
    ets:delete(S#state.requests, Id),
    {reply, ok, S};
handle_call({cancel_request, Id}, _From, S) ->
    ets:delete(S#state.requests, Id),
    {reply, ok, S};
handle_call({request_info, Id}, _From, S) ->
    {reply, lookup_request(Id, S), S};
handle_call({process_want, Frame}, _From, S) ->
    {reply, do_process_want(Frame), S};
handle_call({process_block, Frame}, _From, S) ->
    {reply, do_process_block(Frame), S};
handle_call({process_manifest_req, Frame}, _From, S) ->
    {reply, do_process_manifest_req(Frame), S}.

handle_cast({process_cancel, Frame}, S) ->
    drop_cancelled(maps:get(blocks, Frame, []), S),
    {noreply, S};
handle_cast(_, S) ->
    {noreply, S}.

handle_info(_, S) -> {noreply, S}.
terminate(_, _) -> ok.

%%====================================================================
%% Internal — inbound processing
%%====================================================================

do_process_want(#{blocks := []}) ->
    {error, empty_wants};
do_process_want(#{blocks := [#{mcid := MCID} | _]}) ->
    fetch_for_want(macula_content_store:get_block(MCID), MCID).

fetch_for_want({ok, Data}, MCID) ->
    {ok, macula_frame:block(#{mcid => MCID, payload => Data})};
fetch_for_want({error, _} = E, _MCID) ->
    E.

do_process_block(#{mcid := MCID, payload := Data}) ->
    macula_content_store:put_block(MCID, Data).

do_process_manifest_req(#{mcid := MCID}) ->
    fetch_manifest_response(macula_content_store:get_manifest(MCID), MCID).

fetch_manifest_response({ok, Manifest}, MCID) ->
    {ok, macula_frame:manifest_res(#{mcid => MCID, manifest => Manifest})};
fetch_manifest_response({error, not_found}, MCID) ->
    {ok, macula_frame:manifest_res(#{mcid => MCID, manifest => not_found})}.

drop_cancelled(MCIDs, S) ->
    ets:foldl(
        fun({Id, #request{mcids = M}}, _Acc) ->
            cancel_if_match(any_match(M, MCIDs), Id, S)
        end, ok, S#state.requests).

cancel_if_match(true, Id, S)  -> ets:delete(S#state.requests, Id), ok;
cancel_if_match(false, _, _S) -> ok.

any_match([], _)        -> false;
any_match([H | T], List) -> lists:member(H, List) orelse any_match(T, List).

lookup_request(Id, S) ->
    case ets:lookup(S#state.requests, Id) of
        []                       -> {error, not_found};
        [{Id, #request{} = Req}] -> {ok, request_info_map(Req)}
    end.

request_info_map(#request{mcids = M, target_node = T,
                          created_at = C, status = St}) ->
    #{mcids => M, target_node => T, created_at => C, status => St}.

new_request_id() ->
    crypto:strong_rand_bytes(16).
