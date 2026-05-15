%%% @doc Periodic health beacon — publishes mailbox + heap of the
%%% station's most load-bearing processes on `_mesh.health.v1' so
%%% realm-side dashboards can render a load monitor without needing
%%% a separate management plane.
%%%
%%% Cadence: every `?TICK_MS' (10s). Payload is ETF-encoded — same
%%% process families both sides, no cross-language compat concern.
%%% Realm decodes with `binary_to_term/2' + `[safe]'.
%%%
%%% Why this exists: bloom-convergence shows pubsub routing health,
%%% but a station can be wedged at the BEAM level (peer_observer
%%% mailbox 10k deep, GC-stalled, etc.) while still broadcasting a
%%% perfectly fine bloom every 30s. That's invisible to the
%%% bloom-convergence LV. This module's payload exposes the
%%% process-level signal the bloom view can't see.
-module(macula_station_health_publisher).
-behaviour(gen_server).

-export([start_link/1, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(TICK_MS, 10_000).
-define(MESH_REALM, <<0:256>>).
-define(TOPIC, <<"_mesh.health.v1">>).

%% Processes worth reporting. Each entry is `{atom_name, display_label}'.
%% `display_label' is a binary the realm UI shows. Atom is what
%% `whereis/1' takes locally; binary travels on the wire so the
%% realm doesn't need the atoms registered.
-define(TRACKED, [
    {macula_station_peer_observer,     <<"peer_observer">>},
    {macula_station_peering_router,    <<"peering_router">>},
    {macula_station_pubsub_dispatcher, <<"pubsub_dispatcher">>},
    {macula_station_bloom_exchange,    <<"bloom_exchange">>},
    {macula_station_record_fanout,     <<"record_fanout">>},
    {macula_dht,                       <<"dht">>},
    {macula_dht_replicate,             <<"dht_replicate">>},
    {macula_station_event_dedup,       <<"event_dedup">>}
]).

-record(state, {
    identity   :: macula_identity:key_pair(),
    timer_ref  :: reference() | undefined
}).

-type opts() :: #{identity := macula_identity:key_pair()}.
-export_type([opts/0]).

%%====================================================================
%% API
%%====================================================================

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(#{identity := _} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) -> gen_server:stop(Pid).

%%====================================================================
%% gen_server
%%====================================================================

init(#{identity := Kp}) ->
    process_flag(trap_exit, true),
    {ok, schedule_tick(#state{identity = Kp})}.

handle_call(_Msg, _From, S) -> {reply, {error, unknown_call}, S}.
handle_cast(_Msg, S) -> {noreply, S}.

handle_info({tick, Ref}, #state{timer_ref = Ref, identity = Kp} = S) ->
    Payload = build_payload(Kp),
    broadcast(Payload),
    {noreply, schedule_tick(S)};
handle_info(_, S) ->
    {noreply, S}.

terminate(_Reason, _S) -> ok.
code_change(_OldVsn, S, _Extra) -> {ok, S}.

%%====================================================================
%% Internal
%%====================================================================

schedule_tick(S) ->
    Ref = make_ref(),
    erlang:send_after(?TICK_MS, self(), {tick, Ref}),
    S#state{timer_ref = Ref}.

build_payload(Kp) ->
    Procs = lists:filtermap(fun probe/1, ?TRACKED),
    Term = #{
        <<"node_id">> => macula_identity:public(Kp),
        <<"ts_ms">>   => erlang:system_time(millisecond),
        <<"procs">>   => Procs
    },
    erlang:term_to_binary(Term).

probe({RegName, Label}) ->
    case whereis(RegName) of
        undefined ->
            false;
        Pid when is_pid(Pid) ->
            case process_info(Pid, [message_queue_len, heap_size, total_heap_size, reductions]) of
                undefined -> false;
                Info ->
                    {true, #{
                        <<"name">>      => Label,
                        <<"mbox">>      => proplists:get_value(message_queue_len, Info, 0),
                        <<"heap_w">>    => proplists:get_value(total_heap_size, Info, 0),
                        <<"reductions">>=> proplists:get_value(reductions, Info, 0)
                    }}
            end
    end.

broadcast(Payload) ->
    Conns = macula_station_peer_links:connections(),
    lists:foreach(
      fun({_Url, LinkPid}) ->
              catch macula_station_link:publish(LinkPid, ?MESH_REALM, ?TOPIC, Payload)
      end,
      Conns),
    ok.
