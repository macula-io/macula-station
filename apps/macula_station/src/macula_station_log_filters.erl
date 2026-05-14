%%% @doc Logger filters installed by the station application.
%%%
%%% Drops the high-volume pubsub-verify-failed warnings emitted by
%%% both `peer_observer' (legacy path) and `pubsub_dispatcher' (the
%%% SDK 4.4.4 dispatcher path). The warning fires whenever a
%%% multi-hop pubsub EVENT loops back to a station that didn't sign
%%% the original frame — verify runs against the immediate-sender
%%% NodeId, which fails for relayed-and-not-resigned frames. The
%%% drop is the load-bearing loop kill for the bloom-mesh fan-out:
%%% see `project_pubsub_resign_loop_lesson' in the operator's memory
%%% for why every other workaround we tried regressed cross-station
%%% traffic. As a result the warnings are non-blocking but produce
%%% high log volume; this filter suppresses them without changing
%%% the protocol.
%%%
%%% A proper Phase 2 fix migrates EVENT to a publisher-end-to-end
%%% signed envelope (UCAN), at which point the verify step changes
%%% and this filter becomes obsolete.
-module(macula_station_log_filters).

-export([install/0, drop_pubsub_sig_invalid/2]).

-define(PEER_OBSERVER_PREFIX,
        "[peer_observer] pubsub frame verify failed:").
-define(DISPATCHER_PREFIX,
        "[pubsub_dispatcher] verify failed:").

%% @doc Add the pubsub-signature-invalid filter to the default
%% handler. Idempotent: re-installing returns `{error, exists}'
%% which we swallow.
-spec install() -> ok.
install() ->
    case logger:add_handler_filter(
            default,
            drop_pubsub_sig_invalid,
            {fun ?MODULE:drop_pubsub_sig_invalid/2, []}) of
        ok                  -> ok;
        {error, {already_exist, _}} -> ok;
        {error, _Reason}    -> ok
    end.

%% @doc Logger filter callback. Returns `stop' to suppress matching
%% messages; `ignore' to leave non-matching messages unchanged.
-spec drop_pubsub_sig_invalid(logger:log_event(), term()) ->
        logger:filter_return().
drop_pubsub_sig_invalid(#{level := warning,
                          msg := {Fmt, _Args}}, _) when is_list(Fmt) ->
    classify(Fmt);
drop_pubsub_sig_invalid(_LogEvent, _) ->
    ignore.

classify(Fmt) ->
    case match_any(Fmt, [?PEER_OBSERVER_PREFIX, ?DISPATCHER_PREFIX]) of
        true  -> stop;
        false -> ignore
    end.

match_any(_Fmt, []) -> false;
match_any(Fmt, [P | Rest]) ->
    case string:prefix(Fmt, P) of
        nomatch -> match_any(Fmt, Rest);
        _       -> true
    end.
