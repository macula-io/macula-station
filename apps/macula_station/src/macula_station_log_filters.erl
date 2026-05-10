%%% @doc Logger filters installed by the station application.
%%%
%%% Drops the high-volume `[peer_observer] pubsub frame verify failed:
%%% signature_invalid' warning. The warning fires whenever a multi-hop
%%% pubsub EVENT loops back to a station that didn't sign the original
%%% frame — `peer_observer' verifies against the immediate-sender
%%% NodeId, which fails for relayed-and-not-resigned frames. The drop
%%% is the load-bearing loop kill for the bloom-mesh fan-out: see
%%% `project_pubsub_resign_loop_lesson' in the operator's memory for
%%% why every other workaround we tried regressed cross-station
%%% traffic. As a result the warning is non-blocking but produces
%%% high log volume; this filter suppresses it without changing the
%%% protocol.
%%%
%%% A proper Phase 2 fix migrates EVENT to a publisher-end-to-end
%%% signed envelope (UCAN), at which point the verify step changes
%%% and this filter becomes obsolete.
-module(macula_station_log_filters).

-export([install/0, drop_pubsub_sig_invalid/2]).

-define(SIG_INVALID_PREFIX,
        "[peer_observer] pubsub frame verify failed:").

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
    classify(string:prefix(Fmt, ?SIG_INVALID_PREFIX));
drop_pubsub_sig_invalid(_LogEvent, _) ->
    ignore.

classify(nomatch) -> ignore;
classify(_Rest)   -> stop.
