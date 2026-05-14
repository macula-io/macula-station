-module(macula_station_log_filters_tests).

-include_lib("eunit/include/eunit.hrl").

drops_pubsub_signature_invalid_warning_test() ->
    Event = #{level => warning,
              msg   => {"[peer_observer] pubsub frame verify failed: ~p",
                        [signature_invalid]},
              meta  => #{}},
    ?assertEqual(stop,
                 macula_station_log_filters:drop_pubsub_sig_invalid(Event, [])).

drops_pubsub_signature_invalid_for_other_reasons_too_test() ->
    %% Same prefix, different reason atom — still a pubsub-verify
    %% noise event, drop it.
    Event = #{level => warning,
              msg   => {"[peer_observer] pubsub frame verify failed: ~p",
                        [malformed_envelope]},
              meta  => #{}},
    ?assertEqual(stop,
                 macula_station_log_filters:drop_pubsub_sig_invalid(Event, [])).

drops_dispatcher_verify_failed_warning_test() ->
    %% SDK 4.4.4 dispatcher path emits its own verify-failed warning
    %% with a richer format. Same load-bearing loop-kill, same drop.
    Event = #{level => warning,
              msg   => {"[pubsub_dispatcher] verify failed: ~p type=~p "
                        "topic=~s realm=~s peer_node_id=~s publisher=~s "
                        "has_publisher_sig=~p",
                        [signature_invalid, event, <<"_mesh.bloom">>,
                         <<"abc">>, <<"def">>, <<"ghi">>, false]},
              meta  => #{}},
    ?assertEqual(stop,
                 macula_station_log_filters:drop_pubsub_sig_invalid(Event, [])).

passes_unrelated_warning_test() ->
    Event = #{level => warning,
              msg   => {"[handler_dispatch] handler crashed: ~p", [boom]},
              meta  => #{}},
    ?assertEqual(ignore,
                 macula_station_log_filters:drop_pubsub_sig_invalid(Event, [])).

passes_info_level_with_same_prefix_test() ->
    %% Filter only fires on `warning' level — info / debug / error
    %% with the same prefix would still pass through.
    Event = #{level => info,
              msg   => {"[peer_observer] pubsub frame verify failed: ~p",
                        [signature_invalid]},
              meta  => #{}},
    ?assertEqual(ignore,
                 macula_station_log_filters:drop_pubsub_sig_invalid(Event, [])).

passes_report_style_message_test() ->
    %% Reports (logger:warning(#{...})) don't have a Format string —
    %% pattern-clause falls through to ignore.
    Event = #{level => warning,
              msg   => {report, #{kind => something_else}},
              meta  => #{}},
    ?assertEqual(ignore,
                 macula_station_log_filters:drop_pubsub_sig_invalid(Event, [])).

install_is_idempotent_test() ->
    ?assertEqual(ok, macula_station_log_filters:install()),
    ?assertEqual(ok, macula_station_log_filters:install()),
    %% Cleanup so other tests don't see the filter.
    _ = logger:remove_handler_filter(default, drop_pubsub_sig_invalid),
    ok.
