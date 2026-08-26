%% @doc CT suite — Phase 3.5 end-to-end: point-to-point overlay relay by
%% NodeId. Two independent `macula_station_link' connections into one
%% real station; A sends an `overlay_relay'-wrapped frame targeting B's
%% own pubkey; B receives it via `overlay_subscribe/3' with the correct
%% sender attribution. No HyParView protocol logic involved — this suite
%% only proves the transport primitive the dispatcher (Layer 2 plan
%% Phase 4) will be built on top of.
-module(macula_station_overlay_relay_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([suite/0, all/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([overlay_relay_reaches_other_connection/1,
         overlay_relay_to_unconnected_peer_is_silently_dropped/1]).

-define(REALM, <<0:256>>).

suite() ->
    [{timetrap, {minutes, 5}}].

all() ->
    [overlay_relay_reaches_other_connection,
     overlay_relay_to_unconnected_peer_is_silently_dropped].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(macula),
    Config.
end_per_suite(_Config) -> ok.

init_per_testcase(_Name, Config) ->
    PrivDir = ?config(priv_dir, Config),
    [{cluster_opts, #{base_dir => PrivDir}} | Config].
end_per_testcase(_Name, _Config) -> ok.

%%------------------------------------------------------------------
%% overlay_relay_reaches_other_connection
%%------------------------------------------------------------------

overlay_relay_reaches_other_connection(Config) ->
    Opts    = ?config(cluster_opts, Config),
    [St]    = macula_station_test_cluster:spawn_cluster(1, Opts),
    Url     = station_url(macula_station_test_cluster:listen_addr(St)),

    AKp = macula_identity:generate(),
    BKp = macula_identity:generate(),
    APub = macula_identity:public(AKp),
    BPub = macula_identity:public(BKp),

    {ok, A} = dial(Url, AKp),
    {ok, B} = dial(Url, BKp),
    try
        ok = wait_connected(A),
        ok = wait_connected(B),

        {ok, SubRef} = macula_station_link:overlay_subscribe(B, ?REALM, self()),

        Frame = macula_frame:sign(
                  macula_frame:hyparview_disconnect(#{realm => ?REALM}), AKp),
        ok = macula_station_link:send_overlay_frame(A, BPub, Frame),

        receive
            {macula_overlay_frame, R, RecvFrame, Meta} ->
                ?assertEqual(SubRef, R),
                ?assertEqual(Frame, RecvFrame),
                ?assertEqual(hyparview_disconnect,
                             macula_frame:frame_type(RecvFrame)),
                %% The relayed frame's sender is A's OWN identity — not
                %% the station's, and not B's. This is the sender-
                %% attribution fix's end-to-end proof.
                ?assertEqual(APub, maps:get(sender, Meta))
        after 5_000 -> ct:fail(overlay_frame_not_relayed)
        end
    after
        macula_station_link:stop(A),
        macula_station_link:stop(B),
        macula_station_test_cluster:stop_cluster([St])
    end.

%%------------------------------------------------------------------
%% overlay_relay_to_unconnected_peer_is_silently_dropped
%%------------------------------------------------------------------

overlay_relay_to_unconnected_peer_is_silently_dropped(Config) ->
    Opts    = ?config(cluster_opts, Config),
    [St]    = macula_station_test_cluster:spawn_cluster(1, Opts),
    Url     = station_url(macula_station_test_cluster:listen_addr(St)),

    AKp = macula_identity:generate(),
    {ok, A} = dial(Url, AKp),
    try
        ok = wait_connected(A),

        %% NobodyPub is not connected to this station at all.
        NobodyPub = macula_identity:public(macula_identity:generate()),
        Frame = macula_frame:sign(
                  macula_frame:hyparview_disconnect(#{realm => ?REALM}), AKp),

        %% No crash, no error reply — a silent drop, same as any other
        %% unrecognised frame the station has always dropped.
        ?assertEqual(ok, macula_station_link:send_overlay_frame(A, NobodyPub, Frame)),
        ?assert(macula_station_link:is_connected(A))
    after
        macula_station_link:stop(A),
        macula_station_test_cluster:stop_cluster([St])
    end.

%%------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------

dial(Url, Kp) ->
    macula_station_link:start_link(#{
        seed                => Url,
        identity            => Kp,
        connect_timeout_ms  => 5_000,
        verify              => none
    }).

wait_connected(Pid) -> wait_connected(Pid, 50).

wait_connected(_Pid, 0) -> {error, timeout};
wait_connected(Pid, N) ->
    case macula_station_link:is_connected(Pid) of
        true  -> ok;
        false -> timer:sleep(100), wait_connected(Pid, N - 1)
    end.

station_url({Ip, Port}) ->
    build_url(list_to_binary(inet:ntoa(Ip)), Port).

build_url(Host, Port) ->
    <<"quic://[", Host/binary, "]:", (integer_to_binary(Port))/binary>>.
