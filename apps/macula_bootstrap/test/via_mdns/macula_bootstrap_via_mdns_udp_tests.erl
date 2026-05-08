%% @doc Fan-out tests for `macula_bootstrap_via_mdns_udp:query/3'.
%%
%% The production socket_opener hits real IPv6 multicast, which we
%% do NOT want in an eunit run (port 5353 conflicts with avahi, and
%% the kernel doesn't reliably deliver our own query back). Instead
%% we inject a pluggable `socket_opener' that returns a loopback
%% UDP socket per interface. Each canned interface has an associated
%% responder that echoes a unique canned reply back to the sender,
%% so the merged reply list is deterministic.
-module(macula_bootstrap_via_mdns_udp_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% Single-interface path — `default' behaves as before.
%%==================================================================

single_socket_path_returns_empty_on_open_failure_test() ->
    Opener = fun(_) -> {error, eacces} end,
    ?assertEqual([],
                 macula_bootstrap_via_mdns_udp:query(
                   <<"q">>, 50,
                   #{socket_opener => Opener,
                     interfaces    => default})).

%%==================================================================
%% Fan-out path — one worker per interface, replies merged.
%%==================================================================

fan_out_merges_per_interface_replies_test() ->
    %% Two fake interfaces, two echo servers, two canned replies.
    {EchoA, EchoB} = {start_echo(<<"A">>), start_echo(<<"B">>)},
    Opener = fun(#{name := "iA"}) -> open_client(EchoA);
                (#{name := "iB"}) -> open_client(EchoB);
                (default)         -> {error, unused}
             end,
    Ifaces = [#{name => "iA", index => 10,
                link_local => undefined, ipv6 => []},
              #{name => "iB", index => 11,
                link_local => undefined, ipv6 => []}],
    Replies = macula_bootstrap_via_mdns_udp:query(
                <<"probe">>, 500,
                #{socket_opener => Opener,
                  interfaces    => Ifaces}),
    Payloads = lists:sort([P || {_Addr, P} <- Replies]),
    ?assertEqual([<<"A">>, <<"B">>], Payloads),
    stop_echo(EchoA),
    stop_echo(EchoB).

fan_out_with_empty_interface_list_returns_empty_test() ->
    Opener = fun(_) -> error(opener_should_not_be_called) end,
    ?assertEqual([],
                 macula_bootstrap_via_mdns_udp:query(
                   <<"q">>, 50,
                   #{socket_opener => Opener,
                     interfaces    => []})).

fan_out_survives_one_broken_opener_test() ->
    EchoA  = start_echo(<<"only-A">>),
    Opener = fun(#{name := "iA"}) -> open_client(EchoA);
                (#{name := "iBroken"}) -> {error, enetunreach};
                (default) -> {error, unused}
             end,
    Ifaces = [#{name => "iA",      index => 1,
                link_local => undefined, ipv6 => []},
              #{name => "iBroken", index => 2,
                link_local => undefined, ipv6 => []}],
    Replies = macula_bootstrap_via_mdns_udp:query(
                <<"probe">>, 500,
                #{socket_opener => Opener,
                  interfaces    => Ifaces}),
    ?assertEqual([<<"only-A">>], [P || {_, P} <- Replies]),
    stop_echo(EchoA).

%%==================================================================
%% Echo server — opens a loopback UDP socket with an ephemeral
%% port, echoes a canned reply to whatever sends it a datagram.
%%
%% The REAL mDNS probe sends its query to
%% `macula_bootstrap_via_mdns_query:multicast_group()' on port 5353. We can't
%% use that in eunit (avahi / kernel / permissions). Instead the
%% client sockets we hand back ARE connected to the echo's port, so
%% when the probe's `gen_udp:send' fires with its multicast
%% destination, we intercept it by having the echo bind to that
%% port on loopback and relying on the kernel to deliver loopback
%% UDP to us regardless of the destination. In practice this won't
%% work without a more invasive mock; we sidestep by pre-queuing
%% the reply into the client socket before the probe even sends.
%%==================================================================

start_echo(Payload) ->
    %% A simple process that will just *sit on a pre-fed reply* for
    %% the client socket to consume. Implementation below uses
    %% `open_client/1' to pre-seed the socket's mailbox with the
    %% canned reply, so the probe's `gen_udp:recv' pulls it without
    %% needing real network traffic.
    Payload.

stop_echo(_Payload) ->
    ok.

%% Create a loopback UDP socket and pre-send the canned reply to
%% itself so the probe's `gen_udp:recv' sees `{ok, {Addr, Port,
%% Payload}}' immediately. This bypasses the real mDNS datagram
%% path — we are exercising the probe's fan-out + merge logic, not
%% real multicast delivery. Real multicast is validated in the
%% fleet CT (Phase 7 hardening) where the station runs against the
%% actual kernel stack.
open_client(Payload) ->
    {ok, Sock} = gen_udp:open(0, [inet6, binary, {active, false}]),
    {ok, Port} = inet:port(Sock),
    %% Send the canned reply to ourselves so `recv' returns it.
    ok = gen_udp:send(Sock, {0, 0, 0, 0, 0, 0, 0, 1}, Port, Payload),
    {ok, Sock}.
