%% @doc DHT outbound transport adapter.
%%
%% Stateless module installed as the DHT's `send_frame' callback at
%% station boot. Resolves a target NodeId to its peer connection via
%% the registered `macula_station_peer_observer' and pushes the frame
%% over the existing `macula_peering' connection.
%%
%% Lookups happen per send (`whereis/1' + `gen_server:call' to the
%% observer). This is deliberately restart-tolerant: an observer
%% restart does not invalidate a captured pid because no pid is
%% captured. Cost per send is one `whereis/1' plus one observer
%% call, both microsecond-class.
%%
%% On a missing route the call returns `{error, no_route}' so the DHT
%% can decide whether to retry, drop, or surface the failure to its
%% caller. No retries happen here.
%%
%% Reference: PLAN_DHT_TRANSPORT_WIRING §2 option (C).
-module(macula_station_dht_transport).

-export([send_frame/2]).

-spec send_frame(macula_identity:pubkey(), macula_frame:frame()) ->
        ok | {error, no_observer | no_route | term()}.
send_frame(<<_:256>> = NodeId, Frame) when is_map(Frame) ->
    deliver(whereis(macula_station_peer_observer), NodeId, Frame).

deliver(undefined, _NodeId, _Frame) ->
    {error, no_observer};
deliver(Observer, NodeId, Frame) when is_pid(Observer) ->
    route(macula_station_peer_observer:conn_for(Observer, NodeId), Frame).

route({ok, ConnPid}, Frame) ->
    macula_peering:send_frame(ConnPid, Frame);
route(error, _Frame) ->
    {error, no_route}.
