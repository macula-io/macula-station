%% @doc mDNS responder (Tier B advertise side, Part 5 §5).
%%
%% Publishes this station on the link-local segment so peers running
%% Tier B probes find us. Listens on the mDNS group
%% `[ff02::fb]:5353' for queries matching `_macula._udp.local' and
%% answers with a TXT record carrying `node_id=<hex>', `port=...',
%% `tier=...'. The response is unsigned — peers corroborate via a
%% QUIC handshake (Part 5 §5.1), so a LAN adversary replaying our
%% TXT gains nothing without our private key.
%%
%% == Lifecycle ==
%%
%% A gen_server owns the UDP socket and stays running for the
%% station's lifetime. The `socket_opener' opt is pluggable so unit
%% tests can hand the responder an ephemeral-port loopback socket
%% instead of joining the real multicast group (joining port 5353
%% conflicts with `avahi-daemon' on typical Linux hosts; production
%% deployments either disable avahi or add `SO_REUSEPORT').
%%
%% == Privacy (O6) ==
%%
%% `silent => true' starts the responder with the socket bound but
%% drops every query. Useful for operators who want to receive
%% without advertising. Default `silent => false' follows Part 5 §5.3
%% rationale (LAN is already a trust boundary).
-module(macula_bootstrap_mdns_responder).
-behaviour(gen_server).

-export([
    start_link/1,
    stop/1,
    port/1,
    set_silent/2
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export_type([start_opts/0, socket_opener/0]).

-type socket_opener() :: fun(() -> {ok, gen_udp:socket()} | {error, term()}).

-type start_opts() :: #{
    node_id := macula_identity:pubkey(),
    port    := 1..65535,
    tier    := 0..4,
    silent  => boolean(),
    socket_opener => socket_opener()
}.

-record(state, {
    socket    :: gen_udp:socket(),
    node_info :: macula_bootstrap_mdns:node_info(),
    silent    :: boolean()
}).

%%==================================================================
%% API
%%==================================================================

-spec start_link(start_opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:stop(Pid).

%% @doc The local UDP port the responder is bound to. Useful in
%% tests — production always binds to the well-known mDNS port.
-spec port(pid()) -> 1..65535.
port(Pid) ->
    gen_server:call(Pid, port).

-spec set_silent(pid(), boolean()) -> ok.
set_silent(Pid, Silent) when is_boolean(Silent) ->
    gen_server:call(Pid, {set_silent, Silent}).

%%==================================================================
%% gen_server callbacks
%%==================================================================

init(Opts) ->
    NodeInfo = node_info(Opts),
    Silent   = maps:get(silent, Opts, false),
    Opener   = maps:get(socket_opener, Opts, fun default_opener/0),
    open(Opener(), NodeInfo, Silent).

open({ok, Sock}, NodeInfo, Silent) ->
    {ok, #state{socket = Sock, node_info = NodeInfo, silent = Silent}};
open({error, Reason}, _NodeInfo, _Silent) ->
    {stop, {open_failed, Reason}}.

node_info(#{node_id := NodeId, port := Port, tier := Tier})
  when is_binary(NodeId), byte_size(NodeId) =:= 32,
       is_integer(Port), Port >= 1, Port =< 65535,
       is_integer(Tier), Tier >= 0, Tier =< 4 ->
    #{node_id => NodeId, port => Port, tier => Tier}.

handle_call(port, _From, #state{socket = Sock} = S) ->
    {ok, Port} = inet:port(Sock),
    {reply, Port, S};
handle_call({set_silent, Silent}, _From, S) ->
    {reply, ok, S#state{silent = Silent}};
handle_call(_Msg, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Msg, S) ->
    {noreply, S}.

handle_info({udp, Sock, SrcAddr, SrcPort, Bin},
            #state{socket = Sock} = S) ->
    maybe_reply(SrcAddr, SrcPort, Bin, S),
    ok = inet:setopts(Sock, [{active, once}]),
    {noreply, S};
handle_info(_Msg, S) ->
    {noreply, S}.

terminate(_Reason, #state{socket = Sock}) ->
    catch gen_udp:close(Sock),
    ok.

code_change(_Old, S, _Extra) ->
    {ok, S}.

%%==================================================================
%% Internal
%%==================================================================

maybe_reply(_Src, _SPort, _Bin, #state{silent = true}) ->
    ok;
maybe_reply(Src, SPort, Bin, #state{socket = Sock, node_info = Info}) ->
    send(macula_bootstrap_mdns:build_advertisement(Bin, Info),
         Sock, Src, SPort).

send({ok, RespBin}, Sock, Src, SPort) ->
    gen_udp:send(Sock, Src, SPort, RespBin);
send(ignore, _Sock, _Src, _SPort) ->
    ok.

default_opener() ->
    gen_udp:open(
      macula_bootstrap_mdns:multicast_port(),
      [inet6, binary, {active, once}, {reuseaddr, true},
       {multicast_loop, false}, {multicast_ttl, 1},
       {add_membership,
        {macula_bootstrap_mdns:multicast_group(),
         {0, 0, 0, 0, 0, 0, 0, 0}}}]).
