%% @doc Adapter over `macula_quic' (in the macula SDK 3.1+).
%%
%% Translates the legacy v2-era `macula_transport' API (umbrella
%% wrapper that hecate-station was originally built against) into
%% the SDK's flat `macula_quic' API. Exists so the station's
%% listener / peering code can keep its option-map calling
%% conventions while the underlying transport lives in the SDK
%% where it belongs.
%%
%% Type aliases preserve the legacy names — `listener()',
%% `connection()', `stream()', `connect_opts()' — so existing
%% type specs across the station continue to type-check.
-module(hecate_transport).

-export([
    listen/1,
    accept/1,
    close_listener/1,
    connect/1,
    controlling_process_conn/2,
    open_stream/1,
    accept_stream/1,
    setopt_active/2,
    send/2,
    close_connection/1
]).

-export_type([listener/0, connection/0, stream/0, connect_opts/0]).

-type listener()     :: reference().
-type connection()   :: reference().
-type stream()       :: reference().
-type connect_opts() :: #{
    host        := binary() | string(),
    port        := inet:port_number(),
    cert        => binary() | string(),
    key         => binary() | string(),
    timeout_ms  => timeout(),
    _           => _
}.

%%====================================================================
%% Listener
%%====================================================================

-spec listen(map()) -> {ok, listener()} | {error, term()}.
listen(#{bind := Bind, port := Port} = Opts) ->
    Cert = maps:get(certfile, Opts),
    Key  = maps:get(keyfile, Opts),
    Alpn = maps:get(alpn, Opts, [<<"macula">>]),
    macula_quic:listen(Bind, Port, [
        {cert, Cert},
        {key,  Key},
        {alpn, Alpn}
    ]).

-spec accept(listener()) -> ok | {error, term()}.
accept(Listener) ->
    macula_quic:async_accept(Listener).

-spec close_listener(listener()) -> ok.
close_listener(Listener) ->
    macula_quic:close_listener(Listener).

%%====================================================================
%% Connection
%%====================================================================

-spec connect(connect_opts()) -> {ok, connection()} | {error, term()}.
connect(#{host := Host, port := Port} = Opts) ->
    Timeout = maps:get(timeout_ms, Opts, 30000),
    Tls = case maps:find(cert, Opts) of
              {ok, Cert} -> [{cert, Cert}, {key, maps:get(key, Opts)}];
              error      -> []
          end,
    Alpn = maps:get(alpn, Opts, [<<"macula">>]),
    macula_quic:connect(Host, Port, Tls ++ [{alpn, Alpn}], Timeout).

-spec controlling_process_conn(connection(), pid()) -> ok | {error, term()}.
controlling_process_conn(Conn, Pid) ->
    macula_quic:controlling_process(Conn, Pid).

-spec close_connection(connection()) -> ok.
close_connection(Conn) ->
    macula_quic:close_connection(Conn).

%%====================================================================
%% Stream
%%====================================================================

-spec open_stream(connection()) -> {ok, stream()} | {error, term()}.
open_stream(Conn) ->
    macula_quic:open_stream(Conn).

-spec accept_stream(connection()) -> ok | {error, term()}.
accept_stream(Conn) ->
    macula_quic:async_accept_stream(Conn).

-spec setopt_active(stream(), boolean()) -> ok.
setopt_active(Stream, Active) when is_boolean(Active) ->
    macula_quic:setopt(Stream, active, Active).

-spec send(stream(), iodata()) -> ok | {error, term()}.
send(Stream, Data) ->
    macula_quic:send(Stream, Data).
