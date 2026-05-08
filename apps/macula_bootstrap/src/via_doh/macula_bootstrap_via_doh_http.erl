%% @doc Concrete DoH resolver — RFC 8484 POST over `inets:httpc'.
%%
%% Thin behaviour-conforming wrapper around `macula_bootstrap_via_doh_resolver':
%% the pure codec builds the DNS query and parses the response; this
%% module plugs in the HTTP transport and the zone-base configuration.
%%
%% == Configuration ==
%%
%% <ul>
%%   <li>`application:get_env(macula_bootstrap, doh_zone_base, _)' —
%%       the DNS zone under which foundation PKARR records are
%%       published (default `&lt;&lt;"macula.io"&gt;&gt;'). Every resolver
%%       sees the same zone: rotating zones across resolvers defeats
%%       corroboration.</li>
%% </ul>
%%
%% == Startup requirement ==
%%
%% `inets' and `ssl' must be running for `httpc:request/4' to work.
%% They are declared as application dependencies of `macula_bootstrap'.
%%
%% This module has no unit tests — its single responsibility is
%% translating `{ok, HttpStatus, Body}' into the shape
%% `macula_bootstrap_via_doh_resolver:resolve/4' expects. Coverage lives in
%% `macula_bootstrap_via_doh_resolver_tests' (codec) and in the network-integrated
%% CT suite gated on `MACULA_DOH_ENABLE=1'.
-module(macula_bootstrap_via_doh_http).
-behaviour(macula_bootstrap_via_doh_resolver_behaviour).

-export([resolve/3]).

-define(CONTENT_TYPE,     "application/dns-message").
-define(DEFAULT_TIMEOUT,  1500).

-spec resolve(macula_bootstrap_via_doh_resolver_behaviour:url(),
              macula_identity:pubkey(),
              macula_bootstrap_via_doh_resolver_behaviour:resolve_opts()) ->
            macula_bootstrap_via_doh_resolver_behaviour:resolve_result().
resolve(Url, Pubkey, Opts) ->
    Timeout  = maps:get(timeout_ms, Opts, ?DEFAULT_TIMEOUT),
    ZoneBase = application:get_env(macula_bootstrap, doh_zone_base,
                                   <<"macula.io">>),
    CodecOpts = #{zone_base => ZoneBase, timeout_ms => Timeout},
    macula_bootstrap_via_doh_resolver:resolve(Url, Pubkey, CodecOpts,
                                 send_fun(Timeout)).

%%------------------------------------------------------------------
%% HTTP transport
%%------------------------------------------------------------------

send_fun(Timeout) ->
    fun(Url, Body) -> post(url_as_string(Url), Body, Timeout) end.

post(Url, Body, Timeout) ->
    Request  = {Url, [{"accept", ?CONTENT_TYPE}], ?CONTENT_TYPE, Body},
    HttpOpts = [{timeout, Timeout}, {connect_timeout, Timeout}],
    translate(httpc:request(post, Request, HttpOpts,
                            [{body_format, binary}])).

translate({ok, {{_Ver, 200, _Phrase}, _Headers, RespBody}}) ->
    {ok, RespBody};
translate({ok, {{_Ver, Code, Phrase}, _Headers, _Body}}) ->
    {error, {http_status, Code, iolist_to_binary(Phrase)}};
translate({error, _Reason} = E) ->
    E.

url_as_string(U) when is_binary(U) -> binary_to_list(U);
url_as_string(U) when is_list(U)   -> U.
