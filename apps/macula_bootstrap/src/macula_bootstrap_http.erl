%% @doc HTTP transport behaviour for chain adapters.
%%
%% Both concrete chain adapters (`macula_bootstrap_via_blockchain_eth_jsonrpc'
%% and `macula_bootstrap_via_blockchain_esplora') reach out over HTTP. Sharing
%% a single transport abstraction lets:
%% <ul>
%%   <li>Production use `macula_bootstrap_http_httpc' (OTP `inets').</li>
%%   <li>Unit tests plug in a canned module that returns pre-baked
%%       responses — no network I/O, deterministic assertions.</li>
%% </ul>
%%
%% Adapters call back into this behaviour via the module supplied in
%% their probe options (`http' key); missing option means the default
%% httpc-backed implementation.
-module(macula_bootstrap_http).

-export_type([url/0, get_result/0, post_result/0]).

-type url() :: binary() | string().

-type get_result()  :: {ok, binary()} | {error, term()}.
-type post_result() :: {ok, binary()} | {error, term()}.

-callback get(url(), TimeoutMs :: pos_integer()) -> get_result().

-callback post_json(url(), Body :: binary(), TimeoutMs :: pos_integer()) ->
              post_result().
