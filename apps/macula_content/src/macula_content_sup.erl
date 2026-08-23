%% @doc Supervisor for the content subsystem.
%%
%% Children:
%% <ul>
%%   <li>`macula_content_store' — gen_server backing local content
%%       blocks and manifests via filesystem + ETS index.</li>
%%   <li>`macula_content_bitswap' — gen_server tracking outbound
%%       requests and dispatching inbound WANT / HAVE / BLOCK /
%%       MANIFEST_REQ / MANIFEST_RES / CANCEL frames.</li>
%% </ul>
%%
%% Strategy is `one_for_one'. Both children are `permanent' — losing
%% either should not cascade-restart the other. Store options come
%% from the application env (`store_path' or default `/var/lib/...').
-module(macula_content_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy  => one_for_one,
                 intensity => 5,
                 period    => 10},
    StoreOpts = store_opts(),
    Children = [
        #{id       => macula_content_store,
          start    => {macula_content_store, start_link, [StoreOpts]},
          restart  => permanent,
          shutdown => 5000,
          type     => worker,
          modules  => [macula_content_store]},
        #{id       => macula_content_bitswap,
          start    => {macula_content_bitswap, start_link, []},
          restart  => permanent,
          shutdown => 5000,
          type     => worker,
          modules  => [macula_content_bitswap]}
    ],
    {ok, {SupFlags, Children}}.

store_opts() ->
    case application:get_env(macula_content, store_path) of
        {ok, Path} -> #{store_path => Path};
        undefined  -> #{}
    end.
