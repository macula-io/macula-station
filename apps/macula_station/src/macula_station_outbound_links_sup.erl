%% @doc Supervisor for outbound `macula_station_outbound_link' workers.
%%
%% Two populations of children:
%%
%% <ul>
%%   <li><strong>Static</strong> — one per `outbound_peers' config entry,
%%       spawned once at `init/1' from the boot-time peer list.
%%       `restart => permanent': these are the operator's declared
%%       backbone and should always come back.</li>
%%   <li><strong>Dynamic</strong> — added at runtime via `dial/3' by
%%       `macula_station_peering_redundancy', for peers discovered from
%%       DHT knowledge rather than configured by the operator.
%%       `restart => temporary': that watchdog owns reconnect/cooldown
%%       decisions for peers it selected itself, so the supervisor must
%%       not also auto-restart them — two independent restart policies
%%       fighting over the same child would race.</li>
%% </ul>
%%
%% Started from `macula_station_app' boot pipeline AFTER the listener
%% is up and BEFORE the bootstrap cascade runs. The cascade's
%% `seed_dial' tier reads the resulting peer-set from
%% `macula_station_peer_links'.
%%
%% Restart strategy: `one_for_one' so one peer's persistent dial-failures
%% don't take down the others. Each child's intensity tolerates rapid
%% reconnect cycles in the link's own backoff loop.
-module(macula_station_outbound_links_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).
-export([dial/3, undial/1]).

-export_type([opts/0, peer/0]).

-type peer() :: #{host := binary(), port := inet:port_number()}.

-type opts() :: #{
    identity     := macula_identity:key_pair(),
    capabilities => non_neg_integer(),
    peers        := [peer()]
}.

-spec start_link(opts()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Opts).

init(#{identity := Kp, peers := Peers} = Opts) ->
    Caps     = maps:get(capabilities, Opts, 0),
    SupFlags = #{strategy  => one_for_one,
                 intensity => 10,
                 period    => 60},
    Children = [link_child(Peer, Kp, Caps, permanent) || Peer <- Peers],
    {ok, {SupFlags, Children}}.

%% @doc Dynamically dial a peer discovered outside the static
%% `outbound_peers' config (e.g. by `macula_station_peering_redundancy').
%% Idempotent against a peer already dialled — `already_present'/
%% `already_started' both collapse to the existing child's pid rather
%% than erroring, since the caller only cares "is there now a link for
%% this peer", not who created it.
-spec dial(peer(), macula_identity:key_pair(), non_neg_integer()) ->
    {ok, pid()} | {error, term()}.
dial(#{host := Host, port := Port} = Peer, Kp, Caps) ->
    Spec = link_child(Peer, Kp, Caps, temporary),
    on_start_child(supervisor:start_child(?MODULE, Spec), Host, Port).

on_start_child({ok, Pid}, _Host, _Port) -> {ok, Pid};
on_start_child({error, {already_started, Pid}}, _Host, _Port) -> {ok, Pid};
on_start_child({error, already_present}, Host, Port) ->
    %% Child spec exists but isn't running (e.g. a `temporary' child
    %% that already exited) -- drop the stale spec and retry once.
    Url = peer_url(Host, Port),
    _ = supervisor:delete_child(?MODULE, {link, Url}),
    {error, already_present};
on_start_child({error, _} = E, _Host, _Port) -> E.

%% @doc Tear down a dynamically-dialled peer. No-op if it was never
%% dialled or has already exited and been reaped. Only meant for
%% `temporary' (dynamic) children -- calling this on a static
%% `outbound_peers' entry would silence a configured backbone peer,
%% which is an operator decision, not this module's to make.
-spec undial(peer()) -> ok.
undial(#{host := Host, port := Port}) ->
    Id = {link, peer_url(Host, Port)},
    _ = supervisor:terminate_child(?MODULE, Id),
    _ = supervisor:delete_child(?MODULE, Id),
    ok.

link_child(#{host := Host, port := Port}, Kp, Caps, Restart) ->
    Url = peer_url(Host, Port),
    Opts = #{url => Url, identity => Kp, capabilities => Caps},
    #{
        id       => {link, Url},
        start    => {macula_station_outbound_link, start_link, [Opts]},
        restart  => Restart,
        shutdown => 5_000,
        type     => worker,
        modules  => [macula_station_outbound_link]
    }.

%% ⚠ AN IPv6 LITERAL MUST BE BRACKETED HERE TOO. Without it this built
%% `quic://::1:5000', which `macula_station_outbound_link:parse_url/1' cannot
%% split -- the port delimiter is ambiguous when the host is full of colons.
%% This is the construction half of the same defect; fixing only the parser
%% would leave every URL this function emits for an IPv6 peer unparseable.
peer_url(Host, Port) when is_binary(Host) ->
    iolist_to_binary(["quic://", bracket_if_ipv6(Host), ":",
                      integer_to_binary(Port)]);
peer_url(Host, Port) when is_list(Host) ->
    peer_url(list_to_binary(Host), Port).

%% A colon in a host can only be IPv6: DNS names and IPv4 literals have none.
bracket_if_ipv6(Host) ->
    wrap(binary:match(Host, <<":">>) =/= nomatch, Host).

wrap(true,  Host) -> [$[, Host, $]];
wrap(false, Host) -> Host.
