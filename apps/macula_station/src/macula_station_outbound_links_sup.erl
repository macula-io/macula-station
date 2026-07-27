%% @doc Supervisor for outbound `macula_station_outbound_link' workers — one
%% per configured peer URL.
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

-export_type([opts/0]).

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
    Children = [link_child(Peer, Kp, Caps) || Peer <- Peers],
    {ok, {SupFlags, Children}}.

link_child(#{host := Host, port := Port}, Kp, Caps) ->
    Url = peer_url(Host, Port),
    Opts = #{url => Url, identity => Kp, capabilities => Caps},
    #{
        id       => {link, Url},
        start    => {macula_station_outbound_link, start_link, [Opts]},
        restart  => permanent,
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
