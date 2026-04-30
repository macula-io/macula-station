%% @doc Interface enumeration for multi-interface mDNS (Part 5 §5,
%% Phase 6.4.y).
%%
%% Lists host network interfaces that can carry mDNS traffic — UP,
%% multicast-capable, not loopback, with at least one IPv6 address
%% — and returns a normalised description for each. Consumers:
%% <ul>
%%   <li>`macula_bootstrap_mdns_udp' — probe fan-out: send one mDNS
%%       query per interface instead of relying on the kernel's
%%       default egress pick.</li>
%%   <li>`macula_bootstrap_mdns_responder_sup' (future) — spawn one
%%       responder child per interface so each NIC advertises + listens
%%       on its own link-local scope.</li>
%%   <li>Operator tooling — `/status' can enumerate which interfaces
%%       the station considers candidates.</li>
%% </ul>
%%
%% The description carries:
%% <ul>
%%   <li>`name' — OS interface label (`enp5s0', `wlan0', `br-…').</li>
%%   <li>`index' — Linux `ifindex' read from
%%       `/sys/class/net/$name/ifindex', or `undefined' on non-Linux
%%       hosts. Needed for the `IPV6_MULTICAST_IF' raw socket option.</li>
%%   <li>`link_local' — first `fe80::/10' address found on the
%%       interface, or `undefined'.</li>
%%   <li>`ipv6' — every IPv6 address on the interface, in the order
%%       the kernel reports them.</li>
%% </ul>
%%
%% The enumeration source is pluggable so unit tests can feed canned
%% `inet:getifaddrs/0' output without touching a live host. Default
%% source is `inet:getifaddrs/0'.
-module(macula_bootstrap_mdns_ifaces).

-export([list/0, list/1, eligible/1, describe/2, read_ifindex/1]).

-export_type([iface/0, source/0]).

-type iface() :: #{
    name       := string(),
    index      := non_neg_integer() | undefined,
    link_local := inet:ip6_address() | undefined,
    ipv6       := [inet:ip6_address()]
}.

-type iface_props() :: [tuple()].
-type source() :: fun(() ->
    {ok, [{string(), iface_props()}]} | {error, term()}).

%%==================================================================
%% API
%%==================================================================

%% @doc List mDNS-eligible interfaces on this host using
%% `inet:getifaddrs/0' as the source of truth.
-spec list() -> [iface()].
list() ->
    list(fun inet:getifaddrs/0).

%% @doc Variant that takes a pluggable enumeration source.
-spec list(source()) -> [iface()].
list(Source) when is_function(Source, 0) ->
    on_source(Source()).

on_source({ok, L}) ->
    [describe(Name, Props) || {Name, Props} <- L, eligible(Props)];
on_source({error, _}) ->
    [].

%% @doc Predicate: interface is eligible for mDNS traffic.
-spec eligible(iface_props()) -> boolean().
eligible(Props) ->
    has_flags([up, multicast], Props)
        andalso not has_flag(loopback, Props)
        andalso has_ipv6(Props).

%% @doc Build the interface description from `inet:getifaddrs/0'
%% output for one interface.
-spec describe(string(), iface_props()) -> iface().
describe(Name, Props) ->
    IPv6 = [A || A <- proplists:get_all_values(addr, Props), is_ipv6(A)],
    #{name       => Name,
      index      => read_ifindex(Name),
      link_local => first_link_local(IPv6),
      ipv6       => IPv6}.

%% @doc Read the Linux ifindex for an interface by name. Returns
%% `undefined' on non-Linux hosts or on read failure.
-spec read_ifindex(string()) -> non_neg_integer() | undefined.
read_ifindex(Name) when is_list(Name) ->
    Path = filename:join(["/sys/class/net", Name, "ifindex"]),
    on_ifindex_read(file:read_file(Path)).

on_ifindex_read({ok, Bin}) ->
    parse_int(string:trim(binary_to_list(Bin)));
on_ifindex_read({error, _}) ->
    undefined.

%%==================================================================
%% Internals
%%==================================================================

has_flags([],          _Props) -> true;
has_flags([F | Rest],   Props) ->
    has_flag(F, Props) andalso has_flags(Rest, Props).

has_flag(F, Props) ->
    lists:member(F, proplists:get_value(flags, Props, [])).

has_ipv6(Props) ->
    lists:any(fun is_ipv6/1, proplists:get_all_values(addr, Props)).

is_ipv6({_, _, _, _, _, _, _, _}) -> true;
is_ipv6(_)                        -> false.

%% fe80::/10 — the first 10 bits are `1111111010', i.e. the top
%% 16-bit group begins with `0xfe80..0xfebf'. Keep the test cheap by
%% masking to the top nibble (`0xfe8X..0xfeBX') and then the low bits.
is_link_local({First, _, _, _, _, _, _, _}) ->
    (First band 16#ffc0) =:= 16#fe80;
is_link_local(_) ->
    false.

first_link_local(Addrs) ->
    case [A || A <- Addrs, is_link_local(A)] of
        [LL | _] -> LL;
        []       -> undefined
    end.

parse_int(S) ->
    on_parse(string:to_integer(S)).

on_parse({N, ""}) when is_integer(N), N >= 0 -> N;
on_parse(_)                                  -> undefined.
