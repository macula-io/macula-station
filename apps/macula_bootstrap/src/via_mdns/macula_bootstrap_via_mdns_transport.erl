%% @doc mDNS UDP transport behaviour.
%%
%% The pure Tier B codec (`macula_bootstrap_via_mdns_query') and the Tier B
%% orchestrator (`macula_bootstrap_via_mdns') are transport-agnostic.
%% This behaviour abstracts the "send a query and collect responses"
%% step so:
%% <ul>
%%   <li>Production uses `macula_bootstrap_via_mdns_udp' (real IPv6
%%       multicast via `gen_udp').</li>
%%   <li>Unit tests plug a deterministic fake that returns canned
%%       `{SrcAddress, PacketBin}' pairs.</li>
%% </ul>
%%
%% Implementations MUST honour `TimeoutMs' as the total budget — send
%% + collect combined. Returning an empty list is legal (no peers
%% responded) and simply causes Tier B to fall through.
-module(macula_bootstrap_via_mdns_transport).

-export_type([reply/0]).

-type reply() :: {inet:ip6_address(), binary()}.

-callback query(QueryBin :: binary(),
                TimeoutMs :: pos_integer()) ->
            [reply()].
