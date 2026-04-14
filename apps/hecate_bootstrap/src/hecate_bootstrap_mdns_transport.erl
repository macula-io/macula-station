%% @doc mDNS UDP transport behaviour.
%%
%% The pure Tier B codec (`hecate_bootstrap_mdns') and the Tier B
%% orchestrator (`hecate_bootstrap_tier_b') are transport-agnostic.
%% This behaviour abstracts the "send a query and collect responses"
%% step so:
%% <ul>
%%   <li>Production uses `hecate_bootstrap_mdns_udp' (real IPv6
%%       multicast via `gen_udp').</li>
%%   <li>Unit tests plug a deterministic fake that returns canned
%%       `{SrcAddress, PacketBin}' pairs.</li>
%% </ul>
%%
%% Implementations MUST honour `TimeoutMs' as the total budget — send
%% + collect combined. Returning an empty list is legal (no peers
%% responded) and simply causes Tier B to fall through.
-module(hecate_bootstrap_mdns_transport).

-export_type([reply/0]).

-type reply() :: {inet:ip6_address(), binary()}.

-callback query(QueryBin :: binary(),
                TimeoutMs :: pos_integer()) ->
            [reply()].
