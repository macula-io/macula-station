%% @doc Mainline DHT transport behaviour (Tier C, Part 5 §6).
%%
%% Hides the BEP 5 Kademlia-over-UDP particulars from Tier C's
%% orchestration. Production will plug in a real Mainline DHT
%% client (either a minimal Erlang implementation or a Rust NIF
%% wrapping `mainline-dht-go' / `libtorrent'); unit tests plug in a
%% canned in-memory fake.
%%
%% The single callback fetches a BEP 44 mutable item at the supplied
%% `target_id' (SHA-1 of foundation pubkey per `hecate_bootstrap_bep44').
%% Corroboration across DHT nodes is the transport's concern —
%% Tier C treats the returned item as a single signed blob to verify.
-module(hecate_bootstrap_dht_transport).

-export_type([target_id/0, get_result/0]).

-type target_id()  :: <<_:160>>.

-type get_result() ::
        {ok, hecate_bootstrap_bep44:item()}
      | {error, term()}.

-callback get_mutable(target_id(), TimeoutMs :: pos_integer()) ->
            get_result().
