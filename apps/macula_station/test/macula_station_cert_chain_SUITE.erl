%% @doc CT suite — Slice 7c Direction B end-to-end across TWO stations.
%% A producer publishes its advertisement on station B, with its X.509
%% service-cert chain embedded in the `procedure_advertisement'. A consumer
%% connected to station A resolves that record cross-station over the DHT
%% (so the chain arrives wire-decoded, the shape that broke earlier
%% readers), verifies it to the realm CA it trusts, then resolves B's
%% station endpoint, dials B directly, and calls the raw capability. A
%% squatter whose chain roots in a different realm CA is rejected by the
%% same check and is never dialed.
%%
%% Topology matches macula_station_procedure_advertisement_SUITE: A is the
%% consumer, B is the producer/serving station, A dials B.
%%
%% The realm's real CAs sign with P-256 (secp256r1); the leaf binds the
%% service's Ed25519 key (= the advertiser key that signs the record), so
%% the suite mints that exact mixed-key shape rather than an all-Ed25519
%% stand-in.
-module(macula_station_cert_chain_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("public_key/include/public_key.hrl").

-export([suite/0, all/0,
         init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([verified_provider_resolves_dials_and_calls/1,
         squatter_wrong_realm_ca_dropped/1]).

-define(DHT_REALM, <<0:256>>).
-define(ORG, <<"acme">>).

suite() -> [{timetrap, {minutes, 5}}].

all() ->
    [verified_provider_resolves_dials_and_calls,
     squatter_wrong_realm_ca_dropped].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(macula),
    Config.
end_per_suite(_Config) -> ok.

init_per_testcase(_Name, Config) ->
    [{cluster_opts, #{base_dir => ?config(priv_dir, Config)}} | Config].
end_per_testcase(_Name, _Config) -> ok.

%% The full Direction B data plane across two stations: a producer's
%% advertisement lives on B; the consumer on A resolves it cross-station,
%% its embedded chain verifies to the realm CA, and the capability call
%% to B succeeds — all over the wire.
verified_provider_resolves_dials_and_calls(Config) ->
    with_pair(Config, fun(PoolA, B) ->
        BPub = macula_station_test_cluster:pubkey(B),
        Cap  = <<"_dht.find_record">>,   %% a capability B actually serves
        Uri  = disc_uri(?DHT_REALM, ?ORG, Cap),
        Kp     = macula_identity:generate(),
        AdvPub = macula_identity:public(Kp),
        #{realm_ca := RealmCa, chain := Chain} = issue_chain(AdvPub, ?ORG),

        Ad = macula_record:sign(
               macula_record:procedure_advertisement(
                 AdvPub, Uri, BPub, #{cert_chain => Chain}), Kp),
        ok = put_on(B, Ad),

        %% consumer A RESOLVES B's advertisement cross-station (wire-decoded)
        %% and VERIFIES the chain to the realm CA it trusts
        {ok, [AdRec]} = resolve(PoolA, macula_record:procedure_key(Uri)),
        ?assertEqual(ok,
                     macula_record:verify_advertisement_cert_chain(
                       RealmCa, AdRec, ?ORG)),
        #{serving_station := SS} =
            macula_record:read_procedure_advertisement(AdRec),
        ?assertEqual(BPub, SS),

        %% RESOLVE B's endpoint -> DIAL B -> CALL the raw capability
        {ok, EpRec} = resolve_one(PoolA, macula_record:station_endpoint_key(SS)),
        #{quic_port := EPort, host_advertised := [EHost | _]} =
            macula_record:read_station_endpoint(EpRec),
        DialUrl = build_url(EHost, EPort),
        ?assertMatch({ok, _},
                     macula:call_station(PoolA, DialUrl, ?DHT_REALM, Cap,
                                         #{key => <<0:256>>}, 5_000))
    end).

%% A squatter embeds a full, well-formed chain — but rooted in its OWN
%% realm CA, not the one the consumer trusts. The consumer on A resolves it
%% cross-station and the same verify call rejects it, so a `verify => true'
%% consumer drops it and never dials.
squatter_wrong_realm_ca_dropped(Config) ->
    with_pair(Config, fun(PoolA, B) ->
        BPub  = macula_station_test_cluster:pubkey(B),
        Cap   = <<"_dht.find_record">>,
        Uri   = disc_uri(?DHT_REALM, ?ORG, Cap),
        Kp    = macula_identity:generate(),
        Squat = macula_identity:public(Kp),
        %% squatter's chain roots in a realm CA the consumer does NOT trust
        #{chain := SquatChain} = issue_chain(Squat, ?ORG),
        %% the realm CA the consumer actually trusts (unrelated issuance)
        #{realm_ca := TrustedRealmCa} =
            issue_chain(macula_identity:public(macula_identity:generate()), ?ORG),

        Ad = macula_record:sign(
               macula_record:procedure_advertisement(
                 Squat, Uri, BPub, #{cert_chain => SquatChain}), Kp),
        ok = put_on(B, Ad),

        {ok, [AdRec]} = resolve(PoolA, macula_record:procedure_key(Uri)),
        ?assertMatch({error, _},
                     macula_record:verify_advertisement_cert_chain(
                       TrustedRealmCa, AdRec, ?ORG))
    end).

%%------------------------------------------------------------------
%% Cluster helper — A (consumer) dials B (producer/serving station)
%%------------------------------------------------------------------

with_pair(Config, Fun) ->
    Opts    = ?config(cluster_opts, Config),
    Handles = macula_station_test_cluster:spawn_cluster(2, Opts),
    [A, B]  = Handles,
    ok = macula_station_test_cluster:dial(A, B),
    ok = macula_station_test_cluster:wait_for_handshakes(A, 1, 10_000),
    UrlA = station_url(macula_station_test_cluster:listen_addr(A)),
    {ok, PoolA} = macula:connect([UrlA], #{verify => none}),
    try
        ok = wait_healthy(PoolA),
        Fun(PoolA, B)
    after
        catch macula:close(PoolA),
        macula_station_test_cluster:stop_cluster(Handles)
    end.

%% Producer publishes on its own station B.
put_on(Station, Record) ->
    macula_station_test_cluster:rpc(
      Station, macula_dht, put_record, [macula_dht, Record]).

%% Consumer resolves via its SDK pool, retrying while the DHT settles.
resolve(Pool, Key) -> resolve(Pool, Key, 50).
resolve(_Pool, _Key, 0) -> {ok, []};
resolve(Pool, Key, N) ->
    resolve_retry(catch macula:find_records(Pool, Key), Pool, Key, N).

resolve_retry({ok, [_ | _] = Recs}, _Pool, _Key, _N) -> {ok, Recs};
resolve_retry(_Other, Pool, Key, N) ->
    timer:sleep(100),
    resolve(Pool, Key, N - 1).

resolve_one(Pool, Key) -> resolve_one(Pool, Key, 50).
resolve_one(_Pool, _Key, 0) -> {error, not_found};
resolve_one(Pool, Key, N) ->
    resolve_one_retry(catch macula:find_record(Pool, Key), Pool, Key, N).

resolve_one_retry({ok, Rec}, _Pool, _Key, _N) -> {ok, Rec};
resolve_one_retry(_Other, Pool, Key, N) ->
    timer:sleep(100),
    resolve_one(Pool, Key, N - 1).

wait_healthy(Pool) -> wait_healthy(Pool, 100).
wait_healthy(_Pool, 0) -> {error, no_healthy_link};
wait_healthy(Pool, N) ->
    healthy_or_retry(macula:status(Pool), Pool, N).

healthy_or_retry({ok, #{healthy_links := H}}, _Pool, _N) when H >= 1 -> ok;
healthy_or_retry(_Other, Pool, N) ->
    timer:sleep(100),
    wait_healthy(Pool, N - 1).

disc_uri(Realm, Org, Name) ->
    <<(binary:encode_hex(Realm))/binary, "/", Org/binary, "/", Name/binary>>.

station_url({Ip, Port}) ->
    build_url(list_to_binary(inet:ntoa(Ip)), Port).

build_url(Host, Port) ->
    <<"quic://[", Host/binary, "]:", (integer_to_binary(Port))/binary>>.

%%------------------------------------------------------------------
%% Minimal in-process X.509 CA — realm CA (P-256) -> org CA (P-256) ->
%% Ed25519 leaf binding `LeafPub', O=`Org'. Returns the realm CA PEM and
%% the leaf-first [leaf, org CA] PEM bundle a service embeds.
%%------------------------------------------------------------------

issue_chain(LeafPub, Org) ->
    {RealmPub, RealmPriv} = p256_key(),
    {OrgPub, OrgPriv}     = p256_key(),
    RealmSubj = subject(<<"io.macula">>, <<"io.macula">>),
    OrgSubj   = subject(<<"io.macula.", Org/binary>>, Org),
    LeafSubj  = subject(<<"mri:app:io.macula/", Org/binary, "/svc">>, Org),
    RealmDer = sign_cert(RealmSubj, p256_spki(RealmPub), RealmSubj, RealmPriv, true),
    OrgDer   = sign_cert(OrgSubj, p256_spki(OrgPub), RealmSubj, RealmPriv, true),
    LeafDer  = sign_cert(LeafSubj, ed_spki(LeafPub), OrgSubj, OrgPriv, false),
    #{realm_ca => pem([RealmDer]), chain => pem([LeafDer, OrgDer])}.

p256_key() ->
    {Pub, Priv} = crypto:generate_key(ecdh, secp256r1),
    {Pub, #'ECPrivateKey'{version = 1, privateKey = Priv,
                          parameters = {namedCurve, ?'secp256r1'},
                          publicKey = Pub}}.

p256_spki(Pub) ->
    #'OTPSubjectPublicKeyInfo'{
       algorithm = #'PublicKeyAlgorithm'{algorithm = ?'id-ecPublicKey',
                                         parameters = {namedCurve, ?'secp256r1'}},
       subjectPublicKey = #'ECPoint'{point = Pub}}.

ed_spki(Pub) ->
    #'OTPSubjectPublicKeyInfo'{
       algorithm = #'PublicKeyAlgorithm'{algorithm = ?'id-Ed25519',
                                         parameters = asn1_NOVALUE},
       subjectPublicKey = #'ECPoint'{point = Pub}}.

subject(CN, O) ->
    {rdnSequence,
     [[#'AttributeTypeAndValue'{type = {2, 5, 4, 3}, value = {utf8String, CN}}],
      [#'AttributeTypeAndValue'{type = {2, 5, 4, 10}, value = {utf8String, O}}]]}.

sign_cert(Subject, Spki, IssuerSubject, IssuerKey, IsCA) ->
    TBS = #'OTPTBSCertificate'{
             version = v3,
             serialNumber = rand:uniform(1 bsl 60),
             signature = #'SignatureAlgorithm'{algorithm = ?'ecdsa-with-SHA256',
                                               parameters = asn1_NOVALUE},
             issuer = IssuerSubject,
             validity = #'Validity'{notBefore = {utcTime, "230101000000Z"},
                                    notAfter  = {utcTime, "330101000000Z"}},
             subject = Subject,
             subjectPublicKeyInfo = Spki,
             extensions = [basic_constraints(IsCA)]},
    public_key:pkix_sign(TBS, IssuerKey).

basic_constraints(IsCA) ->
    #'Extension'{extnID = ?'id-ce-basicConstraints', critical = true,
                 extnValue = #'BasicConstraints'{cA = IsCA,
                                                 pathLenConstraint = asn1_NOVALUE}}.

pem(Ders) ->
    public_key:pem_encode([{'Certificate', D, not_encrypted} || D <- Ders]).
