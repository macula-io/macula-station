%% @doc Test helpers for the Phase 1 walking-skeleton CT suite.
-module(phase1_helper).

-export([
    generate_test_cert/1,
    free_port/0,
    wait_for/2
]).

%% @doc Generate a self-signed RSA cert + key into Dir. Returns
%% `{CertPath, KeyPath}'. Requires `openssl' on PATH (CI ubuntu has it).
-spec generate_test_cert(file:name_all()) -> {string(), string()}.
generate_test_cert(Dir) ->
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    CertPath = filename:join(Dir, "cert.pem"),
    KeyPath  = filename:join(Dir, "key.pem"),
    Cmd = lists:flatten(io_lib:format(
        "openssl req -x509 -newkey rsa:2048 -nodes "
        "-keyout ~s -out ~s -days 1 -subj /CN=localhost 2>&1",
        [KeyPath, CertPath])),
    Out = os:cmd(Cmd),
    true = filelib:is_regular(CertPath) orelse error({openssl_failed, Out}),
    true = filelib:is_regular(KeyPath)  orelse error({openssl_failed, Out}),
    {CertPath, KeyPath}.

%% @doc Bind+release an ephemeral TCP port to learn a free number.
%% Quick approximation; the kernel may reuse it concurrently.
-spec free_port() -> inet:port_number().
free_port() ->
    {ok, S} = gen_udp:open(0, [{reuseaddr, true}]),
    {ok, Port} = inet:port(S),
    ok = gen_udp:close(S),
    Port.

%% @doc Poll a predicate until it returns true or timeout (ms).
-spec wait_for(fun(() -> boolean()), pos_integer()) -> ok | timeout.
wait_for(Pred, TimeoutMs) ->
    Deadline = erlang:monotonic_time(millisecond) + TimeoutMs,
    poll(Pred, Deadline).

poll(Pred, Deadline) ->
    poll_step(Pred(), Pred, Deadline).

poll_step(true, _Pred, _Deadline) ->
    ok;
poll_step(false, Pred, Deadline) ->
    Now = erlang:monotonic_time(millisecond),
    poll_wait(Pred, Deadline, Deadline - Now).

poll_wait(_Pred, _Deadline, Remaining) when Remaining =< 0 ->
    timeout;
poll_wait(Pred, Deadline, _Remaining) ->
    timer:sleep(50),
    poll(Pred, Deadline).
