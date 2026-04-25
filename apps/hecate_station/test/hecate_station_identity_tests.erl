-module(hecate_station_identity_tests).
-include_lib("eunit/include/eunit.hrl").

%%==================================================================
%% Path convention
%%==================================================================

path_for_joins_data_dir_test() ->
    ?assertEqual("/tmp/foo/identity.erl.bin",
                 hecate_station_identity:path_for("/tmp/foo")).

%%==================================================================
%% load_or_generate/1 — the common boot idiom
%%==================================================================

cold_boot_generates_and_persists_test_() ->
    {setup, fun tmpdir/0, fun rm_rf/1, fun(Dir) ->
        fun() ->
            Path = hecate_station_identity:path_for(Dir),
            ?assertNot(filelib:is_regular(Path)),
            {ok, Kp} = hecate_station_identity:load_or_generate(Path),
            ?assert(is_map(Kp)),
            ?assertEqual(32, byte_size(hecate_identity:public(Kp))),
            ?assert(filelib:is_regular(Path))
        end
    end}.

warm_boot_returns_same_identity_test_() ->
    {setup, fun tmpdir/0, fun rm_rf/1, fun(Dir) ->
        fun() ->
            Path = hecate_station_identity:path_for(Dir),
            {ok, Kp1} = hecate_station_identity:load_or_generate(Path),
            {ok, Kp2} = hecate_station_identity:load_or_generate(Path),
            ?assertEqual(hecate_identity:public(Kp1),
                         hecate_identity:public(Kp2)),
            ?assertEqual(hecate_identity:private(Kp1),
                         hecate_identity:private(Kp2))
        end
    end}.

missing_dir_recovery_test_() ->
    {setup, fun tmpdir/0, fun rm_rf/1, fun(Dir) ->
        fun() ->
            %% Nested sub-directory that does not exist yet; the
            %% loader must create parent directories on the fly.
            Nested = filename:join([Dir, "a", "b", "c"]),
            Path   = hecate_station_identity:path_for(Nested),
            ?assertNot(filelib:is_dir(Nested)),
            {ok, _Kp} = hecate_station_identity:load_or_generate(Path),
            ?assert(filelib:is_regular(Path))
        end
    end}.

persisted_file_is_mode_0600_test_() ->
    {setup, fun tmpdir/0, fun rm_rf/1, fun(Dir) ->
        fun() ->
            Path      = hecate_station_identity:path_for(Dir),
            {ok, _Kp} = hecate_station_identity:load_or_generate(Path),
            {ok, FI}  = file:read_file_info(Path),
            Mode      = element(8, FI),
            %% Mask to permission bits only; ignore file-type bits.
            ?assertEqual(8#0600, Mode band 8#0777)
        end
    end}.

%%==================================================================
%% load/1 — pure read
%%==================================================================

load_missing_returns_enoent_test_() ->
    {setup, fun tmpdir/0, fun rm_rf/1, fun(Dir) ->
        fun() ->
            Path = hecate_station_identity:path_for(Dir),
            ?assertEqual({error, enoent},
                         hecate_station_identity:load(Path))
        end
    end}.

generate_overwrites_even_when_file_exists_test_() ->
    %% generate/1 is not load-or-generate: it always mints + persists.
    {setup, fun tmpdir/0, fun rm_rf/1, fun(Dir) ->
        fun() ->
            Path       = hecate_station_identity:path_for(Dir),
            {ok, Kp1}  = hecate_station_identity:generate(Path),
            {ok, Kp2}  = hecate_station_identity:generate(Path),
            ?assertNotEqual(hecate_identity:public(Kp1),
                            hecate_identity:public(Kp2)),
            {ok, KpR}  = hecate_station_identity:load(Path),
            ?assertEqual(hecate_identity:public(Kp2),
                         hecate_identity:public(KpR))
        end
    end}.

%%==================================================================
%% Helpers
%%==================================================================

tmpdir() ->
    Base = filename:join(["/tmp", "hecate-station-test",
                          integer_to_list(erlang:unique_integer([positive]))]),
    ok   = filelib:ensure_dir(filename:join(Base, "placeholder")),
    Base.

rm_rf(Dir) ->
    _ = os:cmd("rm -rf " ++ Dir),
    ok.
