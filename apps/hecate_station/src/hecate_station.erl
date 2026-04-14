%% @doc Hecate Station — public API facade.
%%
%% Phase 1: a station is a `hecate_station_server' gen_server linked to
%% the caller. Multiple stations can run in one BEAM VM (used by the
%% walking-skeleton CT suite).
%%
%% Production deployment will wire a single station via the application's
%% supervision tree from `sys.config'; that path lands in Phase 8.
-module(hecate_station).

-export([
    start_link/1,
    stop/1, stop/2,
    identity/1,
    listen_addr/1,
    connect_to/2,
    peers/1,
    tombstones/1,
    swim_members/1,
    version/0
]).

-spec start_link(hecate_station_config:opts()) -> {ok, pid()} | {error, term()}.
start_link(Spec) ->
    hecate_station_server:start_link(Spec).

-spec stop(pid()) -> {ok, [macula_record:record()]}.
stop(Pid) ->
    hecate_station_server:stop(Pid).

-spec stop(pid(), atom()) -> {ok, [macula_record:record()]}.
stop(Pid, Reason) ->
    hecate_station_server:stop(Pid, Reason).

-spec identity(pid()) -> macula_identity:key_pair().
identity(Pid) ->
    hecate_station_server:identity(Pid).

-spec listen_addr(pid()) -> {inet:ip_address() | string(), inet:port_number()}.
listen_addr(Pid) ->
    hecate_station_server:listen_addr(Pid).

-spec connect_to(pid(), hecate_station_server:connect_target()) ->
    {ok, pid()} | {error, term()}.
connect_to(Pid, Target) ->
    hecate_station_server:connect_to(Pid, Target).

-spec peers(pid()) -> [{pid(), map()}].
peers(Pid) ->
    hecate_station_server:peers(Pid).

-spec tombstones(pid()) -> [macula_record:record()].
tombstones(Pid) ->
    hecate_station_server:tombstones(Pid).

-spec swim_members(pid()) -> [hecate_swim:member()].
swim_members(Pid) ->
    hecate_station_server:swim_members(Pid).

-spec version() -> binary().
version() ->
    <<"0.1.0-phase1">>.
