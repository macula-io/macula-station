#!/usr/bin/env bash
# Phase 0 skeleton bootstrap. Idempotent.
# Creates apps/ layout per PLAN_MACULA_V2_PART7 §3.
set -euo pipefail

cd "$(dirname "$0")/.."
ROOT="$(pwd)"

LIB_APPS=(
  macula_identity
  macula_record
  macula_frame
  macula_transport
  macula_peering
  macula_handler
  macula_dht
  macula_swim
  macula_routing
  macula_bootstrap
  macula_overlay
  macula_realm
  macula_diagnostics
)

mk_lib_app() {
  local app="$1"
  mkdir -p "apps/${app}/src"
  cat > "apps/${app}/src/${app}.app.src" <<EOF
{application, ${app}, [
    {description, "Macula V2 — ${app} (skeleton, Phase 0)"},
    {vsn, "0.1.0"},
    {registered, []},
    {applications, [kernel, stdlib]},
    {env, []},
    {modules, []},
    {licenses, ["Apache-2.0"]},
    {links, [{"GitHub", "https://github.com/hecate-social/hecate-station"}]}
]}.
EOF
}

mk_root_app() {
  local app=hecate_station
  mkdir -p "apps/${app}/src"

  cat > "apps/${app}/src/${app}.app.src" <<EOF
{application, ${app}, [
    {description, "Hecate Station — Macula V2 reference station (skeleton, Phase 0)"},
    {vsn, "0.1.0"},
    {registered, [${app}_sup]},
    {mod, {${app}_app, []}},
    {applications, [
        kernel,
        stdlib,
        macula_identity,
        macula_record,
        macula_frame,
        macula_transport,
        macula_peering,
        macula_handler,
        macula_dht,
        macula_swim,
        macula_routing,
        macula_bootstrap,
        macula_overlay,
        macula_realm,
        macula_diagnostics
    ]},
    {env, []},
    {modules, []},
    {licenses, ["Apache-2.0"]},
    {links, [{"GitHub", "https://github.com/hecate-social/hecate-station"}]}
]}.
EOF

  cat > "apps/${app}/src/${app}_app.erl" <<'EOF'
%% @doc Application callback. Skeleton only — Phase 0.
-module(hecate_station_app).
-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    hecate_station_sup:start_link().

stop(_State) ->
    ok.
EOF

  cat > "apps/${app}/src/${app}_sup.erl" <<'EOF'
%% @doc Top-level supervisor. Skeleton only — Phase 0.
-module(hecate_station_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    {ok, {SupFlags, []}}.
EOF

  cat > "apps/${app}/src/${app}.erl" <<'EOF'
%% @doc Hecate Station facade. Skeleton only — Phase 0.
-module(hecate_station).

-export([version/0]).

-spec version() -> binary().
version() -> <<"0.1.0-phase0-skeleton">>.
EOF
}

for a in "${LIB_APPS[@]}"; do mk_lib_app "$a"; done
mk_root_app

echo "Skeleton scaffolded at ${ROOT}/apps/"
ls apps
