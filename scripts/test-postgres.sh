#!/usr/bin/env bash
# Each argument is a PostgreSQL bin directory. Only disposable clusters are used.
set -euo pipefail
cd "$(dirname "$0")/.."
export GOCACHE="${GOCACHE:-/private/tmp/pgcheck-gocache}"
if [[ $# -eq 0 ]]; then
  echo "Usage: bash scripts/test-postgres.sh /path/to/postgresql/bin [...]" >&2
  exit 2
fi
test_root=$(mktemp -d /private/tmp/pgcheck-test.XXXXXX)
running_bin=
cleanup() {
  if [[ -n "$running_bin" ]]; then
    "$running_bin/pg_ctl" -D "$test_root/data" -m immediate -w stop >/dev/null 2>&1 || true
  fi
  rm -rf "$test_root"
}
trap cleanup EXIT
go build -o "$test_root/pgcheck" .
export PGCHECK_TEST_BIN="$test_root/pgcheck"
for pg_bin in "$@"; do
  "$pg_bin/postgres" --version
  "$pg_bin/initdb" -D "$test_root/data" -A trust -U pgcheck_test --no-locale -E UTF8 > "$test_root/init.log"
  running_bin=$pg_bin
  "$pg_bin/pg_ctl" -D "$test_root/data" -l "$test_root/server.log" -w start \
    -o "-k $test_root -c listen_addresses='' -c cluster_name=pgcheck-integration -c shared_preload_libraries=pg_stat_statements -c track_io_timing=on -c autovacuum_naptime=86400 -c fsync=off -c max_parallel_workers_per_gather=0" >/dev/null
  export PGCHECK_TEST_DSN="host=$test_root port=5432 user=pgcheck_test dbname=postgres sslmode=disable"
  export PGCHECK_TEST_HOST="$test_root"
  export PGCHECK_TEST_PSQL="$pg_bin/psql"
  go test -count=1 -v ./...
  "$pg_bin/pg_ctl" -D "$test_root/data" -m fast -w stop >/dev/null
  "$pg_bin/pg_ctl" -D "$test_root/data" -l "$test_root/server.log" -w start \
    -o "-k $test_root -c listen_addresses='' -c cluster_name=pgcheck-integration -c shared_preload_libraries=''" >/dev/null
  PGCHECK_TEST_MODE=unloaded go test -count=1 -v ./internal/app -run '^TestIntegrationUnloadedExtension$'
  "$pg_bin/pg_ctl" -D "$test_root/data" -m fast -w stop >/dev/null
  running_bin=
  rm -rf "$test_root/data"
done
