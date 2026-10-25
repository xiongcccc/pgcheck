package app

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func integrationDB(t *testing.T) (*sql.DB, int) {
	t.Helper()
	dsn := os.Getenv("PGCHECK_TEST_DSN")
	if dsn == "" {
		t.Skip("run scripts/test-postgres.sh for disposable PostgreSQL integration tests")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	var cluster, user string
	var version int
	if err := db.QueryRow("SELECT current_setting('cluster_name'), current_user, current_setting('server_version_num')::int").Scan(&cluster, &user, &version); err != nil {
		t.Fatal(err)
	}
	if cluster != "pgcheck-integration" || user != "pgcheck_test" {
		t.Fatal("integration tests require the disposable cluster created by scripts/test-postgres.sh")
	}
	return db, version / 10000
}

func execSQL(t *testing.T, db *sql.DB, query string) {
	t.Helper()
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("SQL failed: %v\n%s", err, query)
	}
}

func fixtureSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	execSQL(t, db, "CREATE SCHEMA pgcheck_fixture; SET search_path = pgcheck_fixture, pg_catalog")
	t.Cleanup(func() {
		execSQL(t, db, "SET search_path = public; DROP SCHEMA pgcheck_fixture CASCADE")
	})
}

func assetSQL(t *testing.T, file string) string {
	t.Helper()
	a := New(os.DirFS("../.."), BuildInfo{})
	query, err := a.stepSQL(queryStep{File: file})
	if err != nil {
		t.Fatal(err)
	}
	return query
}

func resultRows(t *testing.T, db *sql.DB, query string) []map[string]string {
	t.Helper()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("query failed: %v\n%s", err, query)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	var result []map[string]string
	for rows.Next() {
		values := make([]sql.NullString, len(cols))
		dest := make([]any, len(cols))
		for i := range values {
			dest[i] = &values[i]
		}
		if err := rows.Scan(dest...); err != nil {
			t.Fatal(err)
		}
		row := make(map[string]string)
		for i, col := range cols {
			if values[i].Valid {
				row[col] = values[i].String
			} else {
				row[col] = "<NULL>"
			}
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return result
}

func expectCell(t *testing.T, row map[string]string, key, want string) {
	t.Helper()
	if got := row[key]; got != want {
		t.Fatalf("%s = %q, want %q; row=%v", key, got, want, row)
	}
}

func TestIntegrationStatementArithmetic(t *testing.T) {
	db, _ := integrationDB(t)
	fixtureSchema(t, db)
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy=%v", legacy), func(t *testing.T) {
			timeCols := "total_exec_time, mean_exec_time"
			if legacy {
				timeCols = "total_time, mean_time"
			}
			execSQL(t, db, fmt.Sprintf(`CREATE VIEW statements AS
                SELECT 10::oid AS userid,
                    CASE WHEN id = 99 THEN 0::oid ELSE (SELECT oid FROM pg_database WHERE datname = current_database()) END AS dbid,
                    id::bigint AS queryid, calls::bigint, affected::bigint AS rows,
                    elapsed::double precision AS %s, elapsed::double precision / nullif(calls, 0) AS %s,
                    2::bigint AS temp_blks_read, 3::bigint AS temp_blks_written,
                    1.25::double precision AS temp_blk_read_time, 2.5::double precision AS temp_blk_write_time,
                    CASE WHEN id = 10 THEN E'select\n1' ELSE NULL END AS query,
                    id <> 77 AS toplevel
                FROM (VALUES (10,2,6,100),(11,2,8,300),(12,0,0,1000),(99,2,1,9000),(77,2,1,9000)) AS v(id,calls,affected,elapsed)`,
				strings.Split(timeCols, ", ")[0], strings.Split(timeCols, ", ")[1]))
			query := strings.ReplaceAll(assetSQL(t, "query_hotspot.sql"), "{{pg_stat_statements}}", "pgcheck_fixture.statements")
			rows := resultRows(t, db, query)
			if len(rows) != 2 {
				t.Fatalf("expected current-database, top-level, nonzero-call rows only: %v", rows)
			}
			expectCell(t, rows[0], "queryid", "11")
			expectCell(t, rows[0], "exec_time_share_pct", "75.00")
			expectCell(t, rows[0], "rows_per_call", "4.00")
			expectCell(t, rows[0], "stats_since", "<NULL>")
			expectCell(t, rows[1], "query", "select 1")
			query = strings.ReplaceAll(assetSQL(t, "query_temp.sql"), "{{pg_stat_statements}}", "pgcheck_fixture.statements")
			rows = resultRows(t, db, query)
			expectCell(t, rows[0], "temp_read", "16 kB")
			expectCell(t, rows[0], "temp_written", "24 kB")
			expectCell(t, rows[0], "temp_io_per_call", "20 kB")
			expectCell(t, rows[0], "temp_io_ms", "3.75")
			execSQL(t, db, "SET track_io_timing = off")
			expectCell(t, resultRows(t, db, query)[0], "temp_io_ms", "<NULL>")
			execSQL(t, db, "SET track_io_timing = on; DROP VIEW statements")
		})
	}
	execSQL(t, db, `CREATE VIEW statements AS SELECT
        10::oid AS userid, (SELECT oid FROM pg_database WHERE datname=current_database()) AS dbid,
        n::bigint AS queryid, 1::bigint AS calls, 0::bigint AS rows,
        n::float8 AS total_exec_time, n::float8 AS mean_exec_time,
        9223372036854775807::bigint AS temp_blks_read,
        9223372036854775807::bigint AS temp_blks_written, 'select 1'::text AS query
        FROM generate_series(1,25) n`)
	for _, file := range []string{"query_hotspot.sql", "query_temp.sql"} {
		query := strings.ReplaceAll(assetSQL(t, file), "{{pg_stat_statements}}", "pgcheck_fixture.statements")
		rows := resultRows(t, db, query)
		if len(rows) != 10 {
			t.Fatalf("%s should return top 10, got %d", file, len(rows))
		}
		if file == "query_hotspot.sql" {
			expectCell(t, rows[0], "queryid", "25")
		}
	}
}

func TestIntegrationTableHotspots(t *testing.T) {
	db, _ := integrationDB(t)
	fixtureSchema(t, db)
	execSQL(t, db, `CREATE TABLE same_name (n int) WITH (fillfactor=70);
        CREATE TABLE other (n int);
        CREATE VIEW pg_statio_user_tables AS
        SELECT 'same_name'::regclass::oid AS relid, 'pgcheck_fixture'::name AS schemaname, 'same_name'::name AS relname,
            1::bigint AS heap_blks_read, NULL::bigint AS idx_blks_read,
            2::bigint AS toast_blks_read, 3::bigint AS tidx_blks_read,
            6::bigint AS heap_blks_hit, NULL::bigint AS idx_blks_hit,
            0::bigint AS toast_blks_hit, 0::bigint AS tidx_blks_hit;
        CREATE VIEW pg_stat_user_tables AS
        SELECT 'same_name'::regclass::oid AS relid, 'pgcheck_fixture'::name AS schemaname, 'same_name'::name AS relname,
            0::bigint AS n_tup_ins, 10::bigint AS n_tup_upd, 0::bigint AS n_tup_del, 9::bigint AS n_tup_hot_upd`)
	rows := resultRows(t, db, assetSQL(t, "table_hotspot.sql"))
	if len(rows) != 1 {
		t.Fatalf("unexpected read rows: %v", rows)
	}
	expectCell(t, rows[0], "bytes_read", "48 kB")
	expectCell(t, rows[0], "cache_hit_pct", "50.00")
	expectCell(t, rows[0], "read_share_pct", "100.00")
	rows = resultRows(t, db, assetSQL(t, "table_updates.sql"))
	expectCell(t, rows[0], "hot_update_pct", "90.00")
	expectCell(t, rows[0], "fillfactor", "70")
	expectCell(t, rows[0], "newpage_updates", "<NULL>")
}

func TestIntegrationVacuumThresholds(t *testing.T) {
	db, major := integrationDB(t)
	fixtureSchema(t, db)
	execSQL(t, db, `CREATE TABLE threshold_test (n int) WITH
        (autovacuum_enabled=false, autovacuum_vacuum_threshold=7, autovacuum_vacuum_scale_factor=0.25);
        INSERT INTO threshold_test SELECT generate_series(1,100);
        ANALYZE threshold_test;
        CREATE VIEW pg_stat_user_tables AS SELECT
            'threshold_test'::regclass::oid AS relid, 'pgcheck_fixture'::name AS schemaname, 'threshold_test'::name AS relname,
            33::bigint AS n_dead_tup, 100::bigint AS n_live_tup,
            0::bigint AS n_tup_del, 33::bigint AS n_tup_upd, 0::bigint AS autovacuum_count,
            NULL::timestamptz AS last_vacuum, NULL::timestamptz AS last_autovacuum;
        CREATE VIEW pg_stat_all_tables AS SELECT * FROM pg_stat_user_tables`)
	query := assetSQL(t, "vacuum_need.sql")
	rows := resultRows(t, db, query)
	expectCell(t, rows[0], "vacuum_threshold", "32.00")
	expectCell(t, rows[0], "is_vacuum", "true")
	expectCell(t, rows[0], "autovacuum_enabled", "false")
	if major >= 12 {
		queue := resultRows(t, db, assetSQL(t, "vacuum_queue.sql"))
		expectCell(t, queue[0], "vacuum_threshold", rows[0]["vacuum_threshold"])
		expectCell(t, queue[0], "status", "autovacuum disabled")
		expectCell(t, queue[0], "reltuples", "100")
		expectCell(t, queue[0], "effective_settings", "vt: 7, vsf: 0.25, DISABLED")
		expectCell(t, queue[0], "index_count", "<NULL>")
	}
	if major >= 18 {
		execSQL(t, db, "ALTER TABLE threshold_test SET (autovacuum_vacuum_max_threshold=10)")
		expectCell(t, resultRows(t, db, query)[0], "vacuum_threshold", "10")
		execSQL(t, db, "ALTER TABLE threshold_test SET (autovacuum_vacuum_max_threshold=-1)")
		expectCell(t, resultRows(t, db, query)[0], "vacuum_threshold", "32.00")
	}
	if major >= 13 {
		execSQL(t, db, `ALTER TABLE threshold_test SET (autovacuum_vacuum_insert_threshold=5, autovacuum_vacuum_insert_scale_factor=0);
            CREATE OR REPLACE VIEW pg_stat_user_tables AS SELECT
                'threshold_test'::regclass::oid AS relid, 'pgcheck_fixture'::name AS schemaname, 'threshold_test'::name AS relname,
                0::bigint AS n_dead_tup, 100::bigint AS n_live_tup,
                0::bigint AS n_tup_del, 0::bigint AS n_tup_upd, 0::bigint AS autovacuum_count,
                NULL::timestamptz AS last_vacuum, NULL::timestamptz AS last_autovacuum,
                6::bigint AS n_ins_since_vacuum;
            CREATE OR REPLACE VIEW pg_stat_all_tables AS SELECT * FROM pg_stat_user_tables`)
		rows = resultRows(t, db, query)
		expectCell(t, rows[0], "is_vacuum", "true")
		if len(resultRows(t, db, assetSQL(t, "vacuum_queue.sql"))) != 1 {
			t.Fatal("insert-only candidate missing from vacuum_queue")
		}
		execSQL(t, db, "ALTER TABLE threshold_test SET (autovacuum_vacuum_insert_threshold=-1)")
		expectCell(t, resultRows(t, db, query)[0], "is_vacuum", "false")
	}
}

func TestIntegrationIONulls(t *testing.T) {
	db, _ := integrationDB(t)
	fixtureSchema(t, db)
	for _, modern := range []bool{false, true} {
		bytesColumn := "8192::bigint AS op_bytes"
		if modern {
			bytesColumn = "NULL::numeric AS read_bytes, 32768::numeric AS write_bytes, NULL::numeric AS extend_bytes"
		}
		execSQL(t, db, `CREATE VIEW pg_stat_io AS SELECT
            'background writer'::text AS backend_type, 'relation'::text AS object, 'normal'::text AS context,
            NULL::bigint AS reads, 2::bigint AS writes, NULL::bigint AS extends,
            NULL::float8 AS read_time, 12.5::float8 AS write_time, NULL::float8 AS extend_time,
            0::bigint AS writebacks, 0::float8 AS writeback_time, 0::bigint AS hits,
            50::bigint AS evictions, 0::bigint AS reuses, NULL::bigint AS fsyncs, NULL::float8 AS fsync_time,
            now() AS stats_reset, `+bytesColumn)
		rows := resultRows(t, db, assetSQL(t, "io_hotspot.sql"))
		expectCell(t, rows[0], "total_ops", "2")
		expectCell(t, rows[0], "total_time", "12.5")
		wantBytes := "16 kB"
		if modern {
			wantBytes = "32 kB"
		}
		expectCell(t, rows[0], "io_bytes", wantBytes)
		expectCell(t, resultRows(t, db, assetSQL(t, "io.sql"))[0], "io_bytes", wantBytes)
		execSQL(t, db, "DROP VIEW pg_stat_io")
	}
}

func TestIntegrationToastSettings(t *testing.T) {
	db, _ := integrationDB(t)
	fixtureSchema(t, db)
	execSQL(t, db, `CREATE TABLE with_toast (payload text) WITH
        (autovacuum_enabled=false, autovacuum_vacuum_threshold=7, autovacuum_vacuum_scale_factor=0)`)
	query := assetSQL(t, "vacuum_settings.sql") + ` SELECT vacuum_threshold, autovacuum_enabled FROM vacuum_settings
        WHERE oid = (SELECT reltoastrelid FROM pg_class WHERE oid='with_toast'::regclass)`
	rows := resultRows(t, db, query)
	if len(rows) != 1 {
		t.Fatal("TOAST settings missing")
	}
	expectCell(t, rows[0], "vacuum_threshold", "7")
	expectCell(t, rows[0], "autovacuum_enabled", "false")
	execSQL(t, db, `ALTER TABLE with_toast SET
        (toast.autovacuum_vacuum_threshold=11, toast.autovacuum_vacuum_scale_factor=0)`)
	rows = resultRows(t, db, query)
	expectCell(t, rows[0], "vacuum_threshold", "11")
}

func TestIntegrationCheckpointWindow(t *testing.T) {
	db, major := integrationDB(t)
	fixtureSchema(t, db)
	execSQL(t, db, `CREATE VIEW pg_stat_bgwriter AS SELECT
        0::bigint AS checkpoints_timed, 0::bigint AS checkpoints_req,
        0::bigint AS buffers_clean, 0::bigint AS buffers_checkpoint, 0::bigint AS buffers_backend,
        0::bigint AS buffers_backend_fsync, 0::bigint AS maxwritten_clean, 0::bigint AS buffers_alloc,
        now() - interval '1 hour' AS stats_reset`)
	file := "bgworker_checkpoint_state.sql"
	if major >= 17 {
		execSQL(t, db, `CREATE VIEW pg_stat_checkpointer AS SELECT
            0::bigint AS num_timed, 0::bigint AS num_requested, 0::bigint AS buffers_written,
            0::float8 AS write_time, 0::float8 AS sync_time,
            now() - interval '1 hour' AS stats_reset`)
		file = "bgworker_checkpoint_state_17.sql"
	}
	rows := resultRows(t, db, assetSQL(t, file))
	expectCell(t, rows[0], "checkpoint_write_avg", "<NULL>")
	expectCell(t, rows[0], "minutes_between_checkpoints", "<NULL>")
	if major >= 17 {
		execSQL(t, db, `CREATE OR REPLACE VIEW pg_stat_checkpointer AS SELECT
            1::bigint AS num_timed, 1::bigint AS num_requested, 2::bigint AS buffers_written,
            0::float8 AS write_time, 0::float8 AS sync_time,
            now() - interval '1 hour' AS stats_reset, 1::bigint AS num_done`)
	} else {
		execSQL(t, db, `CREATE OR REPLACE VIEW pg_stat_bgwriter AS SELECT
            1::bigint AS checkpoints_timed, 0::bigint AS checkpoints_req,
            0::bigint AS buffers_clean, 2::bigint AS buffers_checkpoint, 0::bigint AS buffers_backend,
            0::bigint AS buffers_backend_fsync, 0::bigint AS maxwritten_clean, 0::bigint AS buffers_alloc,
            now() - interval '1 hour' AS stats_reset`)
	}
	rows = resultRows(t, db, assetSQL(t, file))
	expectCell(t, rows[0], "checkpoint_write_avg", "16 kB")
	minutes, err := strconv.ParseFloat(rows[0]["minutes_between_checkpoints"], 64)
	if err != nil || minutes < 59.9 || minutes > 60.1 {
		t.Fatalf("wrong reset window: %v", rows[0])
	}
}

func cliRun(t *testing.T, cfgPath string, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Getenv("PGCHECK_TEST_BIN"), append([]string{"--config", cfgPath}, args...)...)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	err := cmd.Run()
	return output.String(), err
}

func cliConfig(t *testing.T, native bool, username string, extra []string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pgcheck.json")
	psql := os.Getenv("PGCHECK_TEST_PSQL")
	if native {
		psql = "pgcheck-intentionally-missing-psql"
	}
	data, err := json.Marshal(map[string]any{
		"connection": map[string]string{"host": os.Getenv("PGCHECK_TEST_HOST"), "port": "5432", "user": username, "database": "postgres", "sslmode": "disable"},
		"psql":       map[string]any{"path": psql, "extra_args": extra},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestIntegrationCLI(t *testing.T) {
	db, major := integrationDB(t)
	// This unusual schema also exercises identifier quoting and extension discovery.
	execSQL(t, db, `CREATE SCHEMA "stats space"; CREATE EXTENSION pg_stat_statements SCHEMA "stats space";
        CREATE ROLE pgcheck_reader LOGIN; CREATE ROLE pgcheck_monitor LOGIN;
        GRANT pg_monitor TO pgcheck_monitor;
        GRANT USAGE ON SCHEMA "stats space" TO pgcheck_reader, pgcheck_monitor;
        CREATE TABLE public.pgcheck_workload (id int, payload text) WITH (fillfactor=70);
        INSERT INTO public.pgcheck_workload SELECT n, repeat(md5(n::text),100) FROM generate_series(1,10000) n;
        ANALYZE public.pgcheck_workload;
        SET work_mem = '64kB'; SELECT count(*) FROM (SELECT * FROM public.pgcheck_workload ORDER BY payload) s;
        UPDATE public.pgcheck_workload SET payload = payload || 'a' WHERE id < 500`)
	t.Cleanup(func() {
		execSQL(t, db, `DROP EXTENSION pg_stat_statements; DROP SCHEMA "stats space";
            DROP TABLE public.pgcheck_workload; DROP OWNED BY pgcheck_reader, pgcheck_monitor;
            DROP ROLE pgcheck_reader, pgcheck_monitor`)
	})
	// Closing the workload connection publishes cumulative counters on old versions too.
	execSQL(t, db, "RESET work_mem")
	var tempBlocks int64
	if err := db.QueryRow(`SELECT coalesce(sum(temp_blks_written),0)::bigint
        FROM "stats space".pg_stat_statements WHERE query LIKE '%ORDER BY payload%'`).Scan(&tempBlocks); err != nil || tempBlocks == 0 {
		t.Fatalf("sort workload did not generate temporary I/O: blocks=%d err=%v", tempBlocks, err)
	}
	for _, native := range []bool{false, true} {
		for _, user := range []string{"pgcheck_test", "pgcheck_monitor", "pgcheck_reader"} {
			t.Run(fmt.Sprintf("native=%v/user=%s", native, user), func(t *testing.T) {
				cfg := cliConfig(t, native, user, nil)
				for _, command := range []string{"query_hotspot", "query_temp", "table_hotspot", "vacuum_needed", "dbstatus", "checkpoint", "privilege", "wraparound_risk", "xmin_blockers", "replication_slots", "connections", "lock_tree", "wait_event", "analyze_needed"} {
					args := []string{command}
					if commandMap()[command].Database {
						args = append(args, "postgres")
					}
					out, err := cliRun(t, cfg, args...)
					if err != nil {
						t.Fatalf("%s: %v\n%s", command, err, out)
					}
					if strings.Contains(out, "{{") {
						t.Fatalf("unresolved SQL template: %s", out)
					}
					if command == "query_temp" && !strings.Contains(out, "pgcheck_workload") && user != "pgcheck_reader" {
						t.Fatalf("real spill query missing from result: %s", out)
					}
				}
				for _, command := range []string{"io", "io_hotspot", "vacuum_queue"} {
					args := []string{command}
					if command == "vacuum_queue" {
						args = append(args, "postgres")
					}
					out, err := cliRun(t, cfg, args...)
					if major < commandMap()[command].MinVersion {
						if err == nil || !strings.Contains(out, "PostgreSQL") {
							t.Fatalf("missing minimum-version explanation: %s", out)
						}
					} else if err != nil {
						t.Fatalf("%s: %v\n%s", command, err, out)
					}
				}
				for _, command := range []string{"query_hotspot", "query_temp", "table_hotspot"} {
					for _, flag := range []string{"-x", "-A", "-t", "-At", "-xAt"} {
						if out, err := cliRun(t, cfg, flag, command, "postgres"); err != nil {
							t.Fatalf("%s %s: %v\n%s", flag, command, err, out)
						}
					}
					out, err := cliRun(t, cfg, command, "postgres", "--show-sql")
					if err != nil || !strings.Contains(out, "-- Adapted from fabiotr/pg_scripts") || strings.Contains(out, "{{") {
						t.Fatalf("SQL trace: %v\n%s", err, out)
					}
				}
			})
		}
	}
	for _, native := range []bool{false, true} {
		cfg := cliConfig(t, native, "pgcheck_test", nil)
		data, err := os.ReadFile(cfg)
		if err != nil {
			t.Fatal(err)
		}
		// An explicit database must override an unavailable default on both backends.
		var config map[string]any
		if err := json.Unmarshal(data, &config); err != nil {
			t.Fatal(err)
		}
		config["connection"].(map[string]any)["database"] = "pgcheck_no_such_database"
		data, err = json.Marshal(config)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(cfg, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if out, err := cliRun(t, cfg, "query_hotspot", "postgres"); err != nil {
			t.Fatalf("explicit database override: %v\n%s", err, out)
		}
		cfg = cliConfig(t, native, "pgcheck_test", nil)
		out, err := cliRun(t, cfg, "quick", "postgres")
		if err != nil || !strings.Contains(out, "== vacuum queue ==") || strings.Contains(out, "{{") {
			t.Fatalf("quick regression: %v\n%s", err, out)
		}
		if major >= 12 && strings.Contains(out, "warning:") {
			t.Fatalf("unexpected quick warning: %s", out)
		}
	}
	execSQL(t, db, `REVOKE USAGE ON SCHEMA "stats space" FROM pgcheck_reader`)
	for _, native := range []bool{false, true} {
		out, err := cliRun(t, cliConfig(t, native, "pgcheck_reader", nil), "query_hotspot", "postgres")
		if err == nil || !strings.Contains(strings.ToLower(out), "permission denied") {
			t.Fatalf("schema permission diagnostic: %v\n%s", err, out)
		}
	}
	execSQL(t, db, `GRANT USAGE ON SCHEMA "stats space" TO pgcheck_reader;
        ALTER ROLE pgcheck_reader SET pg_stat_statements.track = 'none'`)
	for _, native := range []bool{false, true} {
		out, err := cliRun(t, cliConfig(t, native, "pgcheck_reader", nil), "query_hotspot", "postgres")
		if err == nil || !strings.Contains(out, "collection is disabled") {
			t.Fatalf("disabled collection diagnostic: %v\n%s", err, out)
		}
	}
	execSQL(t, db, "ALTER ROLE pgcheck_reader RESET pg_stat_statements.track")
	// Internal metadata must remain parseable even when users request CSV/expanded output.
	for _, extra := range [][]string{{"--csv"}, {"-x"}, {"-P", "fieldsep=;"}} {
		if major < 12 && extra[0] == "--csv" {
			continue
		}
		cfg := cliConfig(t, false, "pgcheck_test", extra)
		if out, err := cliRun(t, cfg, "query_hotspot", "postgres"); err != nil {
			t.Fatalf("metadata with %v: %v\n%s", extra, err, out)
		}
	}
	// An engine upgrade does not automatically upgrade extension SQL objects.
	oldVersion := "1.6"
	if major >= 13 {
		oldVersion = "1.8"
	}
	execSQL(t, db, `DROP EXTENSION pg_stat_statements; CREATE EXTENSION pg_stat_statements SCHEMA "stats space" VERSION '`+oldVersion+`'`)
	for _, native := range []bool{false, true} {
		for _, command := range []string{"query_hotspot", "query_temp"} {
			if out, err := cliRun(t, cliConfig(t, native, "pgcheck_test", nil), command, "postgres"); err != nil {
				t.Fatalf("old extension %s: %v\n%s", oldVersion, err, out)
			}
		}
	}
	execSQL(t, db, "ALTER EXTENSION pg_stat_statements UPDATE")
	// A reset may leave zero denominators but must not make any new check fail.
	execSQL(t, db, `SELECT "stats space".pg_stat_statements_reset()`)
	for _, command := range []string{"query_hotspot", "query_temp"} {
		if out, err := cliRun(t, cliConfig(t, true, "pgcheck_test", nil), command, "postgres"); err != nil {
			t.Fatalf("after reset: %v\n%s", err, out)
		}
	}
	execSQL(t, db, "DROP EXTENSION pg_stat_statements")
	for _, native := range []bool{false, true} {
		cfg := cliConfig(t, native, "pgcheck_test", nil)
		out, err := cliRun(t, cfg, "query_hotspot", "postgres")
		if err == nil || !strings.Contains(out, "not installed") || strings.Contains(out, "permission denied") {
			t.Fatalf("missing-extension diagnostic: %v\n%s", err, out)
		}
	}
	execSQL(t, db, `CREATE EXTENSION pg_stat_statements SCHEMA "stats space"`)
	var elapsed float64
	start := time.Now()
	resultRows(t, db, assetSQL(t, "table_hotspot.sql"))
	elapsed = time.Since(start).Seconds()
	t.Logf("table_hotspot on test fixture: %.4fs; PostgreSQL %s", elapsed, strconv.Itoa(major))
}

func TestIntegrationUnloadedExtension(t *testing.T) {
	if os.Getenv("PGCHECK_TEST_MODE") != "unloaded" {
		t.Skip("requires the second, non-preloaded test cluster phase")
	}
	db, _ := integrationDB(t)
	execSQL(t, db, "CREATE EXTENSION pg_stat_statements")
	t.Cleanup(func() { execSQL(t, db, "DROP EXTENSION pg_stat_statements") })
	for _, native := range []bool{false, true} {
		cfg := cliConfig(t, native, "pgcheck_test", nil)
		out, err := cliRun(t, cfg, "query_hotspot", "postgres")
		if err == nil || !strings.Contains(out, "not loaded") || strings.Contains(out, "permission denied") {
			t.Fatalf("preload diagnostic: %v\n%s", err, out)
		}
	}
}
