package app

import (
	"context"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/xiongcccc/pgcheck/internal/pgexec"
	"github.com/xiongcccc/pgcheck/internal/queries"
)

type BuildInfo struct {
	Version string
	Commit  string
	Date    string
}

type App struct {
	build  BuildInfo
	q      queries.Store
	runner *pgexec.Runner
}

type command struct {
	Name       string
	Usage      string
	Summary    string
	Database   bool
	Extra      []argSpec
	MinVersion int
	Hidden     bool
	Explain    string
	Run        func(context.Context, *App, invocation) error
}

type argSpec struct {
	Name     string
	Required bool
}

type invocation struct {
	Command string
	DB      string
	Args    []string
	Version pgexec.ServerVersion
	ShowSQL bool
	Explain bool
}

func New(sqlFS fs.FS, build BuildInfo) *App {
	cfg := pgexec.DefaultConfig()
	return &App{
		build:  build,
		q:      queries.NewStore(sqlFS),
		runner: pgexec.NewRunner(cfg),
	}
}

func (a *App) Run(args []string) error {
	ctx := context.Background()
	cfg, rest, help, err := parseGlobalArgs(args)
	if err != nil {
		return err
	}
	a.runner = pgexec.NewRunner(cfg)
	if len(rest) == 0 || help || isHelp(rest[0]) {
		a.printHelp()
		return nil
	}
	if rest[0] == "version" {
		fmt.Println(a.build.String())
		return nil
	}

	cmd, ok := commandMap()[strings.ToLower(rest[0])]
	if !ok {
		return fmt.Errorf("unknown command %q; run pgcheck help", rest[0])
	}

	inv := invocation{Command: cmd.Name}
	rest = rest[1:]
	if cmd.Database {
		if len(rest) == 0 {
			return fmt.Errorf("%s requires database name\nusage: %s", cmd.Name, cmd.Usage)
		}
		inv.DB = rest[0]
		rest = rest[1:]
	}
	var tailFlags []string
	rest, tailFlags = splitTailFlags(rest)
	for _, flag := range tailFlags {
		switch flag {
		case "--show-sql":
			inv.ShowSQL = true
		case "--explain":
			inv.Explain = true
		default:
			return fmt.Errorf("unknown command option %q; supported options: --show-sql, --explain", flag)
		}
	}
	for _, spec := range cmd.Extra {
		if spec.Required && len(rest) == 0 {
			return fmt.Errorf("%s requires %s\nusage: %s", cmd.Name, spec.Name, cmd.Usage)
		}
	}
	inv.Args = rest

	if inv.Explain && !inv.ShowSQL {
		fmt.Println(commandExplain(cmd))
		return nil
	}

	if err := a.runner.Check(ctx); err != nil {
		return err
	}

	versionDB := inv.DB
	if versionDB == "" {
		versionDB = cfg.Connection.Database
	}
	version, err := a.runner.ServerVersion(ctx, versionDB)
	if err != nil {
		return fmt.Errorf("detect PostgreSQL server version: %w", err)
	}
	inv.Version = version
	if cmd.MinVersion > 0 && version.Major < cmd.MinVersion {
		if cmd.Name == "io" || cmd.Name == "io_hotspot" {
			return fmt.Errorf("pg_stat_io is available only in PostgreSQL 16+")
		}
		return fmt.Errorf("%s requires PostgreSQL %d or later; connected server major version is %d", cmd.Name, cmd.MinVersion, version.Major)
	}
	if inv.Explain {
		fmt.Println(commandExplain(cmd))
	}
	if err := cmd.Run(ctx, a, inv); err != nil {
		return decoratePermissionError(cmd.Name, err)
	}
	return nil
}

func (a *App) printHelp() {
	fmt.Printf("pgcheck %s - PostgreSQL health check toolkit\n\n", a.build.Version)
	fmt.Println("Usage:")
	fmt.Println("  pgcheck [--config path] [-x] [-A] [-t] <command> [database] [arguments]")
	fmt.Println()
	fmt.Println("Connection:")
	fmt.Println("  Prefer a JSON config file for connection and output settings.")
	fmt.Println("  Environment variables are still supported as defaults for quick local usage.")
	fmt.Println("  Auto-discovered config files: ./pgcheck.json, ./.pgcheck.json, ~/.pgcheck.json.")
	fmt.Println()
	fmt.Println("Options:")
	fmt.Println("  -c, --config <path>                          Read JSON config file")
	fmt.Println("  -x                                           Force expanded output, like psql -x")
	fmt.Println("  -A                                           Force unaligned output, like psql -A")
	fmt.Println("  -t                                           Force tuples-only output, like psql -t")
	fmt.Println("  -h, --help                                   Show this help")
	fmt.Println("  -v, --version                                Show build version")
	fmt.Println()
	fmt.Println("Config example:")
	fmt.Println("  cp pgcheck.example.json pgcheck.json")
	fmt.Println("  pgcheck --config pgcheck.json dbstatus")
	fmt.Println("  pgcheck quick postgres")
	fmt.Println("  pgcheck -c pgcheck.json connections postgres")
	fmt.Println("  pgcheck -x lock postgres")
	fmt.Println("  pgcheck -At dbstatus")
	fmt.Println("  pgcheck table_bloat postgres --show-sql")
	fmt.Println("  pgcheck wraparound_risk postgres --explain")
	fmt.Println()
	fmt.Println("Execution:")
	fmt.Println("  pgcheck uses psql automatically when it is available.")
	fmt.Println("  If psql is not found, pgcheck falls back to its native Go PostgreSQL driver.")
	fmt.Println("  psql-style defaults such as -q, -t, -A, and -X can be configured in pgcheck.json.")
	fmt.Println("  -x, -A, and -t on the command line override output settings for one run.")
	fmt.Println("  Add --show-sql or --explain after a command to inspect how a check works.")
	fmt.Println()
	fmt.Println("Commands:")
	cmds := commands()
	sort.Slice(cmds, func(i, j int) bool { return cmds[i].Name < cmds[j].Name })
	for _, cmd := range cmds {
		if cmd.Hidden {
			continue
		}
		fmt.Printf("  %-44s %s\n", cmd.Usage, cmd.Summary)
	}
	fmt.Println("  help                                         Show this help")
	fmt.Println("  version                                      Show build version")
}

func commands() []command {
	return []command{
		sqlCommand("alltoast", "pgcheck alltoast <database> <schema>", "List TOAST tables in a schema", true, []argSpec{{"schema", true}}, 0, false, []queryStep{{File: "all_toast.sql", Replacements: []replacement{{Old: "'public'", Arg: 0}}}}),
		sqlCommand("analyze_needed", "pgcheck analyze_needed <database>", "Show tables that need ANALYZE", true, nil, 0, false, []queryStep{{File: "analyze_need.sql", Notes: analyzeNeedNotes}}),
		{
			Name:    "checkpoint",
			Usage:   "pgcheck checkpoint",
			Summary: "Show background writer and checkpointer statistics",
			Run:     runCheckpoint,
		},
		sqlCommand("connections", "pgcheck connections <database>", "Show connection summary and active queries", true, nil, 0, false, []queryStep{{File: "current_connection.sql"}, {File: "current_query.sql"}}),
		{
			Name: "dbstatus", Usage: "pgcheck dbstatus", Summary: "Show database-level statistics", Run: runDBStatus,
		},
		withExplain(command{
			Name:     "quick",
			Usage:    "pgcheck quick <database>",
			Summary:  "Run a small best-effort DBA quick check",
			Database: true,
			Run:      runQuick,
		}, "This check runs a small, read-only, best-effort set of common DBA checks: database status, lock tree, long transactions, xmin blockers, replication slots, WAL health, vacuum queue, and active temporary files. Individual permission errors are reported as warnings so one blocked check does not hide the rest."),
		sqlCommand("freeze", "pgcheck freeze <database>", "Show transaction ID consumption and freeze risk", true, nil, 0, true, []queryStep{{File: "age_consume.sql", Notes: freezeNotes}, {File: "age_consume_dblvl.sql", Header: "Database-level transaction ID usage:"}, {File: "age_consume_rel_lvl.sql", Header: "Top relations by frozen xid age:"}}),
		sqlCommand("index_bloat", "pgcheck index_bloat <database>", "Estimate btree index bloat", true, nil, 0, false, []queryStep{{File: "index_bloat.sql", Header: "This query may take a while.", Notes: indexBloatNotes}}),
		sqlCommand("index_create", "pgcheck index_create <database>", "Show CREATE INDEX progress", true, nil, 12, true, []queryStep{{File: "index_create.sql", Repeat: 5, RepeatDelay: time.Second, EmptyMessage: "no running create index process", Notes: indexCreateNotes}}),
		sqlCommand("index_duplicate", "pgcheck index_duplicate <database>", "Find duplicate indexes", true, nil, 0, false, []queryStep{{File: "index_duplicate.sql"}}),
		sqlCommand("index_efficiency", "pgcheck index_efficiency <database>", "Find low-efficiency indexes", true, nil, 0, false, []queryStep{{File: "index_lower_efficiency.sql", Notes: indexLowNotes}}),
		sqlCommand("index_null_risk", "pgcheck index_null_risk <database>", "Find large single-column indexes on high-null columns", true, nil, 0, false, []queryStep{{File: "find_indexed_columns_high_null_frac.sql", Notes: indexNullFracNotes}}),
		sqlCommand("index_health", "pgcheck index_health <database>", "Show index details and invalid indexes", true, nil, 0, false, []queryStep{{File: "index_state.sql"}, {File: "index_state_further.sql"}}),
		withExplain(sqlCommand("io", "pgcheck io", "Show pg_stat_io distribution by backend and object", false, nil, 16, false, []queryStep{{File: "io.sql", Notes: ioNotes}}), "This check reads pg_stat_io, available in PostgreSQL 16+, to show read/write/extend/writeback/fsync counters, timing, evictions, and reuses by backend_type, object, and context."),
		withExplain(sqlCommand("io_hotspot", "pgcheck io_hotspot", "Show pg_stat_io hotspots by time and operations", false, nil, 16, false, []queryStep{{File: "io_hotspot.sql", Notes: ioHotspotNotes}}), "This check ranks pg_stat_io rows by accumulated I/O time and operation count to highlight relation, temp, and WAL I/O pressure from backends, checkpointers, bgwriters, or autovacuum."),
		sqlCommand("integer_pk_risk", "pgcheck integer_pk_risk <database>", "Show int2/int4 primary key capacity risk", true, nil, 10, false, []queryStep{{File: "int_pk_risk.sql", Notes: intPKRiskNotes}}),
		sqlCommand("unused_indexes", "pgcheck unused_indexes <database>", "Find unused and rarely used indexes", true, nil, 0, false, []queryStep{{File: "find_unused_indexes.sql", Notes: unusedIndexesNotes}}),
		withExplain(sqlCommand("lock", "pgcheck lock <database>", "Show lock waits and blocking queue", true, nil, 0, false, []queryStep{{File: "lock_wait_state_further.sql"}, {File: "lock_wait_state.sql", Expanded: true}, {File: "lock_wait_queue.sql", Header: "wait queue:", Notes: lockNotes}}), "This check joins pg_locks and pg_stat_activity to show blocked sessions, blocking sessions, lock modes, wait duration, and lock wait queues."),
		withExplain(sqlCommand("lock_tree", "pgcheck lock_tree <database>", "Show recursive blocking trees and root blockers", true, nil, 0, false, []queryStep{{File: "lock_tree.sql", Notes: lockTreeNotes}}), "This check uses pg_blocking_pids() and a recursive CTE over pg_stat_activity to build lock trees, helping identify root blockers and the sessions blocked beneath them."),
		sqlCommand("long_transaction", "pgcheck long_transaction <database>", "Show long-running transactions", true, nil, 0, false, []queryStep{{File: "long_transaction.sql", Notes: longTransactionNotes}}),
		sqlCommand("object", "pgcheck object <database> <user>", "Show objects owned by a user and role membership", true, []argSpec{{"user", true}}, 0, false, []queryStep{{OptionalInline: userObjectSQL, Replacements: []replacement{{Old: "'pgcheck_user'", Arg: 0}}}, {File: "user_member.sql", Header: "user member relationship:"}}),
		sqlCommand("partition", "pgcheck partition <database>", "Show native and inherited partition information", true, nil, 0, false, []queryStep{{File: "partition_info.sql", Header: "native partition:"}, {File: "partition_inherit_info.sql", Header: "inherit and native partition:"}, {File: "partition_size.sql", Header: "partition size:"}}),
		withExplain(sqlCommand("privilege", "pgcheck privilege", "Show current monitoring privileges", false, nil, 0, true, []queryStep{{File: "privilege.sql", Notes: privilegeNotes}}), "This check inspects the current role, superuser flag, pg_monitor, pg_read_all_stats, pg_read_all_settings, replication privilege, and selected monitoring function execute privileges."),
		sqlCommand("relation", "pgcheck relation <database> <schema>", "List table and index size in a schema", true, []argSpec{{"schema", true}}, 0, false, []queryStep{{File: "all_relation.sql", Replacements: []replacement{{Old: "'public'", Arg: 0}}}}),
		withExplain(sqlCommand("table_bloat", "pgcheck table_bloat <database>", "Estimate table bloat and vacuum blockers", true, nil, 0, false, []queryStep{{File: "relation_bloat.sql", Header: "This query may take a while. Run ANALYZE first for better estimates.", Notes: relationBloatNotes}, {File: "get_oldest_xmin.sql", Header: "Oldest xmin that may block vacuum:", Expanded: true}, {File: "get_oldest_xact.sql", Header: "Oldest values for vacuum blockers:", Expanded: true}}), "This check estimates table bloat from planner statistics and page layout assumptions, then shows old xmin/xact blockers that can prevent VACUUM cleanup."),
		sqlCommand("relconstraint", "pgcheck relconstraint <database> <relation>", "List constraints and multi-column indexes for a relation", true, []argSpec{{"relation", true}}, 0, false, []queryStep{{File: "rel_constraint.sql", Replacements: []replacement{{Old: "'test'", Arg: 0}}}, {File: "rel_multi_index.sql", Replacements: []replacement{{Old: "'test%'", Arg: 0}}, Notes: relConstraintNotes}}),
		sqlCommand("reltoast", "pgcheck reltoast <database> <relation>", "Show TOAST-related columns for a relation", true, []argSpec{{"relation", true}}, 0, false, []queryStep{{File: "single_toast.sql", Replacements: []replacement{{Old: "'test'", Arg: 0}}}, {File: "single_toast_relation.sql", OptionalInline: singleToastRelationSQL, Replacements: []replacement{{Old: "'test'", Arg: 0}}}}),
		{Name: "replication", Usage: "pgcheck replication", Summary: "Show physical streaming replication status", Run: runReplication},
		{Name: "replication_slots", Usage: "pgcheck replication_slots", Summary: "Show replication slot xmin and retained WAL", Run: runReplicationSlots},
		withExplain(sqlCommand("temp_files", "pgcheck temp_files", "Show active sessions using temporary files", false, nil, 0, false, []queryStep{{File: "temp_files.sql", Notes: tempFilesNotes}}), "This check scans pgsql_tmp directories with pg_ls_dir and pg_stat_file, groups temporary files by backend pid, and joins pg_stat_activity to show which active sessions are consuming temporary file space."),
		sqlCommand("vacuum_needed", "pgcheck vacuum_needed <database>", "Show tables likely to need vacuum", true, nil, 0, false, []queryStep{{File: "vacuum_need.sql"}}),
		sqlCommand("vacuum_queue", "pgcheck vacuum_queue <database>", "Show vacuum queue and running vacuum details", true, nil, 0, false, []queryStep{{File: "vacuum_queue.sql", Notes: vacuumQueueNotes}}),
		{
			Name:       "vacuum_state",
			Usage:      "pgcheck vacuum_state <database>",
			Summary:    "Show running VACUUM progress",
			Database:   true,
			MinVersion: 11,
			Run:        runVacuumState,
		},
		sqlCommand("wait_event", "pgcheck wait_event <database>", "Show wait events and wait event types", true, nil, 0, false, []queryStep{{File: "wait_event.sql"}}),
		sqlCommand("wal_archive", "pgcheck wal_archive", "Show WAL archiver statistics", false, nil, 0, true, []queryStep{{File: "wal_archive_state.sql"}}),
		sqlCommand("wal_generate", "pgcheck wal_generate <wal_path>", "Show WAL generation speed by scanning pg_wal", false, []argSpec{{"wal_path", true}}, 0, true, []queryStep{{File: "wal_generate_speed.sql", Replacements: []replacement{{Old: "'/home/postgres/15data/pg_wal'", Arg: 0}}}}),
		sqlCommand("wal_health", "pgcheck wal_health", "Show WAL retention and archiving health", false, nil, 0, true, []queryStep{{File: "wal_health.sql", Notes: walHealthNotes}}),
		withExplain(sqlCommand("wraparound_risk", "pgcheck wraparound_risk <database>", "Show XID and MultiXID wraparound risk", true, nil, 0, false, []queryStep{{File: "xid_wraparound.sql", Header: "database-level wraparound risk:"}, {File: "xid_wraparound_rel.sql", Header: "top relations by XID/MultiXID age:", Notes: xidWraparoundNotes}}), "This check uses age(datfrozenxid), mxid_age(datminmxid), age(relfrozenxid), and mxid_age(relminmxid) to estimate database- and relation-level wraparound risk."),
		withExplain(sqlCommand("xmin_blockers", "pgcheck xmin_blockers", "Show global xmin horizon blockers", false, nil, 0, true, []queryStep{{File: "xmin_horizon.sql", Notes: xminHorizonNotes}}), "This check compares xmin retained by pg_stat_activity, pg_replication_slots, pg_stat_replication, and pg_prepared_xacts to find why the global xmin horizon is not advancing."),
	}
}

func commandMap() map[string]command {
	out := make(map[string]command)
	for _, cmd := range commands() {
		out[cmd.Name] = cmd
	}
	aliases := map[string]string{
		"analyze_need":    "analyze_needed",
		"index_low":       "index_efficiency",
		"index_null_frac": "index_null_risk",
		"index_state":     "index_health",
		"int_pk_risk":     "integer_pk_risk",
		"sequence_risk":   "integer_pk_risk",
		"relation_bloat":  "table_bloat",
		"vacuum_need":     "vacuum_needed",
		"xid_wraparound":  "wraparound_risk",
		"xmin_horizon":    "xmin_blockers",
	}
	for alias, target := range aliases {
		if cmd, ok := out[target]; ok {
			out[alias] = cmd
		}
	}
	return out
}

func splitTailFlags(args []string) ([]string, []string) {
	var positional []string
	var flags []string
	for _, arg := range args {
		if arg == "--show-sql" || arg == "--explain" {
			flags = append(flags, arg)
			continue
		}
		positional = append(positional, arg)
	}
	return positional, flags
}

func decoratePermissionError(commandName string, err error) error {
	msg := err.Error()
	checks := []struct {
		Pattern        string
		Requirement    string
		Recommendation string
	}{
		{"pg_stat_replication", "pg_stat_replication requires pg_monitor or superuser.", "GRANT pg_monitor TO <user>;"},
		{"pg_replication_slots", "pg_replication_slots requires pg_monitor or superuser for full monitoring visibility.", "GRANT pg_monitor TO <user>;"},
		{"pg_locks", "pg_locks visibility is best with pg_monitor or superuser.", "GRANT pg_monitor TO <user>;"},
		{"pg_stat_activity", "pg_stat_activity full query visibility requires pg_read_all_stats, pg_monitor, or superuser.", "GRANT pg_read_all_stats TO <user>;"},
		{"pg_ls_dir", "pg_ls_dir requires execute privilege, pg_monitor, or superuser.", "GRANT EXECUTE ON FUNCTION pg_ls_dir(text, boolean, boolean) TO <user>;"},
		{"pg_ls_waldir", "pg_ls_waldir requires execute privilege, pg_monitor, or superuser.", "GRANT EXECUTE ON FUNCTION pg_ls_waldir() TO <user>;"},
		{"pg_stat_file", "pg_stat_file requires execute privilege, pg_monitor, or superuser.", "GRANT EXECUTE ON FUNCTION pg_stat_file(text, boolean) TO <user>;"},
		{"pg_control", "pg_control_* functions require pg_monitor or superuser.", "GRANT pg_monitor TO <user>;"},
		{"pg_stat_statements", "pg_stat_statements requires the extension and sufficient stats visibility.", "CREATE EXTENSION pg_stat_statements; GRANT pg_read_all_stats TO <user>;"},
	}
	lower := strings.ToLower(msg)
	for _, check := range checks {
		if strings.Contains(lower, strings.ToLower(check.Pattern)) {
			return fmt.Errorf("permission denied while running %s: %s\nRecommendation:\n  %s\nOriginal error: %w", commandName, check.Requirement, check.Recommendation, err)
		}
	}
	if strings.Contains(lower, "permission denied") {
		return fmt.Errorf("permission denied while running %s. Run `pgcheck privilege` to inspect monitoring privileges.\nOriginal error: %w", commandName, err)
	}
	return err
}

func commandExplain(cmd command) string {
	if strings.TrimSpace(cmd.Explain) == "" {
		return "This check runs read-only SQL against PostgreSQL catalog and statistics views."
	}
	return cmd.Explain
}

func printSQLTrace(sqls []string) {
	fmt.Println("SQL:")
	for i, sql := range sqls {
		if len(sqls) > 1 {
			fmt.Printf("-- query %d\n", i+1)
		}
		fmt.Println(strings.TrimSpace(sql))
		if !strings.HasSuffix(strings.TrimSpace(sql), ";") {
			fmt.Println(";")
		}
	}
}

func isHelp(s string) bool {
	switch strings.ToLower(s) {
	case "help", "h", "-h", "--help":
		return true
	default:
		return false
	}
}
