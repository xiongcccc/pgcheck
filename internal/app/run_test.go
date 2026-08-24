package app

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

func TestDatabaseStatusSQLVersionColumns(t *testing.T) {
	pg11 := databaseStatusSQL(11)
	if strings.Contains(pg11, "checksum_failures") {
		t.Fatalf("PostgreSQL 11 query should not include checksum_failures")
	}
	if strings.Contains(pg11, "session_time") {
		t.Fatalf("PostgreSQL 11 query should not include session_time")
	}

	pg12 := databaseStatusSQL(12)
	if !strings.Contains(pg12, "checksum_failures") {
		t.Fatalf("PostgreSQL 12 query should include checksum_failures")
	}
	if strings.Contains(pg12, "session_time") {
		t.Fatalf("PostgreSQL 12 query should not include session_time")
	}

	pg14 := databaseStatusSQL(14)
	if !strings.Contains(pg14, "checksum_failures") || !strings.Contains(pg14, "session_time") {
		t.Fatalf("PostgreSQL 14 query should include checksum and session statistics")
	}
	if !strings.Contains(pg14, "round(100::numeric * blks_hit::numeric") {
		t.Fatalf("database status query should avoid integer division for cache hit ratio")
	}
	if !strings.Contains(pg14, "round(100::numeric * xact_commit::numeric") {
		t.Fatalf("database status query should avoid integer division for commit ratio")
	}
}

func TestExplainDoesNotRequireBackendCheck(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pgcheck.json")
	data := []byte(`{
  "connection": {
    "host": "203.0.113.1",
    "port": "1",
    "user": "postgres",
    "database": "postgres",
    "connect_timeout": "1"
  },
  "psql": {
    "path": "pgcheck-psql-does-not-exist"
  }
}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	app := New(fstest.MapFS{}, BuildInfo{Version: "test"})
	if err := app.Run([]string{"--config", path, "quick", "postgres", "--explain"}); err != nil {
		t.Fatalf("pure explain should not check psql or connect to PostgreSQL: %v", err)
	}
}

func TestQuickCommandRegistered(t *testing.T) {
	cmd, ok := commandMap()["quick"]
	if !ok {
		t.Fatalf("quick command is missing")
	}
	if !cmd.Database {
		t.Fatalf("quick should require a database name")
	}
	if cmd.Run == nil {
		t.Fatalf("quick should have a runner")
	}
	if !strings.Contains(commandExplain(cmd), "best-effort") {
		t.Fatalf("quick explain should describe best-effort behavior")
	}
}

func TestSQLAssetsUseSaferRelations(t *testing.T) {
	vacuumNeed, err := os.ReadFile("../../SQL/vacuum_need.sql")
	if err != nil {
		t.Fatal(err)
	}
	vacuumNeedSQL := string(vacuumNeed)
	if !strings.Contains(vacuumNeedSQL, "pg_stat_user_tables.relid = pg_class.oid") {
		t.Fatalf("vacuum_needed should join pg_class by oid/relid")
	}

	unusedIndexes, err := os.ReadFile("../../SQL/find_unused_indexes.sql")
	if err != nil {
		t.Fatal(err)
	}
	unusedIndexesSQL := string(unusedIndexes)
	for _, expected := range []string{
		"NOT pg_index.indisprimary",
		"AND NOT pg_index.indisunique",
		"AND NOT pg_index.indisexclusion",
		"c.conindid = idx_stat.indexrelid",
		"pg_relation_size(idx_stat.indexrelid) >= 32768",
	} {
		if !strings.Contains(unusedIndexesSQL, expected) {
			t.Fatalf("unused index SQL missing %q", expected)
		}
	}
}

func TestCommandRegistryHasUniqueNames(t *testing.T) {
	seen := map[string]bool{}
	for _, cmd := range commands() {
		if cmd.Name == "" {
			t.Fatalf("command name cannot be empty")
		}
		if cmd.Usage == "" {
			t.Fatalf("%s usage cannot be empty", cmd.Name)
		}
		if seen[cmd.Name] {
			t.Fatalf("duplicate command name %q", cmd.Name)
		}
		seen[cmd.Name] = true
	}
}

func TestCommandAliases(t *testing.T) {
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
	registry := commandMap()
	for alias, target := range aliases {
		cmd, ok := registry[alias]
		if !ok {
			t.Fatalf("alias %q is missing", alias)
		}
		if cmd.Name != target {
			t.Fatalf("alias %q resolved to %q, want %q", alias, cmd.Name, target)
		}
	}
}

func TestSplitTailFlags(t *testing.T) {
	positional, flags := splitTailFlags([]string{"postgres", "--show-sql", "public", "--explain"})
	if strings.Join(positional, ",") != "postgres,public" {
		t.Fatalf("positional = %v", positional)
	}
	if strings.Join(flags, ",") != "--show-sql,--explain" {
		t.Fatalf("flags = %v", flags)
	}
}
