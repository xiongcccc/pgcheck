package app

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
)

func TestHotspotHelpAndOfflineExplain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offline.json")
	if err := os.WriteFile(path, []byte(`{"connection":{"host":"invalid.pgcheck.test"},"psql":{"path":"pgcheck-missing-psql"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"query_hotspot", "query_temp", "table_hotspot"} {
		cmd, ok := commandMap()[name]
		if !ok || !cmd.Database || cmd.Explain == "" || cmd.Hidden {
			t.Fatalf("incomplete registration: %s", name)
		}
		if err := New(fstest.MapFS{}, BuildInfo{}).Run([]string{"-c", path, name, "postgres", "--explain"}); err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{name}, {name, "--show-sql"}, {name, "postgres", "typo"}} {
			if err := New(fstest.MapFS{}, BuildInfo{}).Run(append([]string{"-c", path}, args...)); err == nil {
				t.Fatalf("expected argument error: %v", args)
			}
		}
	}
}

func TestPermissionErrorsDoNotHideOtherFailures(t *testing.T) {
	for _, message := range []string{
		`column "op_bytes" of pg_stat_io does not exist`,
		`relation "pg_stat_statements" does not exist`,
		`pg_stat_statements must be loaded via shared_preload_libraries`,
		`integer out of range in pg_stat_replication`,
	} {
		err := errors.New(message)
		if got := decoratePermissionError("test", err); got != err {
			t.Fatalf("misclassified error: %v", got)
		}
	}
	if got := decoratePermissionError("test", errors.New("permission denied for view pg_stat_activity")); !strings.Contains(got.Error(), "pg_read_all_stats") {
		t.Fatalf("missing permission recommendation: %v", got)
	}
}

func TestHotspotDocumentationAndAttribution(t *testing.T) {
	data, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	sections := strings.Split(string(data), "\n## 中文\n")
	if len(sections) != 2 {
		t.Fatal("expected English and Chinese README sections")
	}
	for _, cmd := range commands() {
		if cmd.Hidden {
			continue
		}
		for i, section := range sections {
			if !strings.Contains(strings.Join(strings.Fields(section), " "), strings.Join(strings.Fields(cmd.Usage), " ")) {
				t.Fatalf("%s usage missing from README language %d", cmd.Name, i)
			}
		}
	}
	fields := []string{
		"total_exec_ms", "mean_exec_ms", "exec_time_share_pct", "rows_per_call",
		"temp_read", "temp_written", "temp_io_per_call", "temp_io_ms",
		"bytes_read", "read_share_pct", "cache_hit_pct", "total_size",
		"hot_update_pct", "newpage_updates", "fillfactor",
	}
	for _, field := range fields {
		for i, section := range sections {
			if !strings.Contains(section, "`"+field+"`") {
				t.Fatalf("%s explanation missing from README language %d", field, i)
			}
		}
	}
	notices, err := os.ReadFile("../../THIRD_PARTY_NOTICES.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"query_hotspot.sql", "query_temp.sql", "table_hotspot.sql", "table_updates.sql"} {
		data, err := os.ReadFile("../../SQL/" + file)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), "-- Adapted from fabiotr/pg_scripts") || !strings.Contains(string(data), "c35755bb7a464ffdbbb36f0339ec8d0a9d0865ad") || !strings.Contains(string(notices), file) {
			t.Fatalf("incomplete SQL attribution: %s", file)
		}
	}
}
