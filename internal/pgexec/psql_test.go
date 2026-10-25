package pgexec

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
)

func TestIntegrationOutputOptions(t *testing.T) {
	if os.Getenv("PGCHECK_TEST_DSN") == "" {
		t.Skip("requires disposable integration cluster")
	}
	cfg := DefaultConfig()
	cfg.Connection = ConnectionConfig{Host: os.Getenv("PGCHECK_TEST_HOST"), Port: "5432", User: "pgcheck_test", Database: "postgres", SSLMode: "disable"}
	cfg.PSQL.Path = os.Getenv("PGCHECK_TEST_PSQL")
	r := NewRunner(cfg)
	for _, query := range []string{"SELECT 1 AS n, 'x' AS s", "SELECT 1 AS n, 'x' AS s UNION ALL SELECT 2, 'y'", "SELECT 1 AS n, 'x' AS s WHERE false"} {
		for _, opts := range []Options{{NoAlign: true}, {NoAlign: true, TuplesOnly: true}, {Expanded: true}, {Expanded: true, TuplesOnly: true}, {Expanded: true, NoAlign: true, TuplesOnly: true}} {
			var psql, native bytes.Buffer
			r.Backend, r.Out = "psql", &psql
			if err := r.Exec(context.Background(), opts, query); err != nil {
				t.Fatal(err)
			}
			r.Backend, r.Out = "native", &native
			if err := r.Exec(context.Background(), opts, query); err != nil {
				t.Fatal(err)
			}
			// Aligned spacing and record separators differ, but header suppression must agree.
			psqlHeader := strings.Contains(psql.String(), "RECORD") || strings.Contains(psql.String(), "n|")
			nativeHeader := strings.Contains(native.String(), "RECORD") || strings.Contains(native.String(), "n|")
			if psqlHeader != nativeHeader {
				t.Errorf("header mismatch opts=%+v\npsql=%q\nnative=%q", opts, psql.String(), native.String())
			}
			if opts.NoAlign && !opts.Expanded && psql.String() != native.String() {
				t.Errorf("unaligned tuple mismatch: %q != %q", psql.String(), native.String())
			}
			if opts.Expanded && opts.TuplesOnly && psql.String() != native.String() {
				t.Errorf("expanded tuple mismatch opts=%+v: %q != %q", opts, psql.String(), native.String())
			}
		}
	}
}
