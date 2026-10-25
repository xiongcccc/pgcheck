package app

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/xiongcccc/pgcheck/internal/pgexec"
	"github.com/xiongcccc/pgcheck/internal/queries"
)

const statementCapabilitiesSQL = `SELECT coalesce((
    SELECT json_build_object(
        'schema', n.nspname,
        'loaded', current_setting('pg_stat_statements.max', true) IS NOT NULL,
        'tracking', current_setting('pg_stat_statements.track', true),
        'info', to_regclass(format('%I.pg_stat_statements_info', n.nspname)) IS NOT NULL,
        'columns', (SELECT json_agg(a.attname) FROM pg_attribute a
            WHERE a.attrelid = to_regclass(format('%I.pg_stat_statements', n.nspname))
              AND a.attnum > 0 AND NOT a.attisdropped))
    FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
    WHERE e.extname = 'pg_stat_statements'
), '{}'::json)`

type statementCapabilities struct {
	Schema   string   `json:"schema"`
	Loaded   bool     `json:"loaded"`
	Tracking string   `json:"tracking"`
	Info     bool     `json:"info"`
	Columns  []string `json:"columns"`
}

func runQueryHotspot(ctx context.Context, a *App, inv invocation) error {
	return a.runStatements(ctx, inv, "query_hotspot.sql")
}

func runQueryTemp(ctx context.Context, a *App, inv invocation) error {
	return a.runStatements(ctx, inv, "query_temp.sql")
}

func (a *App) runStatements(ctx context.Context, inv invocation, file string) error {
	opts := pgexec.Options{Database: inv.DB}
	metadata, err := a.runner.QueryScalar(ctx, opts, statementCapabilitiesSQL)
	if err != nil {
		return fmt.Errorf("inspect pg_stat_statements: %w", err)
	}
	var caps statementCapabilities
	if err := json.Unmarshal([]byte(metadata), &caps); err != nil {
		return fmt.Errorf("decode pg_stat_statements capabilities: %w", err)
	}
	if caps.Schema == "" {
		return fmt.Errorf("pg_stat_statements is not installed in database %q; ask the DBA to configure shared_preload_libraries and CREATE EXTENSION pg_stat_statements in this database", inv.DB)
	}
	if !inv.ShowSQL {
		if !caps.Loaded {
			return fmt.Errorf("pg_stat_statements is not loaded; ask the DBA to add it to shared_preload_libraries and restart PostgreSQL")
		}
		if caps.Tracking == "none" {
			return fmt.Errorf("pg_stat_statements.track is none; statement collection is disabled")
		}
	}
	if !hasColumn(caps.Columns, "calls") || (!hasColumn(caps.Columns, "total_exec_time") && !hasColumn(caps.Columns, "total_time")) {
		return fmt.Errorf("pg_stat_statements has an unsupported view definition; ask the DBA to review ALTER EXTENSION pg_stat_statements UPDATE")
	}
	schema := queries.QuoteIdentifier(caps.Schema)
	query, err := a.q.Read(file)
	if err != nil {
		return err
	}
	query = strings.ReplaceAll(query, "{{pg_stat_statements}}", schema+".pg_stat_statements")
	summary := "SELECT NULL::timestamptz AS stats_reset, NULL::bigint AS dealloc, "
	if caps.Info {
		summary = "SELECT stats_reset, dealloc, "
	}
	summary += "current_setting('pg_stat_statements.track') AS track, current_setting('track_io_timing') AS track_io_timing"
	if caps.Info {
		summary += " FROM " + schema + ".pg_stat_statements_info"
	}
	if inv.ShowSQL {
		printSQLTrace([]string{summary, query})
		return nil
	}
	opts.Expanded = a.displayExpanded(false)
	if err := a.runner.Exec(ctx, opts, summary); err != nil {
		return err
	}
	if err := a.runner.Exec(ctx, opts, query); err != nil {
		return err
	}
	printNotes(statementNotes)
	return nil
}

func hasColumn(columns []string, name string) bool {
	for _, column := range columns {
		if column == name {
			return true
		}
	}
	return false
}
