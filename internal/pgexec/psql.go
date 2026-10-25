package pgexec

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
)

type Runner struct {
	Config  Config
	Backend string
	Out     io.Writer
	Err     io.Writer
}

type Options struct {
	Database   string
	Expanded   bool
	TuplesOnly bool
	NoAlign    bool
}

type ServerVersion struct {
	Num   int
	Major int
	Text  string
}

func NewRunner(cfg Config) *Runner {
	cfg.Normalize()
	backend := "native"
	if _, err := exec.LookPath(cfg.PSQL.Path); err == nil {
		backend = "psql"
	}
	return &Runner{
		Config:  cfg,
		Backend: backend,
		Out:     os.Stdout,
		Err:     os.Stderr,
	}
}

func (r *Runner) ServerVersion(ctx context.Context, database string) (ServerVersion, error) {
	out, err := r.QueryScalar(ctx, Options{Database: database, TuplesOnly: true, NoAlign: true}, "SHOW server_version_num;")
	if err != nil {
		return ServerVersion{}, err
	}
	text := strings.TrimSpace(out)
	num, err := strconv.Atoi(text)
	if err != nil {
		return ServerVersion{}, fmt.Errorf("unexpected server_version_num %q", text)
	}
	return ServerVersion{
		Num:   num,
		Major: num / 10000,
		Text:  text,
	}, nil
}

func (r *Runner) QueryScalar(ctx context.Context, opts Options, query string) (string, error) {
	if r.Backend == "native" {
		db, err := sql.Open("postgres", r.connString(opts.Database))
		if err != nil {
			return "", err
		}
		defer db.Close()
		var value sql.NullString
		if err := db.QueryRowContext(ctx, query).Scan(&value); err != nil {
			return "", err
		}
		return value.String, nil
	}
	var stdout, stderr bytes.Buffer
	cmd := r.command(ctx, opts, query, true)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", formatCommandError(err, stderr.String())
	}
	return stdout.String(), nil
}

func (r *Runner) Exec(ctx context.Context, opts Options, query string) error {
	if r.Backend == "native" {
		db, err := sql.Open("postgres", r.connString(opts.Database))
		if err != nil {
			return err
		}
		defer db.Close()
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		defer rows.Close()
		return printRows(r.Out, rows, r.effectiveOptions(opts))
	}
	cmd := r.command(ctx, opts, query, false)
	var stderr bytes.Buffer
	cmd.Stdout = r.Out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return formatCommandError(err, stderr.String())
	}
	if stderr.Len() > 0 {
		_, _ = r.Err.Write(stderr.Bytes())
	}
	return nil
}

func (r *Runner) connString(database string) string {
	connection := r.Config.Connection
	fields := map[string]string{
		"host":     connection.Host,
		"port":     connection.Port,
		"user":     connection.User,
		"password": connection.Password,
		"dbname":   database,
		"sslmode":  connection.SSLMode,
	}
	if fields["dbname"] == "" {
		fields["dbname"] = connection.Database
	}
	parts := make([]string, 0, len(fields))
	for _, key := range []string{"host", "port", "user", "password", "dbname", "sslmode"} {
		if fields[key] == "" {
			continue
		}
		parts = append(parts, key+"="+quoteConnValue(fields[key]))
	}
	return strings.Join(parts, " ")
}

func quoteConnValue(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `'`, `\'`) + "'"
}

func (r *Runner) effectiveOptions(opts Options) Options {
	if r.Config.PSQL.TuplesOnly {
		opts.TuplesOnly = true
	}
	if r.Config.PSQL.NoAlign {
		opts.NoAlign = true
	}
	return opts
}

func printRows(out io.Writer, rows *sql.Rows, opts Options) error {
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	values := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range values {
		dest[i] = &values[i]
	}
	widths := make([]int, len(cols))
	for i, col := range cols {
		widths[i] = len(col)
	}
	var data [][]string
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}
		row := make([]string, len(cols))
		for i, value := range values {
			if value.Valid {
				row[i] = value.String
			} else {
				row[i] = ""
			}
			if len(row[i]) > widths[i] {
				widths[i] = len(row[i])
			}
		}
		data = append(data, row)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if opts.Expanded {
		return printExpanded(out, cols, data, opts)
	}
	if opts.NoAlign {
		if !opts.TuplesOnly {
			fmt.Fprintln(out, strings.Join(cols, "|"))
		}
		if err := printTuplesOnly(out, data, true); err != nil {
			return err
		}
		if !opts.TuplesOnly {
			printRowCount(out, len(data))
		}
		return nil
	}
	if opts.TuplesOnly {
		return printTuplesOnly(out, data, opts.NoAlign)
	}
	return printTable(out, cols, widths, data)
}

func printTable(out io.Writer, cols []string, widths []int, data [][]string) error {
	for i, col := range cols {
		if i > 0 {
			fmt.Fprint(out, "  ")
		}
		fmt.Fprintf(out, "%-*s", widths[i], col)
	}
	fmt.Fprintln(out)
	for i := range cols {
		if i > 0 {
			fmt.Fprint(out, "  ")
		}
		fmt.Fprint(out, strings.Repeat("-", widths[i]))
	}
	fmt.Fprintln(out)
	for _, row := range data {
		for i, cell := range row {
			if i > 0 {
				fmt.Fprint(out, "  ")
			}
			fmt.Fprintf(out, "%-*s", widths[i], cell)
		}
		fmt.Fprintln(out)
	}
	printRowCount(out, len(data))
	return nil
}

func printRowCount(out io.Writer, count int) {
	if count == 1 {
		fmt.Fprintln(out, "(1 row)")
		return
	}
	fmt.Fprintf(out, "(%d rows)\n", count)
}

func printExpanded(out io.Writer, cols []string, data [][]string, opts Options) error {
	nameWidth, valueWidth := 0, 0
	for _, col := range cols {
		if len(col) > nameWidth {
			nameWidth = len(col)
		}
	}
	for _, row := range data {
		for _, value := range row {
			if len(value) > valueWidth {
				valueWidth = len(value)
			}
		}
	}
	for idx, row := range data {
		if !opts.TuplesOnly {
			fmt.Fprintf(out, "-[ RECORD %d ]-------------------------\n", idx+1)
		} else if idx > 0 {
			if opts.NoAlign {
				fmt.Fprintln(out)
			} else {
				fmt.Fprintf(out, "%s+%s\n", strings.Repeat("-", nameWidth+1), strings.Repeat("-", valueWidth+1))
			}
		}
		for i, col := range cols {
			if opts.NoAlign {
				fmt.Fprintf(out, "%s|%s\n", col, row[i])
			} else {
				fmt.Fprintf(out, "%-*s | %s\n", nameWidth, col, row[i])
			}
		}
	}
	if opts.TuplesOnly && !opts.NoAlign {
		fmt.Fprintln(out)
	}
	return nil
}

func printTuplesOnly(out io.Writer, data [][]string, noAlign bool) error {
	sep := " "
	if noAlign {
		sep = "|"
	}
	for _, row := range data {
		fmt.Fprintln(out, strings.Join(row, sep))
	}
	return nil
}

func (r *Runner) command(ctx context.Context, opts Options, query string, scalar bool) *exec.Cmd {
	opts = r.effectiveOptions(opts)
	args := []string{"-v", "ON_ERROR_STOP=1"}
	if r.Config.PSQL.Quiet {
		args = append(args, "-q")
	}
	if r.Config.PSQL.NoPsqlrc {
		args = append(args, "-X")
	}
	if opts.Expanded {
		args = append(args, "-x")
	}
	if opts.TuplesOnly {
		args = append(args, "-t")
	}
	if opts.NoAlign {
		args = append(args, "-A")
	}
	if opts.Database != "" {
		args = append(args, "-d", opts.Database)
	}
	args = append(args, r.Config.PSQL.ExtraArgs...)
	// Metadata parsing must not inherit CSV, expanded output or startup formatting.
	if scalar {
		args = append(args, "-qAtX", "-P", "format=unaligned", "-P", "expanded=off", "-P", "tuples_only=on")
	}
	args = append(args, "-c", query)
	cmd := exec.CommandContext(ctx, r.Config.PSQL.Path, args...)
	cmd.Env = r.psqlEnv()
	return cmd
}

func (r *Runner) psqlEnv() []string {
	env := os.Environ()
	connection := r.Config.Connection
	pairs := map[string]string{
		"PGHOST":     connection.Host,
		"PGPORT":     connection.Port,
		"PGUSER":     connection.User,
		"PGPASSWORD": connection.Password,
		"PGDATABASE": connection.Database,
		"PGSSLMODE":  connection.SSLMode,
	}
	for key, value := range pairs {
		if value == "" {
			continue
		}
		env = append(env, key+"="+value)
	}
	return env
}

func formatCommandError(err error, stderr string) error {
	msg := strings.TrimSpace(stderr)
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if msg == "" {
		return err
	}
	return fmt.Errorf("%w: %s", err, msg)
}
