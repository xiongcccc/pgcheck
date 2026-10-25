<div align="center">

<img src="assets/pgcheck-logo.png" alt="pgcheck logo" width="720">

# pgcheck

**A ready-to-run PostgreSQL DBA helper for quick checks and focused troubleshooting.**

[English](#pgcheck) | [中文](#中文)

<p>
  <a href="https://github.com/xiongcccc/pgcheck/stargazers"><img alt="stars" src="https://img.shields.io/github/stars/xiongcccc/pgcheck?style=flat&logo=github&label=stars&color=0366d6"></a>
  <a href="https://github.com/xiongcccc/pgcheck/issues"><img alt="issues" src="https://img.shields.io/github/issues/xiongcccc/pgcheck?style=flat&logo=github&label=issues&color=f1c40f"></a>
  <a href="https://github.com/xiongcccc/pgcheck/commits/master"><img alt="last commit" src="https://img.shields.io/github/last-commit/xiongcccc/pgcheck?style=flat&logo=github&label=last%20commit&color=6cc644"></a>
  <a href="https://github.com/xiongcccc/pgcheck/blob/master/LICENSE"><img alt="license" src="https://img.shields.io/badge/license-Apache--2.0-blue"></a>
</p>

<p>
  <img alt="version" src="https://img.shields.io/badge/version-2.0.1-2ea44f">
  <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-11--18-336791?logo=postgresql&logoColor=white">
  <img alt="Go" src="https://img.shields.io/badge/Go-1.23%2B-00ADD8?logo=go&logoColor=white">
  <img alt="psql" src="https://img.shields.io/badge/psql-supported-555555">
</p>

<p>
  <img alt="built by" src="https://img.shields.io/badge/built%20by-xiongcccc-ff6a3d">
  <a href="https://postgres-howto.cn/#/README"><img alt="postgres-howto" src="https://img.shields.io/badge/postgres--howto-integrated-5b5ff0"></a>
  <img alt="pg_stat_io" src="https://img.shields.io/badge/pg__stat__io-PG16%2B-8a63d2">
</p>

</div>

`pgcheck` is a lightweight DBA helper for quickly answering practical PostgreSQL questions: is the database blocked, are there long transactions, is vacuum falling behind, are replication slots retaining WAL, are temporary files growing, and which check should I run next?

It is intentionally not a monitoring platform. It runs read-only inspection SQL on demand, keeps output close to `psql`, and keeps each SQL visible through `--show-sql` or `--explain` when you want to verify the logic.

> [!IMPORTANT]
> `pgcheck` runs read-only inspection SQL, but some checks need monitoring privileges such as `pg_monitor`, `pg_read_all_stats`, or execute privilege on PostgreSQL monitoring functions. Run `pgcheck privilege` first when using a restricted monitoring role.

The project started as a Bash-based one-click inspection script. It is now a compact Go CLI with embedded SQL assets, explicit command registration, server-version detection, and a cleaner compatibility model.

## Highlights

- Start with `pgcheck quick <database>` for a small best-effort overview.
- Use focused commands such as `lock_tree`, `xmin_blockers`, `vacuum_queue`, `wal_health`, and `temp_files` when you need to drill down.
- Uses PostgreSQL's standard `psql` behavior when available, including `.pgpass`, service files, SSL options, and familiar output flags.
- Finds expensive statements, cumulative temporary I/O, and table read/update hotspots with three focused checks.
- Detects PostgreSQL server version and chooses version-aware SQL for checks that need it.
- Keeps SQL as plain files under `SQL/` so DBAs can review, reuse, and improve the checks.

## Quick Start

```bash
make build
cp pgcheck.example.json pgcheck.json
chmod 600 pgcheck.json
```

Edit `connection` in `pgcheck.json` for your instance, then run:

```bash
bin/pgcheck privilege
bin/pgcheck quick postgres
```

Replace `postgres` with your target database. The local config is discovered automatically; use `-c /path/to/pgcheck.json` before the command for another instance. Do not commit credentials.

| Question | Next command |
| --- | --- |
| Which sessions are blocking others? | `pgcheck lock_tree postgres` |
| Which statements accumulated the most execution time? | `pgcheck query_hotspot postgres` |
| Which statements accumulated the most temporary I/O? | `pgcheck query_temp postgres` |
| Which tables accumulated reads or updates? | `pgcheck table_hotspot postgres` |
| Which sessions currently have temporary files? | `pgcheck temp_files` |
| Which backend types account for I/O? | `pgcheck io_hotspot` (PG16+) |
| Is ordinary VACUUM work falling behind? | `pgcheck vacuum_needed postgres` |

The two statement commands require `pg_stat_statements`; `quick` and `table_hotspot` do not.

## Compatibility

The original tool was tested up to PostgreSQL 15. The Go refactor provides version-aware checks for PostgreSQL 11-18. This change's new checks and regression suite passed against real PostgreSQL 11.22, 12.22, 13.23, 14.5, 15.14, 16.14, 17.6, and 18.4 instances. See the [verification record](tests/VERIFICATION.md) for the exact coverage and limitations; this is not a claim that every command or minor release was tested.

PostgreSQL 17+ changed some statistics views, including checkpoint and VACUUM progress views. `pgcheck` ships version-specific SQL for those checks. `io` and `io_hotspot` require PG16+ and support PG18's byte-counter layout and WAL rows. VACUUM progress/queue commands require PG12+; PG11 users can use `vacuum_needed`. `wal_generate` requires a valid server-side `pg_wal` path and should be verified per deployment. Compatibility with an old major version does not imply that PostgreSQL still maintains that version.

## Requirements

- Go 1.23+ for building from source.
- PostgreSQL client tools, especially `psql`, are recommended. If `psql` is not available, `pgcheck` automatically falls back to the native Go driver.
- A PostgreSQL role with enough privileges to read the relevant catalog and statistics views.

`pgcheck` keeps the command line intentionally small. Put connection, output, and psql behavior in a JSON config file. Environment variables remain available as defaults for quick local usage.

Use a config file when you do not want to export environment variables:

```bash
cp pgcheck.example.json pgcheck.json
bin/pgcheck --config pgcheck.json dbstatus
bin/pgcheck quick postgres
bin/pgcheck -c pgcheck.json connections postgres
```

Config shape:

```json
{
  "_comment": "Keys starting with _comment are documentation only and ignored by pgcheck.",
  "connection": {
    "_comment": "Equivalent to PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE and PGSSLMODE.",
    "host": "127.0.0.1",
    "port": "5432",
    "user": "postgres",
    "password": "",
    "database": "postgres",
    "sslmode": "disable"
  },
  "psql": {
    "_comment": "These options mirror psql command-line flags and mainly affect the psql backend.",
    "path": "psql",
    "_quiet": "quiet=true is the same as psql -q: reduce non-data messages.",
    "quiet": true,
    "_tuples_only": "tuples_only=true is the same as psql -t: show rows without headers/footers.",
    "tuples_only": false,
    "_no_align": "no_align=true is the same as psql -A: use unaligned output.",
    "no_align": false,
    "_no_psqlrc": "no_psqlrc=true is the same as psql -X: ignore ~/.psqlrc for stable output.",
    "no_psqlrc": true,
    "_extra_args": "extra_args are appended directly to psql, for example [\"--csv\"] or [\"--set\", \"ON_ERROR_STOP=1\"].",
    "extra_args": []
  },
  "output": {
    "_expanded": "auto keeps command defaults; table forces normal table output; expanded forces psql -x style output.",
    "expanded": "auto"
  }
}
```

You can also use environment variables:

```bash
export PGHOST=127.0.0.1
export PGPORT=5432
export PGUSER=postgres
export PGPASSWORD=secret
```

If `psql` is not in `PATH`, set `psql.path` in the config file. When `psql` cannot be found, `pgcheck` automatically uses the native Go driver:

Common psql-style display defaults are configured in `pgcheck.json`. For one-off runs, `pgcheck` also supports `-x`, `-A`, and `-t` before the command, with the same meaning as `psql`:

```bash
bin/pgcheck -x lock postgres
bin/pgcheck -At dbstatus
```

Option notes for users familiar with `psql`:

| Config key | psql flag | Meaning |
| --- | --- | --- |
| `quiet` | `-q` | Reduce non-data messages printed by `psql`. |
| `tuples_only` | `-t` | Print rows without column headers and footers. |
| `no_align` | `-A` | Use unaligned output instead of padded table output. |
| `no_psqlrc` | `-X` | Do not read `~/.psqlrc`, making output more predictable. |
| `extra_args` | passthrough | Append raw arguments to `psql`, such as `--csv` or `--set name=value`. |

## Build

```bash
make build
```

`make build` embeds the version, Git commit, and UTC build time. A direct `go build -o bin/pgcheck .` also works for development, but does not inject the same build metadata. Include the full version output when reporting a problem, together with the PostgreSQL server version and command used; remove credentials and sensitive SQL.

Show help:

```bash
bin/pgcheck help
```

Show version:

```bash
bin/pgcheck version
```

## Commands

```text
pgcheck relation <database> <schema>         List table and index size in a schema
pgcheck relconstraint <database> <relation>  List constraints and multi-column indexes for a relation
pgcheck alltoast <database> <schema>         List TOAST tables in a schema
pgcheck reltoast <database> <relation>       Show TOAST-related columns for a relation
pgcheck analyze_needed <database>            Show tables that need ANALYZE
pgcheck dbstatus                             Show database-level statistics
pgcheck quick <database>                     Run a small best-effort DBA quick check
pgcheck query_hotspot <database>             Show top 10 statements by cumulative execution time
pgcheck query_temp <database>                Show top 10 statements by cumulative temporary I/O
pgcheck table_hotspot <database>             Show top 10 tables by reads and updates
pgcheck index_bloat <database>               Estimate btree index bloat
pgcheck index_duplicate <database>           Find duplicate indexes
pgcheck index_efficiency <database>          Find low-efficiency indexes
pgcheck index_null_risk <database>           Find large single-column indexes on high-null columns
pgcheck index_health <database>              Show index details and invalid indexes
pgcheck integer_pk_risk <database>           Show int2/int4 primary key capacity risk
pgcheck io                                   Show pg_stat_io distribution by backend and object
pgcheck io_hotspot                           Show pg_stat_io hotspots by time and operations
pgcheck unused_indexes <database>            Find unused and rarely used indexes
pgcheck lock <database>                      Show lock waits and blocking queue
pgcheck lock_tree <database>                 Show recursive blocking trees and root blockers
pgcheck checkpoint                           Show background writer and checkpointer statistics
pgcheck freeze <database>                    Show transaction ID consumption and freeze risk
pgcheck replication                          Show physical streaming replication status
pgcheck replication_slots                    Show replication slot xmin and retained WAL
pgcheck connections <database>               Show connection summary and active queries
pgcheck long_transaction <database>          Show long-running transactions
pgcheck privilege                            Show current monitoring privileges
pgcheck table_bloat <database>               Estimate table bloat and vacuum blockers
pgcheck temp_files                           Show active sessions using temporary files
pgcheck vacuum_state <database>              Show running VACUUM progress
pgcheck vacuum_needed <database>             Show tables likely to need vacuum
pgcheck vacuum_queue <database>              Show vacuum candidates and running vacuum details (PG12+)
pgcheck index_create <database>              Show CREATE INDEX progress
pgcheck wal_archive                          Show WAL archiver statistics
pgcheck wal_generate <wal_path>              Show WAL generation speed by scanning pg_wal
pgcheck wal_health                           Show WAL retention and archiving health
pgcheck wait_event <database>                Show wait events and wait event types
pgcheck partition <database>                 Show partition information
pgcheck object <database> <user>             Show objects owned by a user and role membership
pgcheck wraparound_risk <database>           Show XID and MultiXID wraparound risk
pgcheck xmin_blockers                        Show global xmin horizon blockers
```

## Examples

```bash
bin/pgcheck dbstatus
bin/pgcheck quick postgres
bin/pgcheck query_hotspot postgres
bin/pgcheck query_temp postgres
bin/pgcheck table_hotspot postgres
bin/pgcheck -x lock postgres
bin/pgcheck -At dbstatus
bin/pgcheck --config pgcheck.json dbstatus
bin/pgcheck connections postgres
bin/pgcheck lock postgres
bin/pgcheck lock_tree postgres
bin/pgcheck freeze postgres
bin/pgcheck relation postgres public
bin/pgcheck index_bloat postgres
bin/pgcheck xmin_blockers
bin/pgcheck wraparound_risk postgres
bin/pgcheck wal_health
bin/pgcheck analyze_needed postgres
bin/pgcheck integer_pk_risk postgres
bin/pgcheck replication_slots
bin/pgcheck vacuum_queue postgres
bin/pgcheck io
bin/pgcheck temp_files
bin/pgcheck table_bloat postgres --show-sql
bin/pgcheck wraparound_risk postgres --explain
bin/pgcheck wal_generate /var/lib/postgresql/data/pg_wal
```

Legacy command names such as `vacuum_need`, `analyze_need`, `index_low`, `index_null_frac`, `index_state`, `int_pk_risk`, `relation_bloat`, `xid_wraparound`, and `xmin_horizon` remain available as compatibility aliases.

## Focused Hotspot Checks

```bash
pgcheck query_hotspot postgres
pgcheck query_temp postgres
pgcheck table_hotspot postgres
pgcheck query_temp postgres --show-sql
pgcheck -x query_hotspot postgres
pgcheck -At query_temp postgres
pgcheck table_hotspot postgres --explain
```

`query_hotspot` ranks cumulative execution time, with calls, mean time, execution-time share (not CPU usage), and rows retrieved or affected per call. `query_temp` ranks cumulative temporary reads plus writes, not current disk usage. Both need a configured `pg_stat_statements` extension in the selected database. Its schema and installed fields are discovered automatically, including legacy extension SQL objects after an engine upgrade. Top-level statements are selected where supported; older definitions may include nested statements when tracking is `all`.

| Output field | Interpretation |
| --- | --- |
| `total_exec_ms` / `mean_exec_ms` | Cumulative / mean execution milliseconds; excludes separate planning time in modern extension definitions. Legacy `total_time` / `mean_time` are used when needed. |
| `exec_time_share_pct` | Execution-time share across all eligible entries in the selected database, before the top-10 limit; not just the ten displayed rows. |
| `rows_per_call` | Rows retrieved or affected divided by calls; not rows scanned by the execution plan. |
| `temp_read` / `temp_written` | Cumulative temporary blocks read / written multiplied by the server's `block_size`. |
| `temp_io_per_call` | `(read bytes + written bytes) / calls`; not peak temporary space per execution. |
| `temp_io_ms` | Cumulative temporary read plus write milliseconds, only when the fields exist and current `track_io_timing` is on. |

`stats_reset` is the extension-wide reset time and `stats_since` is the per-entry collection start where available. Missing timestamps stay unknown. `dealloc` counts entry eviction events, not failed SQL. Timing is unavailable when unsupported or disabled, and may cover less than the whole statistics window. Restricted roles may see counters without other users' SQL text/query IDs.

`table_hotspot` uses built-in statistics and prints separate read and update top-10 lists. Reads include heap, indexes and TOAST; they may hit the OS cache. Update details include HOT percentage, fillfactor, and cross-page updates on PG16+. Low HOT alone does not justify changing fillfactor. Counters may have been individually reset; these are cumulative rankings, not current rates. These optional checks do not add work or extension dependencies to `quick`.

| Output field | Interpretation |
| --- | --- |
| `bytes_read` / `read_share_pct` | Cumulative bytes read across heap, indexes and TOAST / share of eligible table reads before the top-10 limit. |
| `cache_hit_pct` | Hits divided by hits plus reads for those components; PostgreSQL buffer-cache hits, not OS-cache hits. |
| `total_size` | Current relation size including indexes and TOAST, not cumulative traffic. |
| `hot_update_pct` | HOT updates divided by all updates; the update list only includes tables with updates. |
| `newpage_updates` | Cross-page update count on PG16+; blank on older versions. |
| `fillfactor` | Current table storage setting, using 100 when no override exists; not measured page fullness. |

These are separate snapshots, not an atomic incident timeline. Table statistics require `track_counts`; empty results may mean no eligible activity or unavailable collection, not proof that the instance has no problem. `pg_stat_io`'s `temp relation` rows describe temporary tables, not sort/hash spill files. Use `query_temp` for cumulative spill traffic and `temp_files` for currently present files.

### Prerequisites And Troubleshooting

Ask the DBA to preserve existing preload entries when adding `pg_stat_statements` to `shared_preload_libraries`, restart the server when required, and install the extension in each database to inspect. On PG14+, query identifier computation must also be enabled, normally with `compute_query_id=auto`. Tracking must not be `none`. `pgcheck` does not perform these administrative changes. See the [official extension guide](https://www.postgresql.org/docs/18/pgstatstatements.html) for setup and the [statistics guide](https://www.postgresql.org/docs/18/monitoring-stats.html) for collection semantics.

| Symptom | What to check |
| --- | --- |
| Extension not installed | Have the DBA run `CREATE EXTENSION pg_stat_statements` in the selected database after configuring the module. |
| Extension not loaded | Check the server's preload configuration and restart status; creating the extension alone is insufficient. |
| Tracking disabled | Review `pg_stat_statements.track`; the tool does not enable it automatically. |
| SQL text or query IDs unavailable | Review visibility privileges; `pg_read_all_stats` permits access to other users' query information. |
| Permission denied | Review the exact extension-schema and view/function privileges in the error; do not blindly grant superuser. |
| No temporary-I/O rows or blank timing | Check workload history, statistics resets, available fields, and timing configuration; do not equate blank with zero. |

`--explain` describes the logic without connecting. `--show-sql` connects to detect the server version and, for statement checks, the extension schema/fields; it renders the inspection SQL without running it. Export SQL containing internal placeholders this way before standalone execution.

### Maintenance And I/O Corrections

`vacuum_needed` and `vacuum_queue` share effective global/table settings, including insert triggers on PG13+ and the maximum dead-tuple threshold on PG18+. Queue rows are estimated candidates, not the launcher's actual queue. VACUUM progress and queue checks require PG12+; use `vacuum_needed` on PG11. SQL containing internal placeholders should be exported with `--show-sql` before standalone execution.

I/O totals handle unsupported NULL counters without discarding supported ones; raw unavailable counters stay blank. `io_hotspot.total_ops` means reads + writes + extends, not evictions or fsyncs. Checkpoint averages use the statistics reset window and avoid division by zero; these averages cannot identify a current spike. Freeze-triggered maintenance is separate from ordinary VACUUM thresholds: use `wraparound_risk` and `xmin_blockers` as needed.

## Project Layout

```text
.
├── main.go                 Go entrypoint and SQL embedding
├── internal/
│   ├── app/                CLI commands, version-aware checks, command execution flow
│   ├── pgexec/             psql runner and PostgreSQL server version detection
│   └── queries/            embedded SQL loader and small templating helpers
├── SQL/                    inspection SQL and shared query fragments
├── scripts/test-postgres.sh disposable PostgreSQL integration runner
├── tests/                  test instructions and verification record
├── THIRD_PARTY_NOTICES.md   upstream SQL attribution and license
├── pgcheck.example.json    example configuration file
├── pgcheck.sh              legacy Bash implementation
└── README.md
```

## Design Notes

`pgcheck` uses `psql` automatically when it is available. This preserves standard PostgreSQL behavior, including `.pgpass`, service files, SSL options, psql formatting, and existing environment variables. If `psql` is not found, `pgcheck` falls back to its native Go backend based on `database/sql` and `github.com/lib/pq`.

Several checks are inspired by practical recipes from [postgres-howto in Chinese](https://postgres-howto.cn/#/README), especially [replication lag](https://postgres-howto.cn/#/./docs/17), [pg_wal growth](https://postgres-howto.cn/#/./docs/31), [wraparound risk](https://postgres-howto.cn/#/./docs/44), [xmin horizon](https://postgres-howto.cn/#/./docs/45), [autovacuum queue and progress](https://postgres-howto.cn/#/./docs/67), [unused indexes](https://postgres-howto.cn/#/./docs/75), [int4 PK capacity](https://postgres-howto.cn/#/./docs/80), and [ANALYZE/statistics](https://postgres-howto.cn/#/./docs/94). The SQL is kept as plain files under `SQL/` so it can be reviewed and improved independently.

The new statement and table hotspot SQL is adapted from [Fabio Telles Rodriguez's pg_scripts](https://github.com/fabiotr/pg_scripts). Source files record the pinned commit and modifications. See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) for attribution, diagnostic references, and the upstream BSD-3-Clause license. Include that notice when distributing binaries.

## Development

Run tests:

```bash
go test ./...
```

Run SQL-result, privilege, compatibility and psql/native integration tests against disposable local clusters (never a production database):

```bash
bash scripts/test-postgres.sh /path/to/postgresql-16/bin /path/to/postgresql-18/bin
```

Each installation must include `pg_stat_statements`. The script creates local-only clusters, tests preloaded and non-preloaded states, and stops/removes the clusters on exit. See [tests/README.md](tests/README.md) for coverage and CI details.

Format code:

```bash
gofmt -w main.go internal/**/*.go
```

Build:

```bash
make build
```

## Direction

The project will stay small. Future work should improve correctness, compatibility, packaging, and the quality of a few high-value DBA checks rather than turning `pgcheck` into a heavy observability platform.

- Maintain automated PostgreSQL 11-18 SQL and compatibility tests.
- Keep refining high-signal SQL checks and permission-friendly fallback messages.
- Publish simple Linux/macOS release artifacts for amd64 and arm64.

## License

pgcheck's own code uses Apache License 2.0; see [LICENSE](LICENSE). Adapted SQL retains its upstream license; include [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) in source and binary distributions.

## 中文

`pgcheck` 是一款轻量级 PostgreSQL DBA 小助手，目标是快速回答一线排障中最常见的问题：数据库是不是被锁住了，是否存在长事务，VACUUM 是否落后，复制槽是否保留了太多 WAL，临时文件是否异常增长，以及下一步应该看哪个检查项。

它不是一个监控平台，也不追求把所有能力都塞进来。`pgcheck` 按需执行只读巡检 SQL，输出尽量贴近 `psql`，并通过 `--show-sql` 和 `--explain` 让 DBA 可以随时确认检查逻辑。

这个项目最早是一个 Bash 编写的一键巡检脚本。当前版本已经重构为更紧凑的 Go CLI：SQL 资源会被嵌入二进制，命令通过注册表管理，版本判断基于 PostgreSQL 服务端版本，并提供更清晰的兼容策略。

## 亮点

- 从 `pgcheck quick <database>` 开始，快速得到一组高频 DBA 检查结果。
- 遇到具体问题后，再使用 `lock_tree`、`xmin_blockers`、`vacuum_queue`、`wal_health`、`temp_files` 等命令下钻。
- 使用 SQL 耗时、SQL 临时 I/O、表级读取与更新热点三个入口定位具体对象。
- 可用时复用 PostgreSQL 标准 `psql` 行为，包括 `.pgpass`、service file、SSL 参数和常见输出选项。
- 检测 PostgreSQL 服务端版本，并为需要兼容的检查选择对应 SQL。
- SQL 仍然以普通文件形式保留在 `SQL/` 目录，方便 DBA 审阅、复用和继续沉淀。

## 快速开始

```bash
make build
cp pgcheck.example.json pgcheck.json
chmod 600 pgcheck.json
```

将 `pgcheck.json` 中的 `connection` 修改为目标实例，然后执行：

```bash
bin/pgcheck privilege
bin/pgcheck quick postgres
```

将 `postgres` 替换为目标数据库名。工具会自动发现当前目录的配置；连接其他实例时，可在命令前指定 `-c /path/to/pgcheck.json`。不要将凭据提交到仓库。

| 想回答的问题 | 下一步命令 |
| --- | --- |
| 谁在阻塞其他会话？ | `pgcheck lock_tree postgres` |
| 哪些 SQL 累计执行耗时最多？ | `pgcheck query_hotspot postgres` |
| 哪些 SQL 累计临时读写最多？ | `pgcheck query_temp postgres` |
| 哪些表累计读取或更新最多？ | `pgcheck table_hotspot postgres` |
| 哪些会话当前存在临时文件？ | `pgcheck temp_files` |
| 哪类后台进程贡献了 I/O？ | `pgcheck io_hotspot`（PG16+） |
| 普通 VACUUM 是否跟不上？ | `pgcheck vacuum_needed postgres` |

两个 SQL 级命令需要 `pg_stat_statements`；`quick` 和 `table_hotspot` 不依赖该扩展。

## 兼容性

原始工具主要测试到 PostgreSQL 15。Go 重构版本为 PostgreSQL 11-18 提供版本感知检查。本次新增检查及回归测试已在真实 PostgreSQL 11.22、12.22、13.23、14.5、15.14、16.14、17.6、18.4 实例上通过。具体覆盖和边界见[验证记录](tests/VERIFICATION.md)，不代表全部命令和所有小版本均已验证。

PostgreSQL 17+ 对部分统计视图做了调整，例如 checkpoint 和 VACUUM progress 相关视图，工具提供对应版本的 SQL。`io`、`io_hotspot` 需要 PG16+，并兼容 PG18 的字节计数字段和 WAL 行。VACUUM 进度及队列命令需要 PG12+，PG11 可使用 `vacuum_needed`。`wal_generate` 依赖服务端 `pg_wal` 物理路径，仍需按部署单独验证。工具兼容旧主版本，不代表 PostgreSQL 官方仍在维护该版本。

## 环境要求

- 从源码构建需要 Go 1.23+。
- 推荐安装 PostgreSQL 客户端工具并确保 `psql` 在 `PATH` 中。如果没有 `psql`，`pgcheck` 会自动回退到原生 Go driver。
- 巡检用户需要有读取相关系统视图和统计视图的权限。

`pgcheck` 会尽量保持命令行简单。连接、展示和 psql 行为统一放在 JSON 配置文件里；环境变量仍可作为快速本地使用时的默认值。

如果不想 export 一堆环境变量，可以使用配置文件：

```bash
cp pgcheck.example.json pgcheck.json
bin/pgcheck --config pgcheck.json dbstatus
bin/pgcheck quick postgres
bin/pgcheck -c pgcheck.json connections postgres
```

配置结构：

```json
{
  "_comment": "以 _comment 开头的字段仅用于说明，pgcheck 会忽略它们。",
  "connection": {
    "_comment": "等价于 PGHOST、PGPORT、PGUSER、PGPASSWORD、PGDATABASE 和 PGSSLMODE。",
    "host": "127.0.0.1",
    "port": "5432",
    "user": "postgres",
    "password": "",
    "database": "postgres",
    "sslmode": "disable"
  },
  "psql": {
    "_comment": "这些选项和 psql 命令行参数保持一致，主要影响 psql 后端。",
    "path": "psql",
    "_quiet": "quiet=true 等价于 psql -q：减少非数据类输出。",
    "quiet": true,
    "_tuples_only": "tuples_only=true 等价于 psql -t：只输出行数据，不输出表头和页脚。",
    "tuples_only": false,
    "_no_align": "no_align=true 等价于 psql -A：使用非对齐输出。",
    "no_align": false,
    "_no_psqlrc": "no_psqlrc=true 等价于 psql -X：不读取 ~/.psqlrc，让输出更稳定。",
    "no_psqlrc": true,
    "_extra_args": "extra_args 会直接透传给 psql，例如 [\"--csv\"] 或 [\"--set\", \"ON_ERROR_STOP=1\"]。",
    "extra_args": []
  },
  "output": {
    "_expanded": "auto 保留命令默认展示；table 强制普通表格；expanded 强制 psql -x 风格展示。",
    "expanded": "auto"
  }
}
```

也可以继续使用环境变量：

```bash
export PGHOST=127.0.0.1
export PGPORT=5432
export PGUSER=postgres
export PGPASSWORD=secret
```

如果 `psql` 不在 `PATH` 中，可以在配置文件里设置 `psql.path`。如果仍然找不到 `psql`，`pgcheck` 会自动使用原生 Go driver。

常见 psql 展示选项默认值可以在 `pgcheck.json` 中配置。如果只是临时执行一次，也可以在命令前使用 `-x`、`-A`、`-t`，含义和 `psql` 一致：

```bash
bin/pgcheck -x lock postgres
bin/pgcheck -At dbstatus
```

给熟悉 `psql` 的用户看的对应关系：

| 配置项 | psql 参数 | 含义 |
| --- | --- | --- |
| `quiet` | `-q` | 减少 `psql` 的非数据类输出。 |
| `tuples_only` | `-t` | 只输出行数据，不输出列名和页脚。 |
| `no_align` | `-A` | 使用非对齐输出，而不是补齐宽度的表格输出。 |
| `no_psqlrc` | `-X` | 不读取 `~/.psqlrc`，避免本地 psql 配置影响巡检输出。 |
| `extra_args` | 透传 | 直接追加原始 psql 参数，例如 `--csv` 或 `--set name=value`。 |

## 构建

```bash
make build
```

`make build` 会写入版本号、Git 提交及 UTC 构建时间。开发时也可以直接执行 `go build -o bin/pgcheck .`，但不会注入相同的构建信息。反馈问题时，请附上完整版本输出、PostgreSQL 服务端版本和执行命令，并移除凭据及敏感 SQL。

查看帮助：

```bash
bin/pgcheck help
```

查看版本：

```bash
bin/pgcheck version
```

## 命令列表

```text
pgcheck relation <database> <schema>         查看指定 schema 下表和索引大小
pgcheck relconstraint <database> <relation>  查看指定表的约束和多列索引
pgcheck alltoast <database> <schema>         查看指定 schema 下的 TOAST 表
pgcheck reltoast <database> <relation>       查看指定表的 TOAST 相关列和 TOAST 表信息
pgcheck analyze_needed <database>            查看需要 ANALYZE 的表
pgcheck dbstatus                             查看数据库整体状态
pgcheck quick <database>                     执行一组轻量的 DBA 快速检查
pgcheck query_hotspot <database>             查看累计执行耗时最高的前 10 条 SQL
pgcheck query_temp <database>                查看累计临时 I/O 最高的前 10 条 SQL
pgcheck table_hotspot <database>             分别查看读取和更新最多的前 10 张表
pgcheck index_bloat <database>               估算 btree 索引膨胀
pgcheck index_duplicate <database>           查找重复索引
pgcheck index_efficiency <database>          查找低效索引
pgcheck index_null_risk <database>           查找高 NULL 比例列上的大索引
pgcheck index_health <database>              查看索引详情和异常索引
pgcheck integer_pk_risk <database>           查看 int2/int4 主键容量风险
pgcheck io                                   查看 pg_stat_io 按 backend/object 的 I/O 分布
pgcheck io_hotspot                           查看 pg_stat_io I/O 热点
pgcheck unused_indexes <database>            查找未使用和低频使用索引
pgcheck lock <database>                      查看锁等待和阻塞队列
pgcheck lock_tree <database>                 查看递归阻塞树和根阻塞会话
pgcheck checkpoint                           查看后台写进程和检查点统计
pgcheck freeze <database>                    查看事务 ID 消耗和 freeze 风险
pgcheck replication                          查看物理流复制状态
pgcheck replication_slots                    查看复制槽 xmin 和 WAL 保留
pgcheck connections <database>               查看连接汇总和当前查询
pgcheck long_transaction <database>          查看长事务
pgcheck privilege                            查看当前监控权限
pgcheck table_bloat <database>               估算表膨胀并查看 VACUUM 阻塞信息
pgcheck temp_files                           查看正在使用临时文件的会话
pgcheck vacuum_state <database>              查看正在运行的 VACUUM 进度
pgcheck vacuum_needed <database>             查看可能需要 VACUUM 的表
pgcheck vacuum_queue <database>              查看 VACUUM 候选及执行状态（PG12+）
pgcheck index_create <database>              查看 CREATE INDEX 进度
pgcheck wal_archive                          查看 WAL 归档状态
pgcheck wal_generate <wal_path>              基于 pg_wal 目录估算 WAL 生成速度
pgcheck wal_health                           查看 WAL 保留和归档健康状态
pgcheck wait_event <database>                查看等待事件和等待类型
pgcheck partition <database>                 查看分区表信息
pgcheck object <database> <user>             查看用户拥有的对象和角色成员关系
pgcheck wraparound_risk <database>           查看 XID 和 MultiXID 回卷风险
pgcheck xmin_blockers                        查看全局 xmin horizon 阻塞来源
```

## 示例

```bash
bin/pgcheck dbstatus
bin/pgcheck quick postgres
bin/pgcheck query_hotspot postgres
bin/pgcheck query_temp postgres
bin/pgcheck table_hotspot postgres
bin/pgcheck -x lock postgres
bin/pgcheck -At dbstatus
bin/pgcheck --config pgcheck.json dbstatus
bin/pgcheck connections postgres
bin/pgcheck lock postgres
bin/pgcheck lock_tree postgres
bin/pgcheck freeze postgres
bin/pgcheck relation postgres public
bin/pgcheck index_bloat postgres
bin/pgcheck xmin_blockers
bin/pgcheck wraparound_risk postgres
bin/pgcheck wal_health
bin/pgcheck analyze_needed postgres
bin/pgcheck integer_pk_risk postgres
bin/pgcheck replication_slots
bin/pgcheck vacuum_queue postgres
bin/pgcheck io
bin/pgcheck temp_files
bin/pgcheck table_bloat postgres --show-sql
bin/pgcheck wraparound_risk postgres --explain
bin/pgcheck wal_generate /var/lib/postgresql/data/pg_wal
```

旧命令名仍保留为兼容别名，例如 `vacuum_need`、`analyze_need`、`index_low`、`index_null_frac`、`index_state`、`int_pk_risk`、`relation_bloat`、`xid_wraparound` 和 `xmin_horizon`。

## 热点检查

```bash
pgcheck query_hotspot postgres
pgcheck query_temp postgres
pgcheck table_hotspot postgres
pgcheck query_temp postgres --show-sql
pgcheck -x query_hotspot postgres
pgcheck -At query_temp postgres
pgcheck table_hotspot postgres --explain
```

`query_hotspot` 按累计执行耗时排序，展示调用次数、平均耗时、执行耗时占比（不是 CPU 使用率）和每次调用读取或影响的行数。`query_temp` 按累计临时读写量排序，读写流量不等于当前文件占用。两者需要目标数据库配置 `pg_stat_statements`，会自动识别扩展 schema 和实际字段，也兼容服务端升级但扩展 SQL 对象未升级的情况。有对应字段时只统计顶层语句；旧扩展在 track=all 时可能包含嵌套语句。

| 输出字段 | 如何理解 |
| --- | --- |
| `total_exec_ms` / `mean_exec_ms` | 累计 / 平均执行毫秒数；新扩展定义下不含独立的计划耗时。旧扩展按实际字段使用 `total_time` / `mean_time`。 |
| `exec_time_share_pct` | 占当前数据库全部符合条件条目的执行耗时比例，在截取前 10 条之前计算，不是只在展示行中计算。 |
| `rows_per_call` | 返回或影响的行数除以调用次数，不是执行计划扫描的行数。 |
| `temp_read` / `temp_written` | 累计临时读 / 写块数乘以服务端 `block_size`。 |
| `temp_io_per_call` | （读取字节 + 写入字节）/ 调用次数，不是单次执行的临时空间峰值。 |
| `temp_io_ms` | 累计临时读写毫秒数，仅在字段存在且当前 `track_io_timing` 开启时显示。 |

`stats_reset` 是扩展整体重置时间，`stats_since` 是支持该字段时的单条记录统计起点；未知时间保留为空。`dealloc` 是统计条目淘汰事件次数，不是 SQL 失败次数。未提供字段或未启用计时时，不显示临时 I/O 耗时。受限角色可能看到统计值，但无法看到其他用户的 SQL 文本和 queryid。

`table_hotspot` 使用内置统计视图，分别列出读取热点和更新热点。读取包括 heap、索引和 TOAST，仍可能命中 OS 缓存；更新部分包含 HOT 比例、fillfactor，以及 PG16+ 的跨页更新次数。低 HOT 比例不能单独作为调整 fillfactor 的依据。计数可能被单表重置，这些排行是累计值而非当前速率。三个命令按需执行，不会给 `quick` 增加扩展依赖或额外扫描。

| 输出字段 | 如何理解 |
| --- | --- |
| `bytes_read` / `read_share_pct` | heap、索引及 TOAST 的累计读取字节 / 在截取前 10 张表之前计算的符合条件表读取占比。 |
| `cache_hit_pct` | 上述组成部分的命中数 /（命中数 + 读取数），指 PostgreSQL 缓冲区命中，不是 OS 缓存命中。 |
| `total_size` | 当前包含索引和 TOAST 的表总大小，不是累计读写流量。 |
| `hot_update_pct` | HOT 更新数 / 总更新数；更新榜只包含发生过更新的表。 |
| `newpage_updates` | PG16+ 的跨页更新次数，旧版本为空。 |
| `fillfactor` | 当前表存储参数，未覆盖时为 100，不是实测页面填充率。 |

各列表是独立快照，并非同一个时点的原子事件记录。表统计依赖 `track_counts`；空结果可能表示没有符合条件的活动或未采集统计，不等于实例没有问题。`pg_stat_io` 的 `temp relation` 指临时表，不是排序或哈希溢出文件：累计落盘流量看 `query_temp`，当前存在的文件看 `temp_files`。

### 前置条件与排查

请由 DBA 在保留已有预加载项的前提下，将 `pg_stat_statements` 加入 `shared_preload_libraries`，按需重启服务，并在每个待检查数据库安装扩展。PG14+ 还需要启用 query ID 计算，通常使用 `compute_query_id=auto`；`pg_stat_statements.track` 不能为 `none`。工具不会自动执行这些管理操作。配置细节见[官方扩展说明](https://www.postgresql.org/docs/18/pgstatstatements.html)，统计口径见[官方统计说明](https://www.postgresql.org/docs/18/monitoring-stats.html)。

| 现象 | 检查方向 |
| --- | --- |
| 扩展未安装 | 模块配置完成后，由 DBA 在目标数据库执行 `CREATE EXTENSION pg_stat_statements`。 |
| 扩展未加载 | 检查预加载配置及重启状态，仅创建扩展并不足够。 |
| 语句采集关闭 | 检查 `pg_stat_statements.track`，工具不会自动开启采集。 |
| SQL 文本或 queryid 不可见 | 检查统计可见性；`pg_read_all_stats` 允许查看其他用户的查询信息。 |
| 权限不足 | 按实际报错核对扩展 schema、视图及函数权限，不要直接授予超级用户。 |
| 没有临时 I/O 行或耗时为空 | 检查历史负载、统计重置、字段支持及计时设置，不要把空值当成零。 |

`--explain` 无需连接即可解释检查逻辑。`--show-sql` 仍需连接，以识别服务端版本及 SQL 级检查的扩展 schema/字段；随后输出巡检 SQL，不执行该巡检查询。包含内部占位符的 SQL，应先用它导出完整查询，再独立执行。

### 维护与 I/O 修正

`vacuum_needed` 和 `vacuum_queue` 共用有效全局/表级配置，包括 PG13+ 的插入触发条件和 PG18+ 的死元组阈值上限。队列是候选估算，不是调度器的内部队列。VACUUM 进度和队列命令需要 PG12+；PG11 可以使用 `vacuum_needed`。包含内部占位符的 SQL，应先使用 `--show-sql` 导出完整查询，再独立执行。

I/O 汇总对不支持的 NULL 计数进行安全处理，避免丢失已有计数，原始不可用字段仍保留为空。`io_hotspot.total_ops` 仅为 reads + writes + extends，不包括 evictions 和 fsyncs。checkpoint 平均值按统计重置窗口计算并避免除零，不能据此判定当前尖刺。freeze 触发的维护不同于普通 VACUUM 阈值，需要时结合 `wraparound_risk`、`xmin_blockers` 判断。

## 项目结构

```text
.
├── main.go                 Go 入口和 SQL 嵌入
├── internal/
│   ├── app/                CLI 命令、版本兼容逻辑和执行流程
│   ├── pgexec/             psql 执行器和服务端版本检测
│   └── queries/            嵌入式 SQL 加载和轻量模板处理
├── SQL/                    巡检 SQL 和共享查询片段
├── scripts/test-postgres.sh 临时 PostgreSQL 集成测试入口
├── tests/                  测试说明和验证记录
├── THIRD_PARTY_NOTICES.md   上游 SQL 来源和许可声明
├── pgcheck.example.json    配置文件示例
├── pgcheck.sh              旧版 Bash 实现
└── README.md
```

## 设计说明

`pgcheck` 会在可用时自动使用 `psql`，这样可以继承 `.pgpass`、service file、SSL 参数、psql 展示格式和环境变量等 PostgreSQL 标准能力。如果找不到 `psql`，则自动回退到基于 `database/sql` 和 `github.com/lib/pq` 的原生 Go 后端。

部分巡检项来自 [postgres-howto in Chinese](https://postgres-howto.cn/#/README) 中的实践思路，尤其是 [replication lag](https://postgres-howto.cn/#/./docs/17)、[pg_wal growth](https://postgres-howto.cn/#/./docs/31)、[wraparound risk](https://postgres-howto.cn/#/./docs/44)、[xmin horizon](https://postgres-howto.cn/#/./docs/45)、[autovacuum queue and progress](https://postgres-howto.cn/#/./docs/67)、[unused indexes](https://postgres-howto.cn/#/./docs/75)、[int4 PK capacity](https://postgres-howto.cn/#/./docs/80) 和 [ANALYZE/statistics](https://postgres-howto.cn/#/./docs/94)。这些 SQL 都保留为 `SQL/` 目录下的普通文件，方便审阅和继续演进。

新增热点 SQL 改编自 [Fabio Telles Rodriguez 的 pg_scripts](https://github.com/fabiotr/pg_scripts)。SQL 文件头记录固定提交、来源和修改；[THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md) 保留完整版权、BSD-3-Clause 许可及两份演讲资料的参考说明。分发二进制时也应附带该声明。

## 开发

运行测试：

```bash
go test ./...
```

格式化：

```bash
gofmt -w main.go internal/**/*.go
```

完整 SQL、权限、版本和两种执行后端的测试使用临时数据库实例：

```bash
bash scripts/test-postgres.sh /path/to/postgresql-16/bin /path/to/postgresql-18/bin
```

每套安装需包含 `pg_stat_statements`。脚本仅创建本地临时实例，验证预加载及未预加载状态，并在退出时停止、清理实例，不连接业务数据库。覆盖范围和 CI 说明见 [tests/README.md](tests/README.md)。

构建：

```bash
make build
```

## 后续方向

项目会继续保持轻量。后续更应该投入在正确性、兼容性、打包发布和少数高价值 DBA 检查项的质量上，而不是把 `pgcheck` 做成一个很重的观测平台。

- 持续维护 PostgreSQL 11-18 的 SQL 正确性与兼容性自动测试。
- 持续优化高信号 SQL 检查和权限不足时的降级提示。
- 发布简单可用的 Linux/macOS amd64/arm64 构建产物。

## License

pgcheck 自身代码采用 Apache License 2.0，见 [LICENSE](LICENSE)。改编 SQL 保留上游许可，源码及二进制分发均需附带 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。
