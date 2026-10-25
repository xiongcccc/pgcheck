# PostgreSQL Verification

Run unit tests with `go test ./...`. Database tests are skipped unless explicitly
enabled by the integration runner. No test requires a public or production database.

The [verification record](VERIFICATION.md) lists the locally tested engine versions,
results, and limits. A successful plain `go test ./...` run without test database
variables is not evidence that database compatibility tests ran.

```bash
go test -race -count=1 ./...
go vet ./...
```

For SQL and CLI integration tests:

```bash
bash scripts/test-postgres.sh /path/to/postgresql/bin [...additional bin directories]
```

The runner requires Go and PostgreSQL installations containing `pg_stat_statements`.
It creates a fresh UTF-8 cluster with Unix-socket-only connections in `/private/tmp`,
uses the `pgcheck_test` role and `pgcheck-integration` cluster marker, builds a test
binary, and removes the cluster afterwards. Integration tests refuse to mutate an
instance without those markers. The tests intentionally change statistics, create
roles/objects, and exercise extension install/update states in these disposable clusters.

## Coverage

- SQL result assertions for execution-time share, row averages, temporary bytes,
  legacy/current statement fields, database and top-level filtering, and zero calls.
- Heap/index/TOAST read arithmetic, NULL index counters, HOT ratios and fillfactor.
- Effective global/table VACUUM thresholds, disabled tables, PG13+ insert triggers,
  PG18+ caps, and consistency between the need and queue commands.
- Pre-PG18 and PG18 I/O layouts, write-only/NULL counters, and separate eviction counts.
- Zero-checkpoint denominators and statistics reset windows.
- Real statement/table workloads with `pg_stat_statements` outside `public`.
- Superuser, `pg_monitor`, and ordinary roles through both psql and native execution.
- Legacy extension definitions on newer engines, extension update, reset, missing
  extension, and installed-but-not-preloaded diagnostics.
- `-x`, `-A`, `-t`, combined flags, SQL trace/source comments, offline explanation,
  argument validation, and internal metadata under CSV/expanded psql settings.
- Regression execution of database status, privilege, wraparound, xmin, replication
  slots, connections, locks, waits and ANALYZE checks on every tested engine.
- Every visible command's usage and hotspot metric descriptions in both README
  languages, with pinned SQL attribution and third-party notice coverage.

Fixtures with controlled statistics test arithmetic deterministically; real workload
tests additionally validate catalog integration. Top-N output is not a guarantee of
constant query cost on arbitrarily large catalogs or `pg_stat_statements` settings.

## CI

The PostgreSQL workflow runs the same Go test suite against official PostgreSQL
11-18 containers, separately from unit/static checks. It tests the normal preloaded
state and a second cluster without preloading. Workflow definitions are repeatable
verification infrastructure, not evidence that a particular remote run has passed.

## 中文

实际通过版本、结果和边界见[验证记录](VERIFICATION.md)。没有指定测试数据库时，
普通 `go test ./...` 会跳过数据库测试，不能据此声称已通过兼容性验证。

```bash
go test -race -count=1 ./...
go vet ./...
bash scripts/test-postgres.sh /path/to/postgresql-16/bin /path/to/postgresql-18/bin
```

数据库测试需要包含 `pg_stat_statements` 的 PostgreSQL 安装，并以非 root 用户运行。
脚本在 `/private/tmp` 创建 UTF-8 临时实例，仅允许 Unix socket 连接，
使用 `pgcheck_test` 角色和 `pgcheck-integration` 标记，并在退出时停止、清理实例。
测试会校验这些标记，所有重置统计、角色及扩展操作仅限测试实例。

测试覆盖 SQL 数值、权限、旧扩展、版本分支、错误提示、psql/原生后端及展示参数；
可控统计夹具用于确定性断言，真实负载用于核对系统目录及扩展集成。
同时检查全部可见命令用法及热点指标说明是否在中英 README 中齐全，以及上游来源声明。
小型夹具的执行时间不代表生产规模性能，Top-N 也不代表查询只扫描十条统计记录。

CI 使用官方 PG11-18 容器，分别测试预加载和未预加载状态。
CI 配置不等于远端执行已成功，实际结果仍以相应运行记录为准。
