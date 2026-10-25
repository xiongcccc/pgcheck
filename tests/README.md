# PostgreSQL Verification

Run unit tests with `go test ./...`. Database tests are skipped unless explicitly
enabled by the integration runner. No test requires a public or production database.

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

Fixtures with controlled statistics test arithmetic deterministically; real workload
tests additionally validate catalog integration. Top-N output is not a guarantee of
constant query cost on arbitrarily large catalogs or `pg_stat_statements` settings.

## CI

The PostgreSQL workflow runs the same Go test suite against official PostgreSQL
11-18 containers, separately from unit/static checks. It tests the normal preloaded
state and a second cluster without preloading. Workflow definitions are repeatable
verification infrastructure, not evidence that a particular remote run has passed.

## 中文

测试使用独立临时实例，覆盖 SQL 数值、权限、旧扩展、版本分支、错误提示、
psql/原生后端及展示参数。所有重置统计、角色和扩展操作仅限测试实例。
可控统计夹具用于确定性计算断言，真实负载用于核对系统目录及扩展集成。
CI 配置提供 PG11-18 的持续测试；实际通过版本以对应执行记录为准。
