# Hotspot Checks Verification

Date: 2026-10-25. Covers `query_hotspot`, `query_temp`, and `table_hotspot`,
introduced in commit `f236bc3`, together with the related correctness fixes.
This is a development verification record, not a release or deployment record.

## Executed Checks

All entries below passed locally on macOS ARM64 with Go 1.23.3. Database tests
used real, isolated clusters, not a public or production instance.

| PostgreSQL | Installation | SQL, CLI, and extension-state tests |
| --- | --- | --- |
| 11.22 | Official source build | Passed |
| 12.22 | Official source build | Passed |
| 13.23 | Official source build | Passed |
| 14.5 | Official source build | Passed |
| 15.14 | Official source build | Passed |
| 16.14 | Homebrew | Passed |
| 17.6 | Official source build | Passed |
| 18.4 | Homebrew | Passed |

Each installation included `pg_stat_statements`. The same suite ran against each
engine, followed by a restart without preloading to verify the diagnostic path.
PG14.5 was included specifically because an earlier reader reported a failure
on that version; the regression set includes `wraparound_risk`.

Additional successful checks:

- `go test -race ./...` and `go vet ./...`.
- `gofmt` inspection, `git diff --check`, and `bash -n scripts/test-postgres.sh`.
- `make build`, built-binary help/version smoke checks.
- `CGO_ENABLED=0` cross-compilation for Linux amd64 and arm64.

## Assertions

- Statement rankings: current-database scope, optional top-level filtering,
  cumulative execution shares, row averages, zero calls, top-10 limits, legacy
  extension fields, extreme counters, and unavailable optional fields.
- Temporary I/O: read/write bytes, per-call arithmetic, timing on/off, and a real
  sort workload that spills to disk.
- Table hotspots: heap/index/TOAST aggregation, NULL counters, HOT percentage,
  fillfactor, and version-dependent cross-page update counters.
- Maintenance: effective table/global VACUUM thresholds, disabled autovacuum,
  TOAST inheritance and overrides, insert-trigger settings, PG18 threshold caps,
  and consistent calculations in `vacuum_needed` and `vacuum_queue`.
- Existing diagnostics: NULL-safe I/O totals, pre-/post-PG18 I/O layouts,
  checkpoint reset windows and zero denominators, plus regression execution of
  database status, privileges, wraparound, xmin, replication slots, connections,
  lock trees, waits, ANALYZE checks, and `quick`.
- Execution consistency: psql/native backends, superuser/pg_monitor/ordinary
  roles, `-x`/`-A`/`-t` combinations, metadata parsing with custom psql options,
  explicit database selection, SQL rendering, offline explanations, help,
  bilingual command documentation, and source attribution.
- Extension diagnostics: special schema names, old extension definitions and
  upgrades, reset statistics, missing extension, disabled tracking, missing
  preload, and actual permission failures distinguished from other SQL errors.

See [README.md](README.md) for reproduction and the test safety guards. Controlled
statistics fixtures assert exact results using PostgreSQL's SQL evaluator;
separate real workloads validate catalog and extension integration.

## Documentation Follow-Up

The README/help synchronization after `f236bc3` was verified on 2026-10-25:

- Repeated the full local eight-version database matrix above successfully.
- Repeated race unit tests, static checks, build, and help smoke checks.
- Repeated Linux amd64/arm64 cross-compilation; this remains build-only evidence.
- Expanded consistency assertions to all visible command usages and 15 hotspot
  field names in both README languages, in addition to existing source credits.

Only documentation, help text, and documentation checks changed in this follow-up;
inspection SQL and execution logic were not modified.

## Limits

- One representative minor version per major was exercised, not every minor.
- No production-sized catalog/load benchmark, standby, managed-service variant,
  or full test of every existing command was performed.
- Linux binaries were cross-compiled, not executed locally. Remote Docker CI
  results are not included in this local record; consult the actual workflow run.
- Unit race checks ran locally; the local database matrix ran without the race
  flag. The remote workflow includes it for database tests as well.
- Cumulative counters are not instantaneous rates, device IOPS, or guaranteed
  performance diagnoses. Real-world results still depend on statistics age,
  visibility privileges, extension tracking, and workload.
- No automatic extension creation, configuration changes, statistics resets,
  commits, pushes, or deployments were performed against business environments.

## 中文摘要

本轮已在上述八个真实 PostgreSQL 版本通过 SQL 数值断言、实际负载、权限、
扩展状态、双后端、展示参数和相关老功能回归。测试实例均为本地独立临时实例。
同时通过竞态单测、静态检查和 Linux 双架构交叉编译。

该记录不意味着所有小版本、全部历史命令、托管数据库、备库及生产规模均已验证。
本记录不包含远端 CI 结果，需查看实际运行记录；Linux 运行时仍需由 CI 或实际部署
环境确认，本轮未发布上线。
