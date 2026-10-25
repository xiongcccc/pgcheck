# Third-Party Notices

## pg_scripts

The following SQL was adapted from [fabiotr/pg_scripts](https://github.com/fabiotr/pg_scripts)
at commit `c35755bb7a464ffdbbb36f0339ec8d0a9d0865ad`:

| pgcheck file | Upstream files |
| --- | --- |
| `SQL/query_hotspot.sql` | `sql/statements_exec_17up.sql` |
| `SQL/query_temp.sql` | `sql/statements_temp_17up.sql` |
| `SQL/table_hotspot.sql` | `sql/io_table_heap_91up.sql`, `sql/io_table_index_91up.sql` |
| `SQL/table_updates.sql` | `sql/tables_changes_91up.sql`, `sql/fillfactor_93up.sql` |

Changes include cumulative rather than daily reporting, guarded numeric arithmetic,
schema/capability detection, top-level statement filtering where available, optional
version-specific fields, and combining heap/index/TOAST reads. Source comments in
each file link to the pinned upstream version. These files retain the following
upstream license; pgcheck's own license does not replace it. Distributions containing
these queries, including compiled binaries, must include this notice.

### BSD 3-Clause License

Copyright (c) 2023, Fábio Telles Rodriguez
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

## Diagnostic References

The interpretation of cumulative counters, I/O contexts, HOT updates and maintenance
statistics also draws on these presentations, referenced as diagnostic guidance:

- Fabio Telles Rodriguez, [Metricas e ajustes no PostgreSQL](https://speakerdeck.com/fabiotr/metricas-e-ajustes-no-postgresql).
- Chirag Dave and Sami Imseih (AWS), *Becoming a PG_STAT_* STAR [Returns]*.
- [PostgreSQL statistics documentation](https://www.postgresql.org/docs/18/monitoring-stats.html).

Presentation text and images are not redistributed. Existing SQL source credits
remain in their respective files and in the README's postgres-howto references.
