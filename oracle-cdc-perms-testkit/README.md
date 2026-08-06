# Oracle CDC minimum-permissions test kit

The scripts and environment files used to validate which Oracle grants the
`oracledb_cdc` Redpanda Connect input actually needs for CDC streaming with
snapshotting disabled — and to confirm that, without table read privileges, an
accidentally-enabled snapshot fails loudly with zero rows read.

Everything here is what we actually ran (2026-07-29); the grant scripts for the extra
user profiles were dumped back out of the live test instance to make sure they match
what was tested. Test-only passwords throughout — nothing sensitive.

## Environment

- **Oracle**: Oracle Database Free 23ai, `container-registry.oracle.com/database/free:latest-lite`,
  in Docker, CDB mode. The connector connects to the CDB service `FREE` with
  `pdb_name: FREEPDB1`. Note the rig runs NOARCHIVELOG — streaming from online redo
  logs works fine for testing; ARCHIVELOG + retention is an operational requirement so
  history isn't lost if the connector falls behind, not a hard prerequisite.
- **DB-level config**: supplemental log data + PK logging at the database level, and
  `SUPPLEMENTAL LOG DATA (ALL) COLUMNS` on the source table (both applied by the setup
  scripts).
- **Source table**: `SPACE_APP.T_CHANNEL_DEF` (stand-in schema, 3 seed rows).
- **Redpanda Connect**: built from `main` at `017e01f9346d30a92f8948679371ef55b3521ac2`
  (post-4.102.0) via `go build ./cmd/redpanda-connect` in the
  [redpanda-data/connect](https://github.com/redpanda-data/connect) repo; released
  versions (4.93.0 / 4.97.0 / 4.100.0) taken from the GitHub releases page for the
  version-specific checks. `oracledb_cdc` needs an enterprise licence — the runs loaded
  it from the default path (`/etc/redpanda/redpanda.license`).

## Setup

```bash
./setup-oracle.sh
```

That starts the container (`oracle-cdc-perms`, port 1521), waits for healthy, then
applies:

| script | run as | what it creates |
|---|---|---|
| `sql/10-cdb-restricted-user.sql` | SYS, CDB$ROOT | `C##REDPANDA` — the restricted grant list (no `SELECT ANY TABLE`, no `LOCK ANY TABLE`) |
| `sql/11-pdb-source-schema.sql` | SYSTEM, FREEPDB1 | `SPACE_APP.T_CHANNEL_DEF` + supplemental logging + seed rows |

Two more profiles are optional (apply with `docker exec -i oracle-cdc-perms sqlplus -S / as sysdba < <file>`):

| script | user | profile |
|---|---|---|
| `sql/12-cdb-control-user.sql` | `C##CONTROL` | restricted list **plus** `SELECT ANY TABLE` + `LOCK ANY TABLE` restored (known-good baseline) |
| `sql/13-cdb-logminer-doc-user.sql` | `C##JOEDOC` | LogMiner-only route from the draft permissions doc (§2) + one per-table `FLASHBACK` object grant |

`sql/14-test-variations.sql` holds the per-test grant mutations (revoke
`FLASHBACK ANY TABLE`, swap in a per-table object grant, try `SELECT ANY DICTIONARY`,
…). It's a reference file of independent blocks — run one block per test, not the whole
file.

## Running the connector

```bash
redpanda-connect run pipelines/pipeline-restricted.yaml   # or rpk connect run, or a release binary
```

Generate change events by doing DML on the source table (as `space_app`), e.g.:

```sql
INSERT INTO space_app.t_channel_def VALUES (4, 'chan-d', 40);
UPDATE space_app.t_channel_def SET val = 99 WHERE id = 1;
DELETE FROM space_app.t_channel_def WHERE id = 2;
COMMIT;
```

Two config notes worth knowing:

- All pipelines set `logminer.min_scn_window_size: 0`. On an idle test DB the default
  (1000) defers mining until the SCN advances enough, which looks like a hang.
- Older configs use `stream_snapshot: false` (the 4.93-era field); newer ones use
  `snapshot_mode`. The files use whichever the binary under test expects.
- Pipelines use a **file** checkpoint cache rather than the default Oracle-based one, so
  grant testing isn't confounded by the connector creating its checkpoint table.

## Test matrix (pipeline file → profile → result)

| # | pipeline | user / profile | mode | result |
|---|---|---|---|---|
| 1 | `pipeline-restricted.yaml` | `C##REDPANDA` (restricted) | streaming only | **works** — insert/update/delete all captured |
| 2 | `pipeline-restricted-snap.yaml` | `C##REDPANDA` | snapshot_and_stream | **snapshot blocked loudly**: `ORA-41900`, 0 rows |
| 3 | `pipeline-restricted.yaml` | restricted **minus** `FLASHBACK ANY TABLE` (sql/14, block 1) | streaming only | **fails at startup**: "no user tables found for given include and exclude filters" |
| 4 | `pipeline-restricted.yaml` | no ANY grants; per-table `GRANT FLASHBACK ON` (sql/14, block 2) | streaming only | **works** |
| 4b | `pipeline-restricted-snap.yaml` | same as 4 | snapshot_and_stream | **snapshot blocked**: `ORA-41900`, 0 rows |
| 5 | `pipeline-restricted.yaml` | `SELECT ANY DICTIONARY` in place of `FLASHBACK ANY TABLE` (sql/14, block 3) | streaming only | **fails**: "no user tables found" — it doesn't restore `ALL_TABLES` visibility |
| C | `pipeline-control.yaml` | `C##CONTROL` (full) | streaming only | works (baseline) |
| C2 | `pipeline-control-snap.yaml` | `C##CONTROL` | snapshot_and_stream | full snapshot emitted (3 rows) |
| — | `pipeline-control-493.yaml` | `C##CONTROL`, run with the **4.93.0** binary | streaming only | fails even fully privileged: starts at SCN 0, loops on `ORA-01281` (fixed by #4509 in 4.97.0; 4.97.0 and 4.100.0 verified OK with the same config) |
| — | `pipeline-joedoc.yaml` | `C##JOEDOC` (doc route) | streaming only | works |
| — | `pipeline.yaml` | `C##REDPANDA` | streaming only | initial phase-1 config (4.93-era `stream_snapshot` field), superseded by `pipeline-restricted.yaml` |

## Key takeaways

1. `SELECT ANY TABLE` is **not** required for CDC streaming — the restricted list
   streams fine on ≥4.97.0.
2. What streaming *does* need is each monitored table being **visible in
   `ALL_TABLES`/`ALL_TAB_COLUMNS`** (startup verification + column typing), which
   requires holding *some* privilege on each table. In the restricted list that's
   `FLASHBACK ANY TABLE` — remove it and you get exactly the "no user tables found"
   failure.
3. The version-proof, least-capability option is a per-table
   `GRANT FLASHBACK ON <schema>.<table>` — object grants confer catalog visibility on
   every Oracle version and give no row-read ability, so snapshots stay blocked.
4. The snapshot tripwire is inherent: without read privileges on the source tables, a
   snapshot fails immediately (`ORA-41900` on 23ai; expect `ORA-00942`/`ORA-01031` on
   19c) and zero rows leave the database. No VPD policy needed.
5. Quick probe to run on any target version, connected as the CDC user:
   `SELECT COUNT(*) FROM all_tables WHERE owner = '<CDC SCHEMA>';` — if it's 0, that
   version isn't honouring whatever privilege you're relying on for visibility.

## Caveats

- Oracle Free 23ai, single instance in Docker — not an enterprise topology (RAC,
  standby, archivelog retention). The permission semantics tested are standard Oracle
  behaviour, but a cheap confirmation on a staging DB of the target version is
  worthwhile, especially since pre-23 versions enumerate a different privilege list in
  the `ALL_TABLES` view definition.
- The Oracle-based checkpoint cache wasn't exercised (file cache used to isolate
  variables) — note it needs `CREATE TABLE` + `CREATE PROCEDURE`.

## Teardown

```bash
docker rm -f oracle-cdc-perms
```
