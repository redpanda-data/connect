-- =============================================================================
-- 04_post_start_setup.sql — One-Time Post-Start Setup
-- =============================================================================
-- Run ONCE after HANA Express has started and is healthy, BEFORE running any
-- CDC workload.  This activates log segment retention so the redo log reader
-- can consume segments instead of having HANA overwrite them.
--
-- How to run:
--   docker exec -it hxe /bin/bash -c \
--     "/usr/sap/HXE/HDB90/exe/hdbsql \
--       -i 90 -u SYSTEM -p HXEHana1 -d HXE \
--       -I /hana/mounts/setup/04_post_start_setup.sql"
--
-- Or interactively:
--   docker exec -it hxe hdbsql -i 90 -u SYSTEM -p HXEHana1 -d HXE
--   \i /hana/mounts/setup/04_post_start_setup.sql
-- =============================================================================


-- ---------------------------------------------------------------------------
-- Step 1 — Verify current log mode
-- ---------------------------------------------------------------------------
-- Expected: VALUE = 'normal'
-- 'overwrite' means segments are recycled and the CDC reader will miss events.
SELECT VALUE
FROM SYS.M_INIFILE_CONTENTS
WHERE FILE_NAME = 'global.ini'
  AND SECTION    = 'persistence'
  AND KEY        = 'log_mode';


-- ---------------------------------------------------------------------------
-- Step 2 — Full data backup to activate log segment retention
-- ---------------------------------------------------------------------------
-- In log_mode=normal, HANA only retains log segments once a full backup has
-- been taken.  Until then the segments are still overwritten even if log_mode
-- is already 'normal'.
--
-- The backup path /hana/mounts/backup/ must exist inside the container.
-- Map it from the host: add '-v /tmp/hana-backup:/hana/mounts/backup' to the
-- docker run command (or the volumes: section in docker-compose.yml).
BACKUP DATA USING FILE ('/hana/mounts/backup/FULL');


-- ---------------------------------------------------------------------------
-- Step 3 — Verify log segment retention is now active
-- ---------------------------------------------------------------------------
-- After a successful backup, segments should show STATE = 'Closed' (retained).
-- A segment in STATE = 'Free' means it has been overwritten.
SELECT
    SEGMENT_ID,
    STATE,
    FILE_NAME,
    TOTAL_SIZE,
    USED_SIZE
FROM SYS.M_LOG_SEGMENTS
ORDER BY SEGMENT_ID;


-- ---------------------------------------------------------------------------
-- Step 4 — Fix log file permissions (host-side, not SQL)
-- ---------------------------------------------------------------------------
-- HANA writes log segment files with mode 600 (owner: hxeadm).
-- The CDC sidecar container reads them as a different UID and needs at least
-- read permission.  Run the following on the Docker HOST after this script:
--
--   chmod 640 /tmp/hana-express-data/log/HXE/mnt00001/hdb*/logsegment_000_*.dat
--
-- Or configure the container with supplementary groups so the sidecar UID
-- is in the hxeadm group (GID typically 1001 in SAP images).
--
-- This step cannot be automated via hdbsql because HANA runs as hxeadm and
-- the sidecar runs as a different OS user.

SELECT 'Step 4: Fix log file permissions on the Docker host — see comment above' AS REMINDER
FROM DUMMY;


-- ---------------------------------------------------------------------------
-- Step 5 — Optional: lower log segment size for faster test cycles
-- ---------------------------------------------------------------------------
-- Default log segment size is 64 MB.  Smaller segments roll over faster so
-- CDC tests do not have to write a lot of data to see a segment boundary.
-- Only change this in dev/test environments.
--
-- ALTER SYSTEM ALTER CONFIGURATION ('global.ini', 'SYSTEM')
--     SET ('persistence', 'log_segment_size_mb') = '4'
--     WITH RECONFIGURE;


-- ---------------------------------------------------------------------------
-- Summary query — print current configuration state
-- ---------------------------------------------------------------------------
SELECT
    KEY,
    VALUE
FROM SYS.M_INIFILE_CONTENTS
WHERE FILE_NAME = 'global.ini'
  AND SECTION   = 'persistence'
  AND KEY IN ('log_mode', 'log_segment_size_mb', 'basepath_logvolumes')
ORDER BY KEY;
