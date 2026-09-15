-- Resizes the online redo log groups to ~400MB by adding new, larger groups,
-- cycling log switches until the old (200MB) groups go INACTIVE, then
-- dropping them. Oracle has no in-place resize for an existing group.
SET SERVEROUTPUT ON

-- 1. Add three new 400MB groups (4,5,6) alongside the existing 200MB ones.
ALTER DATABASE ADD LOGFILE GROUP 4 ('/opt/oracle/oradata/FREE/redo04.log') SIZE 400M;
ALTER DATABASE ADD LOGFILE GROUP 5 ('/opt/oracle/oradata/FREE/redo05.log') SIZE 400M;
ALTER DATABASE ADD LOGFILE GROUP 6 ('/opt/oracle/oradata/FREE/redo06.log') SIZE 400M;

-- 2. Cycle through log switches so the old groups (1,2,3) stop being
--    CURRENT/ACTIVE. A handful of switches plus a checkpoint is normally
--    enough on an idle database.
ALTER SYSTEM SWITCH LOGFILE;
ALTER SYSTEM SWITCH LOGFILE;
ALTER SYSTEM SWITCH LOGFILE;
ALTER SYSTEM SWITCH LOGFILE;
ALTER SYSTEM SWITCH LOGFILE;
ALTER SYSTEM SWITCH LOGFILE;
ALTER SYSTEM CHECKPOINT;

-- 3. Confirm 1,2,3 are INACTIVE before continuing - if any still show
--    CURRENT/ACTIVE here, switch a few more times and re-check before
--    running the drops below.
SELECT GROUP#, BYTES/1024/1024 AS MB, STATUS FROM V$LOG ORDER BY GROUP#;

-- 4. Drop the old 200MB groups (fails with ORA-01623/ORA-01624 if a group
--    is still CURRENT or ACTIVE - re-run step 2 if so).
ALTER DATABASE DROP LOGFILE GROUP 1;
ALTER DATABASE DROP LOGFILE GROUP 2;
ALTER DATABASE DROP LOGFILE GROUP 3;

-- 5. Final state - should show only groups 4,5,6 at ~300MB.
SELECT GROUP#, BYTES/1024/1024 AS MB, STATUS FROM V$LOG ORDER BY GROUP#;
EXIT;
