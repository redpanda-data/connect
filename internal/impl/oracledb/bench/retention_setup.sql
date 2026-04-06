-- Archive log retention setup for local CDC development.
-- Keeps archive logs available so LogMiner can read them.
-- Equivalent to: CONFIGURE RETENTION POLICY TO RECOVERY WINDOW OF 1 DAYS;
ALTER SYSTEM SET DB_RECOVERY_FILE_DEST_SIZE = 10G SCOPE=BOTH;
ALTER SYSTEM SET ARCHIVE_LAG_TARGET = 60 SCOPE=BOTH;
EXIT;
