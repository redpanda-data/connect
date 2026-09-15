-- Resets the oracledb_cdc checkpoint on the scnbump demo container (oracledb-scnbump)
-- back to the pre-gap baseline SCN (5113453, captured right after the initial snapshot),
-- so a rerun of a log_count-configured connection against this container re-traverses
-- both simulated SCN jumps (sequence 35: ~19.9M, sequence 63: ~86.9M) from scratch.
UPDATE C##RPCN.CDC_CHECKPOINT_TESTPDB
SET CACHE_VAL = HEXTORAW('6D064E0000000000')
WHERE CACHE_KEY = 'oracledb_cdc';
COMMIT;
SELECT * FROM C##RPCN.CDC_CHECKPOINT_TESTPDB;
EXIT;
