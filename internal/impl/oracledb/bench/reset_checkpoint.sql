-- Resets (or seeds) the oracledb_cdc checkpoint on the 'oracledb' benchmark
-- container to a fixed baseline SCN, so reruns of benchmark_config.yaml
-- start from the same known position.
--
-- CACHE_VAL is a RAW(8) little-endian encoding of the SCN. The baseline
-- below (5111794) is the position reported by "No cached SCN found, fetched
-- current position from database: 5111794" in a prior run's startup log -
-- update the HEXTORAW literals below to reset to a different SCN.
MERGE INTO C##RPCN.CDC_CHECKPOINT_TESTPDB t
USING (SELECT 'oracledb_cdc' AS CACHE_KEY FROM DUAL) s
ON (t.CACHE_KEY = s.CACHE_KEY)
WHEN MATCHED THEN UPDATE SET CACHE_VAL = HEXTORAW('F2FF4D0000000000')
WHEN NOT MATCHED THEN INSERT (CACHE_KEY, CACHE_VAL) VALUES ('oracledb_cdc', HEXTORAW('F2FF4D0000000000'));
COMMIT;
SELECT CACHE_KEY, RAWTOHEX(CACHE_VAL) AS CACHE_VAL_HEX FROM C##RPCN.CDC_CHECKPOINT_TESTPDB;
EXIT;
