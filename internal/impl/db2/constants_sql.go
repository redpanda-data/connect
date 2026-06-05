// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package db2

// SQL format strings used in db2CDCInput. All strings embed user-configured
// table names (signalTable, cpCacheTableName, asnCDCSchema) so they cannot be
// compile-time constants. They are computed once in newDB2CDCInput by calling
// fmt.Sprintf with the validated table names and stored in db2CDCInput for
// reuse across all polling iterations.
//
// All table identifiers embedded here are validated via isValidDB2Identifier
// (schema parts) or validateQualifiedIdentifier (schema.table) before any SQL
// is built. Direct string interpolation is safe for these values.
const (
	// Checkpoint table DML (%s = fully-qualified table name, e.g. "RPCN.CDC_CHECKPOINT").
	//
	// cpMergeSQLFmt upserts a key/value pair. DB2 MERGE guarantees atomicity.
	cpMergeSQLFmt = `
		MERGE INTO %s AS T
		USING (VALUES (?, ?)) AS S(CACHE_KEY, CACHE_VAL)
		ON T.CACHE_KEY = S.CACHE_KEY
		WHEN MATCHED THEN UPDATE SET T.CACHE_VAL = S.CACHE_VAL
		WHEN NOT MATCHED THEN INSERT (CACHE_KEY, CACHE_VAL) VALUES (S.CACHE_KEY, S.CACHE_VAL)
	`
	// cpSelectSQLFmt reads a single checkpoint value by key.
	cpSelectSQLFmt = "SELECT CACHE_VAL FROM %s WHERE CACHE_KEY = ?"
	// cpDeleteSQLFmt removes a checkpoint entry by key.
	cpDeleteSQLFmt = "DELETE FROM %s WHERE CACHE_KEY = ?"

	// Signal table queries (%[1]s = signal table, %[2]d = signalFetchLimit).
	//
	// Signal table schema (auto-created if absent):
	//   CREATE TABLE <table> (
	//     ID   VARCHAR(255) NOT NULL,
	//     TYPE VARCHAR(64)  NOT NULL,
	//     DATA VARCHAR(2048),
	//     PRIMARY KEY (ID)
	//   )
	//
	// Signal types consumed by processSignals:
	//   'execute-snapshot'  – trigger an incremental snapshot of DATA.data-collections
	//   'stop-snapshot'     – abort the in-progress incremental snapshot
	//   'pause-snapshot'    – pause the snapshot mid-run (resumes on 'resume-snapshot')
	//   'resume-snapshot'   – resume a paused snapshot
	sigSelectStopSQLFmt   = "SELECT ID FROM %[1]s WHERE TYPE = 'stop-snapshot'   FETCH FIRST %[2]d ROWS ONLY"
	sigSelectPauseSQLFmt  = "SELECT ID FROM %[1]s WHERE TYPE = 'pause-snapshot'  FETCH FIRST %[2]d ROWS ONLY"
	sigSelectResumeSQLFmt = "SELECT ID FROM %[1]s WHERE TYPE = 'resume-snapshot' FETCH FIRST %[2]d ROWS ONLY"
	sigSelectExecSQLFmt   = "SELECT ID, DATA FROM %[1]s WHERE TYPE = 'execute-snapshot' FETCH FIRST %[2]d ROWS ONLY"
	// sigDeleteSQLFmt deletes any signal row by ID (used for post-snapshot cleanup).
	sigDeleteSQLFmt = "DELETE FROM %s WHERE ID = ?"
	// sigDeleteExecSQLFmt deletes only 'execute-snapshot' signals to avoid
	// accidentally removing control signals that arrived concurrently.
	sigDeleteExecSQLFmt = "DELETE FROM %s WHERE ID = ? AND TYPE = 'execute-snapshot'"
	// sigAbortPollSQLFmt is polled inside the snapshot goroutine to detect
	// stop/pause signals without holding any other lock.
	sigAbortPollSQLFmt = "SELECT TYPE FROM %s WHERE TYPE IN ('stop-snapshot','pause-snapshot') FETCH FIRST 1 ROWS ONLY"

	// Schema-change poll query. %[1]s = asnCDCSchema, %[2]d = schemaChangeFetchLimit.
	// The caller substitutes the lastHex watermark via a second fmt.Sprintf using %%s.
	schemaChangeQueryFmt = `
		SELECT r.SOURCE_OWNER, r.SOURCE_TABLE, r.CD_NEW_SYNCHPOINT
		FROM %[1]s.IBMSNAP_REGISTER r
		WHERE r.SOURCE_OWNER = ?
		  AND r.CD_NEW_SYNCHPOINT > X'%%s'
		ORDER BY r.CD_NEW_SYNCHPOINT
		FETCH FIRST %[2]d ROWS ONLY
	`
)
