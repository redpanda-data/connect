// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logminer

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/logminer/sqlredo"
	"github.com/redpanda-data/connect/v4/internal/impl/oracledb/replication"
)

// redoProcessor is the buffer between the redo rows that LogMiner mines and
// the messages that the connector publishes. It keeps the events of each open
// transaction, assembles LOB values, and at commit merges the events and
// publishes them with a safe checkpoint SCN.
//
// LogMiner passes each row from queryLogMinerContents to processRedoEvent.
// redoProcessor does no mining: it uses no connection, no LogMiner session and
// no SCN window.
//
// Each buffer in this struct holds redo that is mined but not yet published.
// A checkpoint must not go past the lowest SCN that a buffer still holds,
// because a restart does not mine that redo again. Each new buffer must count
// in the checkpoint that processRedoEvent computes at commit. Today only
// txnCache counts (through LowWatermarkSCN). lobStates and pendingLOBWrites
// do not count yet.
type redoProcessor struct {
	// lobEnabled is a copy of Config.LOBEnabled. When it is false,
	// processRedoEvent ignores the LOB operations, and lobStates and
	// pendingLOBWrites stay empty.
	lobEnabled bool

	// txnCache holds the parsed DML events of each open transaction.
	// processRedoEvent adds to it, and inferLOBLocator reads it to find the
	// row that a LOB_WRITE belongs to. The entry of a transaction is removed at
	// its commit or rollback.
	txnCache TransactionCache
	// lobStates holds the LOB fragments of each open transaction, because one
	// LOB value is split across many redo rows. processRedoEvent merges them
	// into the DML events at commit. The entry of a transaction is removed at
	// its commit or rollback.
	lobStates map[sqlredo.TransactionID]*sqlredo.TxnLOBState
	// pendingLOBWrites holds LOB_WRITE events that arrived before their INSERT
	// (BASICFILE DISABLE STORAGE IN ROW ordering from Oracle LogMiner).
	// replayDeferredLOBWrites replays them at commit, when inferLOBLocator can
	// find the INSERT. The entry of a transaction is removed at its commit or
	// rollback, and the writes that are still deferred at commit are dropped.
	pendingLOBWrites map[sqlredo.TransactionID][]*sqlredo.RedoEvent
	// lobColTypes maps each LOB column of the configured tables to its type,
	// for example "TESTDB.PRODUCTS.DESCRIPTION": "NCLOB". Redo rows do not
	// include data types. processRedoEvent reads it to decode LOB data and to
	// find LOB-only UPDATEs, also when lobEnabled is false. It is read only.
	//
	// LogMiner.loadLOBColumnTypes sets it at the start of each ReadChanges
	// call, after construction, because the query on ALL_TAB_COLUMNS needs a
	// database connection and the PDB container switch.
	lobColTypes map[string]string

	// dmlParser parses the SQL_REDO of DML events into column values.
	dmlParser *sqlredo.Parser
	// publisher receives the events of each committed transaction.
	publisher replication.ChangePublisher
	// publishLagMetric records the time from the database commit to the publish.
	publishLagMetric *service.MetricTimer
	log              *service.Logger
}

// processRedoEvent buffers emitted events until a commit or rollback event is processed at which
// point the buffer can be flushed to the Connect pipeline or dropped.
func (rp *redoProcessor) processRedoEvent(ctx context.Context, redoEvent *sqlredo.RedoEvent) error {
	switch redoEvent.Operation {
	case sqlredo.OpStart:
		// Transaction started
		if err := rp.txnCache.StartTransaction(ctx, redoEvent.TransactionID, redoEvent.SCN); err != nil {
			return fmt.Errorf("starting transaction %s: %w", redoEvent.TransactionID, err)
		}

	case sqlredo.OpInsert, sqlredo.OpUpdate, sqlredo.OpDelete:
		// SQL_REDO should always be present for DML operations. If not, it's likely a temporary
		// table (Oracle doesn't generate redo for these) or an unsupported operation.
		if !redoEvent.SQLRedo.Valid || redoEvent.SQLRedo.String == "" {
			rp.log.Warnf("Skipping DML event with no SQL_REDO (operation=%s, table=%s.%s, scn=%d, txn=%s) - likely temporary table or unsupported operation",
				redoEvent.Operation, redoEvent.SchemaName.String, redoEvent.TableName.String, redoEvent.SCN, redoEvent.TransactionID)
			return nil
		}

		// Parse sql insert/update/delete sql statements into key/value object
		event, err := rp.dmlParser.RedoEventToDMLEvent(redoEvent)
		if err != nil {
			rp.log.Debugf("failed to parse SQL_REDO (scn=%d, op=%s, table=%s.%s, txn=%s): %s",
				redoEvent.SCN, redoEvent.Operation, redoEvent.SchemaName.String, redoEvent.TableName.String, redoEvent.TransactionID, redoEvent.SQLRedo.String)
			return fmt.Errorf("parsing sql redo event into dml event: %w", err)
		}

		if err := rp.txnCache.AddEvent(ctx, redoEvent.TransactionID, redoEvent.SCN, &event); err != nil {
			return fmt.Errorf("adding event to transaction %s: %w", redoEvent.TransactionID, err)
		}

	case sqlredo.OpSelectLobLocator:
		if !rp.lobEnabled {
			return nil
		}
		if !redoEvent.SQLRedo.Valid || redoEvent.SQLRedo.String == "" {
			rp.log.Warnf("Skipping SELECT_LOB_LOCATOR with no SQL_REDO (scn=%d, txn=%s)", redoEvent.SCN, redoEvent.TransactionID)
			return nil
		}
		info, err := sqlredo.ParseSelectLobLocator(redoEvent.SQLRedo.String)
		if err != nil {
			rp.log.Warnf("Failed to parse SELECT_LOB_LOCATOR SQL (scn=%d, txn=%s): %v\nSQL: %.500s", redoEvent.SCN, redoEvent.TransactionID, err, redoEvent.SQLRedo.String)
			return nil
		}
		// Resolve LOB type from the schema cache populated at startup.
		colKey := fmt.Sprintf("%s.%s.%s", info.Schema, info.Table, info.Column)
		lobType := rp.lobColTypes[strings.ToUpper(colKey)] // "CLOB", "BLOB", "NCLOB", or "" if unknown

		state := rp.getOrCreateLOBState(redoEvent.TransactionID)
		key := sqlredo.LobKey{
			Schema:   info.Schema,
			Table:    info.Table,
			Column:   info.Column,
			PKString: sqlredo.FormatPKString(info.PKValues),
		}
		if _, exists := state.Accumulators[key]; !exists {
			state.Accumulators[key] = &sqlredo.LobAccumulator{
				Schema:   info.Schema,
				Table:    info.Table,
				Column:   info.Column,
				PKValues: info.PKValues,
				IsBinary: lobType == "BLOB",
			}
		}
		state.ActiveKey = &key

	case sqlredo.OpLobTrim:
		if !rp.lobEnabled {
			return nil
		}
		// LOB_TRIM (op 11) comes in two forms depending on Oracle LOB type:
		//
		// Form A — SELECT "COL" INTO ... FROM "SCHEMA"."TABLE" WHERE ...
		//   Emitted for certain LOB types (e.g. out-of-line SecureFile) without a preceding
		//   SELECT_LOB_LOCATOR. In this case LOB_TRIM itself must establish the accumulator.
		//
		// Form B — dbms_lob.trim(loc_b, N)
		//   Emitted when a SELECT_LOB_LOCATOR has already established the active key.
		//   No schema/table/column info is present. The accumulator is left untouched
		//   regardless of N — see the inline comment below for the rationale.
		//
		//   When N>0 and no fragments have been accumulated, a warning is emitted because
		//   Oracle intends to keep the first N bytes/chars of the pre-existing LOB, which
		//   we do not hold. In the common SecureFile full-rewrite path LOB_WRITE(s) precede
		//   LOB_TRIM and the assembled length equals N, so no data is lost in practice.
		if redoEvent.SQLRedo.Valid && redoEvent.SQLRedo.String != "" {
			if info, err := sqlredo.ParseSelectLobLocator(redoEvent.SQLRedo.String); err == nil {
				// Form A: establish (or reset) the accumulator for this LOB column.
				colKey := fmt.Sprintf("%s.%s.%s", info.Schema, info.Table, info.Column)
				lobType := rp.lobColTypes[strings.ToUpper(colKey)]
				state := rp.getOrCreateLOBState(redoEvent.TransactionID)
				key := sqlredo.LobKey{
					Schema:   info.Schema,
					Table:    info.Table,
					Column:   info.Column,
					PKString: sqlredo.FormatPKString(info.PKValues),
				}
				state.Accumulators[key] = &sqlredo.LobAccumulator{
					Schema:   info.Schema,
					Table:    info.Table,
					Column:   info.Column,
					PKValues: info.PKValues,
					IsBinary: lobType == "BLOB",
				}
				state.ActiveKey = &key
				return nil
			}
		}
		// Form B: LOB_TRIM carries no schema/table/column info — the active key was
		// already established by SELECT_LOB_LOCATOR. Oracle may emit LOB_TRIM before
		// LOB_WRITE (BASICFILE "clear then write") or after (SecureFile "write then
		// finalize"). In both cases the accumulator should be left untouched:
		//   - Before LOB_WRITE: accumulator is empty anyway, so there is nothing to clear.
		//   - After LOB_WRITE:  fragments are already accumulated; clearing them would
		//     destroy the data before commit.
		state, exists := rp.lobStates[redoEvent.TransactionID]
		if !exists || state.ActiveKey == nil {
			return nil
		}
		if redoEvent.SQLRedo.Valid && redoEvent.SQLRedo.String != "" {
			if trimLen, err := sqlredo.ParseLobTrim(redoEvent.SQLRedo.String); err == nil && trimLen > 0 {
				// Warn only for the blatant case: N>0 with no fragments at all, meaning
				// the existing LOB prefix is preserved but we have nothing to emit.
				// Two adjacent cases (N < total written bytes, or N > total written bytes
				// with M>0) also produce an assembled value that does not exactly match N,
				// but Assemble() is not truncated to N here. This is an intentional
				// tradeoff: SecureFile full-rewrite UPDATEs (the common path) always write
				// all bytes then trim to the exact final length, so assembled==N in
				// practice. Partial-update patterns are not supported by this path.
				if acc := state.Accumulators[*state.ActiveKey]; acc != nil && len(acc.Fragments) == 0 {
					rp.log.Warnf("LOB_TRIM to non-zero length %d with no prior LOB_WRITE (scn=%d, txn=%s): assembled value may be incomplete", trimLen, redoEvent.SCN, redoEvent.TransactionID)
				}
			}
		}

	case sqlredo.OpLobWrite:
		if !rp.lobEnabled {
			return nil
		}
		state, exists := rp.lobStates[redoEvent.TransactionID]
		if !exists || state.ActiveKey == nil {
			if !rp.inferLOBLocator(ctx, redoEvent) {
				// INSERT may arrive later in the same LogMiner batch (BASICFILE
				// DISABLE STORAGE IN ROW ordering). Defer and replay after DML.
				rp.log.Debugf("LOB_WRITE before INSERT (scn=%d, txn=%s): deferring", redoEvent.SCN, redoEvent.TransactionID)
				rp.pendingLOBWrites[redoEvent.TransactionID] = append(rp.pendingLOBWrites[redoEvent.TransactionID], redoEvent)
				return nil
			}
			state = rp.lobStates[redoEvent.TransactionID]
		}
		acc := state.Accumulators[*state.ActiveKey]
		if acc == nil {
			rp.log.Warnf("LOB_WRITE has active key but no accumulator (scn=%d, txn=%s)", redoEvent.SCN, redoEvent.TransactionID)
			return nil
		}
		if !redoEvent.SQLRedo.Valid || redoEvent.SQLRedo.String == "" {
			return nil
		}
		// NCLOB LOB_WRITE SQL delivers data as a plain string literal (same as CLOB),
		// not as HEXTORAW. Only BLOB uses binary/hex encoding.
		writeInfo, err := sqlredo.ParseLobWrite(redoEvent.SQLRedo.String, acc.IsBinary)
		if err != nil {
			rp.log.Warnf("Failed to parse LOB_WRITE SQL (scn=%d, txn=%s): %v\nSQL: %.500s", redoEvent.SCN, redoEvent.TransactionID, err, redoEvent.SQLRedo.String)
			return nil
		}
		acc.AddFragment(writeInfo.Offset, writeInfo.Data)

	case sqlredo.OpCommit:
		// Flush all buffered events for given transaction ID
		txn, err := rp.txnCache.GetTransaction(ctx, redoEvent.TransactionID)
		if err != nil {
			return fmt.Errorf("fetching transaction %s on commit: %w", redoEvent.TransactionID, err)
		}
		if txn != nil {
			safeCheckpointSCN := redoEvent.SCN

			// If other transactions are still open, we must not advance the
			// checkpoint past their start SCN - 1. Doing so would cause their
			// already-seen DML events to be skipped on restart (the query resumes
			// from SCN > checkpoint). We subtract 1 because the query is exclusive.
			if lowestOpenSCN := rp.txnCache.LowWatermarkSCN(redoEvent.TransactionID); lowestOpenSCN != math.MaxUint64 && lowestOpenSCN > 0 {
				if lowestOpenSCN-1 < safeCheckpointSCN {
					safeCheckpointSCN = lowestOpenSCN - 1
				}
			}

			if rp.lobEnabled {
				// Replay deferred LOB_WRITEs (BASICFILE DISABLE STORAGE IN ROW) before
				// merging. At commit time, SELECT_LOB_LOCATOR has already claimed all
				// SecureFile LOB columns, so inferLOBLocator can identify the unclaimed
				// BASICFILE column by excluding columns that already have accumulators.
				if err := rp.replayDeferredLOBWrites(ctx, redoEvent.TransactionID); err != nil {
					return err
				}

				// Merge any accumulated LOB data into DML events before publishing.
				if state, ok := rp.lobStates[redoEvent.TransactionID]; ok {
					unmerged := sqlredo.MergeLOBsIntoDMLEvents(state, txn.Events, rp.log)
					// Synthesize UPDATE events for LOB accumulators that had no matching DML
					// event. This handles Oracle SecureFile out-of-row LOBs where Oracle does
					// not emit a DML UPDATE in LogMiner — only SELECT_LOB_LOCATOR + LOB_WRITE
					// + LOB_TRIM operations are recorded.
					//
					// The synthesized event is intentionally sparse: Data contains only the
					// LOB column(s) and OldValues contains only the PK columns extracted from
					// the SELECT_LOB_LOCATOR WHERE clause. Other row columns are not available
					// from redo alone. Carrying over values from a prior event in the same
					// transaction is not possible here: MergeLOBsIntoDMLEvents merges into any
					// matching INSERT (Pass 1) or PK-bearing UPDATE (Pass 2) before returning
					// an accumulator as unmerged, so by definition no full-row DML event for
					// this row exists in the transaction. Downstream consumers should treat a
					// sparse UPDATE (OldValues containing only PK columns) as a LOB-column-only
					// change with no information about other columns.
					for _, acc := range unmerged {
						assembled := acc.Assemble()
						if assembled == nil {
							continue
						}
						synthetic := &sqlredo.DMLEvent{
							Operation:     sqlredo.OpUpdate,
							Schema:        acc.Schema,
							Table:         acc.Table,
							Data:          map[string]any{acc.Column: assembled},
							OldValues:     acc.PKValues,
							TransactionID: redoEvent.TransactionID,
							Timestamp:     redoEvent.Timestamp,
							Username:      redoEvent.Username.String,
						}
						txn.Events = append(txn.Events, synthetic)
						rp.log.Debugf("LOB merge: synthesized UPDATE for %s.%s.%s (pks=%v, fragments=%d)", acc.Schema, acc.Table, acc.Column, acc.PKValues, len(acc.Fragments))
					}
				}
			}

			// Build a set of schema.table pairs that have an INSERT in this transaction.
			// Used below to detect and suppress Oracle-internal LOB-initialisation UPDATEs.
			insertTables := make(map[string]struct{})
			for _, ev := range txn.Events {
				if ev.Operation == sqlredo.OpInsert {
					insertTables[ev.Schema+"."+ev.Table] = struct{}{}
				}
			}

			// Tracks, per LOB-only UPDATE event, whether the pre-pass below actually
			// found a matching INSERT to merge its values into. Keyed by pointer
			// identity rather than schema.table: a table having *some* INSERT in the
			// transaction doesn't mean *this row's* INSERT was found, so suppression
			// must be decided per event, not per table.
			mergedIntoInsert := make(map[*sqlredo.DMLEvent]bool)

			if rp.lobEnabled {
				// Pre-pass: for each LOB-only UPDATE that accompanies an INSERT in this transaction,
				// merge the actual LOB values into the INSERT before we start publishing.
				//
				// Oracle omits LOB columns from the INSERT SQL_REDO entirely and instead emits a
				// separate UPDATE whose SET clause carries the real LOB data. We must propagate
				// those values into the INSERT event before suppressing the UPDATE.
				for _, dmlEvent := range txn.Events {
					if dmlEvent.Operation != sqlredo.OpUpdate || !rp.isLOBOnlyEvent(dmlEvent) {
						continue
					}
					if _, hasInsert := insertTables[dmlEvent.Schema+"."+dmlEvent.Table]; !hasInsert {
						continue
					}
					mergedIntoInsert[dmlEvent] = sqlredo.MergeInlineLOBValues(dmlEvent.Data, dmlEvent.Schema, dmlEvent.Table, dmlEvent.OldValues, txn.Events, rp.log)
				}
			}

			for _, dmlEvent := range txn.Events {
				// Suppress Oracle-internal LOB-initialisation UPDATEs. With LOBEnabled,
				// only once confirmed merged: the table having some other row's INSERT
				// is not enough - if the pre-pass found no matching INSERT for THIS row,
				// the UPDATE is the only remaining record of its LOB data and must be
				// published rather than silently dropped. Without LOBEnabled, no merge
				// is ever attempted (mergedIntoInsert stays empty), so this instead
				// falls back to the table-level check: the LOB values are being
				// discarded either way, so there is no per-row content to lose.
				suppress := dmlEvent.Operation == sqlredo.OpUpdate && rp.isLOBOnlyEvent(dmlEvent)
				if suppress {
					if rp.lobEnabled {
						suppress = mergedIntoInsert[dmlEvent]
					} else {
						_, suppress = insertTables[dmlEvent.Schema+"."+dmlEvent.Table]
					}
				}
				if suppress {
					rp.log.Debugf("suppressing LOB-only UPDATE for %s.%s", dmlEvent.Schema, dmlEvent.Table)
					continue
				}
				msg := toMessageEvent(dmlEvent, redoEvent.SCN, safeCheckpointSCN, redoEvent.Timestamp)
				if err := rp.publisher.Publish(ctx, msg); err != nil {
					return fmt.Errorf("publishing event with SCN '%d': %w", redoEvent.SCN, err)
				}
				rp.publishLagMetric.Timing(time.Since(redoEvent.Timestamp).Nanoseconds())
			}

			if err := rp.txnCache.CommitTransaction(ctx, redoEvent.TransactionID); err != nil {
				return fmt.Errorf("committing transaction %s: %w", redoEvent.TransactionID, err)
			}
		}

		// Always clean up lobStates on commit, including for transactions discarded by
		// the cache (GetTransaction returns nil when MaxTransactionEvents is exceeded).
		// Without this, LOB events that bypass the cache continue to accumulate in
		// lobStates and are never freed.
		if rp.lobEnabled {
			delete(rp.lobStates, redoEvent.TransactionID)
			if pending := rp.pendingLOBWrites[redoEvent.TransactionID]; len(pending) > 0 {
				for _, p := range pending {
					rp.log.Warnf("Dropping deferred LOB_WRITE on commit: txn=%s scn=%d schema=%s table=%s sql=%.200s",
						redoEvent.TransactionID, p.SCN, p.SchemaName.String, p.TableName.String, p.SQLRedo.String)
				}
				delete(rp.pendingLOBWrites, redoEvent.TransactionID)
			}
		}

	case sqlredo.OpRollback:
		// Discard all buffered events for this transaction
		if rp.lobEnabled {
			delete(rp.lobStates, redoEvent.TransactionID)
			delete(rp.pendingLOBWrites, redoEvent.TransactionID)
		}
		if err := rp.txnCache.RollbackTransaction(ctx, redoEvent.TransactionID); err != nil {
			return fmt.Errorf("rolling back transaction %s: %w", redoEvent.TransactionID, err)
		}
	}

	return nil
}

// replayDeferredLOBWrites replays LOB_WRITE events that were buffered because
// their INSERT had not yet arrived. Called after each DML event is added to the
// transaction cache so that inferLOBLocator can now find the INSERT.
func (rp *redoProcessor) replayDeferredLOBWrites(ctx context.Context, txnID sqlredo.TransactionID) error {
	pending := rp.pendingLOBWrites[txnID]
	if len(pending) == 0 {
		return nil
	}
	rp.log.Debugf("replayDeferredLOBWrites: replaying %d LOB_WRITE(s) for txn %s", len(pending), txnID)
	// Clear before replaying so re-buffering during the loop appends to a fresh slice.
	delete(rp.pendingLOBWrites, txnID)
	// Clear ActiveKey so inferLOBLocator is invoked for the first deferred write.
	// The prior SELECT_LOB_LOCATOR may have left ActiveKey pointing at a SecureFile
	// column; without this reset, deferred LOB_WRITEs would land on that column
	// instead of the unclaimed BASICFILE out-of-row column.
	if state, ok := rp.lobStates[txnID]; ok {
		state.ActiveKey = nil
	}
	for _, ev := range pending {
		if err := rp.processRedoEvent(ctx, ev); err != nil {
			return err
		}
	}
	if reDeferred := len(rp.pendingLOBWrites[txnID]); reDeferred > 0 {
		rp.log.Warnf("replayDeferredLOBWrites: %d LOB_WRITE(s) re-deferred after replay for txn %s — inferLOBLocator still failing", reDeferred, txnID)
	}
	return nil
}

func (rp *redoProcessor) getOrCreateLOBState(txnID sqlredo.TransactionID) *sqlredo.TxnLOBState {
	if state, ok := rp.lobStates[txnID]; ok {
		return state
	}

	s := sqlredo.NewTxnLOBState()
	rp.lobStates[txnID] = s
	return s
}

// isLOBOnlyEvent reports whether every column in ev.Data is a known LOB column.
// This identifies Oracle's internal LOB-initialisation UPDATE events, which carry
// only LOB column values and should be suppressed when a matching INSERT already
// exists in the same transaction.
func (rp *redoProcessor) isLOBOnlyEvent(ev *sqlredo.DMLEvent) bool {
	if len(ev.Data) == 0 {
		return false
	}
	for col := range ev.Data {
		key := strings.ToUpper(ev.Schema + "." + ev.Table + "." + col)
		if _, exists := rp.lobColTypes[key]; !exists {
			return false
		}
	}
	return true
}

// inferLOBLocator attempts to create a LOB locator for a LOB_WRITE event that
// arrived without a preceding SELECT_LOB_LOCATOR. This happens with BASICFILE
// out-of-line LOBs where Oracle does not emit locator events in LogMiner.
//
// The method searches backward through the transaction's buffered DML events for
// a LOB-only UPDATE or INSERT that can act as an anchor for the LOB data.
// Returns true if a locator was successfully created.
func (rp *redoProcessor) inferLOBLocator(ctx context.Context, event *sqlredo.RedoEvent) bool {
	if !event.SchemaName.Valid || !event.TableName.Valid {
		return false
	}
	schema := event.SchemaName.String
	table := event.TableName.String
	if schema == "" || table == "" {
		return false
	}

	txn, err := rp.txnCache.GetTransaction(ctx, event.TransactionID)
	if err != nil {
		rp.log.Errorf("Failed to get transaction %s for LOB locator inference: %v", event.TransactionID, err)
		return false
	}
	if txn == nil {
		rp.log.Debugf("inferLOBLocator: txn %s not in cache (scn=%d, schema=%s, table=%s) — no DML events yet",
			event.TransactionID, event.SCN, schema, table)
		return false
	}

	prefix := strings.ToUpper(schema + "." + table + ".")

	// claimedCols holds LOB column names that already have an accumulator for this
	// schema.table, regardless of PKString. At commit time these are columns
	// claimed by SELECT_LOB_LOCATOR.
	var (
		claimedCols           = make(map[string]struct{})
		emptyClaimedKeys      = make(map[string]sqlredo.LobKey)
		claimedFragmentCounts = make(map[string]int)
	)
	if existingState := rp.lobStates[event.TransactionID]; existingState != nil {
		for k, acc := range existingState.Accumulators {
			if k.Schema == schema && k.Table == table {
				claimedCols[k.Column] = struct{}{}
				claimedFragmentCounts[k.Column] = len(acc.Fragments)
				if len(acc.Fragments) == 0 {
					emptyClaimedKeys[k.Column] = k
				}
			}
		}
	}
	{
		claimed := make([]string, 0, len(claimedCols))
		for c, n := range claimedFragmentCounts {
			claimed = append(claimed, fmt.Sprintf("%s(%d)", c, n))
		}
		empty := make([]string, 0, len(emptyClaimedKeys))
		for c := range emptyClaimedKeys {
			empty = append(empty, c)
		}
		rp.log.Debugf("inferLOBLocator: claimedCols=%v emptyClaimedKeys=%v (txn=%s, scn=%d, table=%s.%s)",
			claimed, empty, event.TransactionID, event.SCN, schema, table)
	}

	for i := len(txn.Events) - 1; i >= 0; i-- {
		ev := txn.Events[i]
		if ev.Schema != schema || ev.Table != table {
			continue
		}

		var pkValues map[string]any
		switch {
		case ev.Operation == sqlredo.OpUpdate && rp.isLOBOnlyEvent(ev):
			pkValues = ev.OldValues
		case ev.Operation == sqlredo.OpInsert:
			// Use the INSERT's non-LOB columns as the PK identifier so that
			// MergeLOBsIntoDMLEvents can still match this INSERT after one of
			// its LOB columns has been overwritten with the assembled value
			// (important when an INSERT has multiple out-of-line LOBs).
			pkValues = make(map[string]any, len(ev.Data))
			for col, val := range ev.Data {
				if _, isLOB := rp.lobColTypes[prefix+strings.ToUpper(col)]; isLOB {
					continue
				}
				pkValues[col] = val
			}
		default:
			continue
		}

		pkString := sqlredo.FormatPKString(pkValues)
		{
			evDataCols := make([]string, 0, len(ev.Data))
			for c := range ev.Data {
				evDataCols = append(evDataCols, c)
			}
			rp.log.Debugf("inferLOBLocator: examining event op=%s nDataCols=%d dataCols=%v (txn=%s, scn=%d)",
				ev.Operation, len(ev.Data), evDataCols, event.TransactionID, event.SCN)
		}

		// Candidate LOB columns are those:
		//   - not already claimed by SELECT_LOB_LOCATOR (tracked in claimedCols)
		//   - absent from ev.Data: INSERT omits BASICFILE OOR columns; LOB-only UPDATE
		//     omits them from its SET clause (they never appear there for BASICFILE OOR)
		//   - present with nil (Oracle writes NULL in INSERT SQL_REDO for out-of-row LOBs)
		//   - present with an empty []byte (EMPTY_CLOB()/EMPTY_BLOB() placeholder)
		for k, lobType := range rp.lobColTypes {
			if !strings.HasPrefix(k, prefix) {
				continue
			}
			col := k[len(prefix):]
			// Skip columns already claimed by SELECT_LOB_LOCATOR, unless the
			// accumulator has no fragments yet — meaning SELECT_LOB_LOCATOR arrived
			// after INSERT but the LOB_WRITE events arrived before INSERT and are
			// sitting in the deferred queue. Route them to the existing accumulator.
			if _, claimed := claimedCols[col]; claimed {
				if existingKey, hasEmptyAcc := emptyClaimedKeys[col]; hasEmptyAcc {
					state := rp.getOrCreateLOBState(event.TransactionID)
					state.ActiveKey = &existingKey
					rp.log.Debugf("Inferred LOB locator for %s.%s.%s from empty SELECT_LOB_LOCATOR accumulator (txn=%s)",
						schema, table, col, event.TransactionID)
					return true
				}
				rp.log.Debugf("inferLOBLocator: skip %s.%s.%s — claimed with %d fragment(s) (txn=%s)",
					schema, table, col, claimedFragmentCounts[col], event.TransactionID)
				continue
			}
			val, present := ev.Data[col]
			switch {
			case present:
				// nil means Oracle wrote NULL in INSERT SQL_REDO for this LOB column
				// (BASICFILE DISABLE STORAGE IN ROW). Treat it as a valid candidate.
				if val != nil {
					if b, ok := val.([]byte); !ok || len(b) != 0 {
						rp.log.Debugf("inferLOBLocator: skip %s.%s.%s — INSERT value type=%T val=%.40v (txn=%s)",
							schema, table, col, val, val, event.TransactionID)
						continue
					}
				}
			case ev.Operation != sqlredo.OpInsert:
				// Column absent from a LOB-only UPDATE.
			}

			rp.log.Debugf("inferLOBLocator: CANDIDATE %s.%s.%s present=%v val=%T (txn=%s)",
				schema, table, col, present, val, event.TransactionID)

			key := sqlredo.LobKey{
				Schema:   schema,
				Table:    table,
				Column:   col,
				PKString: pkString,
			}

			// Defer state creation until we have a match to avoid leaking
			// empty TxnLOBState entries when inference fails.
			state := rp.getOrCreateLOBState(event.TransactionID)
			if _, exists := state.Accumulators[key]; exists {
				rp.log.Debugf("inferLOBLocator: skip %s.%s.%s — accumulator already exists for pkString=%q (txn=%s)",
					schema, table, col, pkString, event.TransactionID)
				continue
			}

			state.Accumulators[key] = &sqlredo.LobAccumulator{
				Schema:   schema,
				Table:    table,
				Column:   col,
				PKValues: pkValues,
				IsBinary: lobType == "BLOB",
			}
			state.ActiveKey = &key

			rp.log.Debugf("Inferred LOB locator for %s.%s.%s from %s (txn=%s)",
				schema, table, col, ev.Operation, event.TransactionID)
			return true
		}
	}

	// Log why inference failed: how many events we searched and how many LOB columns we know about.
	var eventsForTable int
	for _, ev := range txn.Events {
		if ev.Schema == schema && ev.Table == table {
			eventsForTable++
		}
	}
	var knownLOBCols []string
	for k := range rp.lobColTypes {
		if strings.HasPrefix(k, prefix) {
			knownLOBCols = append(knownLOBCols, k)
		}
	}
	rp.log.Debugf("inferLOBLocator: no match for %s.%s (txn=%s, scn=%d): txnEvents=%d, eventsForTable=%d, knownLOBCols=%v",
		schema, table, event.TransactionID, event.SCN, len(txn.Events), eventsForTable, knownLOBCols)
	return false
}
