// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
)

const decodingPlugin = "pgoutput"

// Stream is a structure that represents a logical replication stream
// It includes the connection to the database, the context for the stream, and snapshotting functionality
type Stream struct {
	pgConn *pgconn.PgConn

	shutSig *shutdown.Signaller

	ackedLSNMu sync.Mutex
	// The LSN acked by the stream, we may not have acked this to postgres yet (ack, ack, ack)
	ackedLSN LSN

	standbyMessageTimeout time.Duration
	messages              chan StreamMessage
	errors                chan error

	includeTxnMarkers          bool
	slotName                   string
	tables                     []TableFQN
	snapshotBatchSize          int
	decodingPluginArguments    []string
	snapshotMemorySafetyFactor float64
	logger                     *service.Logger
	monitor                    *Monitor
	heartbeat                  *heartbeat
	maxParallelSnapshotTables  int
	unchangedToastValue        any
}

// NewPgStream creates a new instance of the Stream struct
func NewPgStream(ctx context.Context, config *Config) (*Stream, error) {
	if config.ReplicationSlotName == "" {
		return nil, errors.New("missing replication slot name")
	}

	// Cleanup state - this will be accumulated as the function progresses and cleared
	// if we successfully create a stream.
	var cleanups []func()
	defer func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}()

	dbConn, err := pgconn.ConnectConfig(ctx, config.DBConfig.Copy())
	if err != nil {
		return nil, err
	}
	cleanups = append(cleanups, func() {
		if err := dbConn.Close(ctx); err != nil {
			config.Logger.Warnf("unable to properly cleanup db connection on stream creation failure: %s", err)
		}
	})

	if err = dbConn.Ping(ctx); err != nil {
		return nil, err
	}

	schema, err := sanitize.NormalizePostgresIdentifier(config.DBSchema)
	if err != nil {
		return nil, fmt.Errorf("invalid schema name %q: %w", config.DBSchema, err)
	}

	tables := []TableFQN{}
	for _, table := range config.DBTables {
		normalized, err := sanitize.NormalizePostgresIdentifier(table)
		if err != nil {
			return nil, fmt.Errorf("invalid table name %q: %w", table, err)
		}
		tables = append(tables, TableFQN{Schema: schema, Table: normalized})
	}
	stream := &Stream{
		pgConn:                     dbConn,
		messages:                   make(chan StreamMessage),
		errors:                     make(chan error, 1),
		slotName:                   config.ReplicationSlotName,
		snapshotMemorySafetyFactor: config.SnapshotMemorySafetyFactor,
		snapshotBatchSize:          config.BatchSize,
		tables:                     tables,
		maxParallelSnapshotTables:  config.MaxParallelSnapshotTables,
		logger:                     config.Logger,
		shutSig:                    shutdown.NewSignaller(),
		includeTxnMarkers:          config.IncludeTxnMarkers,
		standbyMessageTimeout:      config.PgStandbyTimeout,
		unchangedToastValue:        config.UnchangedToastValue,
	}

	monitor, err := NewMonitor(ctx, config.DBRawDSN, stream.logger, tables, stream.slotName, config.WalMonitorInterval)
	if err != nil {
		return nil, err
	}
	stream.monitor = monitor
	cleanups = append(cleanups, func() {
		if err := monitor.Stop(); err != nil {
			config.Logger.Warnf("unable to properly cleanup monitor on stream creation failure: %s", err)
		}
	})

	if config.HeartbeatInterval > 0 {
		stream.heartbeat, err = newHeartbeat(
			config.DBRawDSN,
			config.HeartbeatInterval,
			config.Logger,
			"redpanda_connect_"+stream.slotName,
			`{"type":"heartbeat"}`,
		)
		if err != nil {
			return nil, err
		}
		stream.heartbeat.Start()
		cleanups = append(cleanups, func() {
			if err := stream.heartbeat.Stop(); err != nil {
				config.Logger.Warnf("unable to properly cleanup heartbeat on stream creation failure: %s", err)
			}
		})
	}

	var version int
	version, err = getPostgresVersion(config.DBRawDSN)
	if err != nil {
		return nil, err
	}

	pluginArguments := []string{
		"proto_version '1'",
		// Sprintf is safe because we validate ReplicationSlotName is alphanumeric in the config
		fmt.Sprintf("publication_names 'pglog_stream_%s'", config.ReplicationSlotName),
	}

	if version > 14 {
		pluginArguments = append(pluginArguments, "messages 'true'")
	}

	stream.decodingPluginArguments = pluginArguments

	pubName := "pglog_stream_" + config.ReplicationSlotName
	stream.logger.Infof("Creating publication %s for tables: %s", pubName, tables)
	if err = CreatePublication(ctx, stream.pgConn, pubName, tables); err != nil {
		return nil, err
	}
	cleanups = append(cleanups, func() {
		// TODO: Drop publication if it was created (meaning it's not existing state we might want to keep).
	})

	s, err := sanitize.SQLQuery("SELECT confirmed_flush_lsn, plugin FROM pg_replication_slots WHERE slot_name = $1", config.ReplicationSlotName)
	if err != nil {
		return nil, err
	}
	connExecResult, err := stream.pgConn.Exec(ctx, s).ReadAll()
	if err != nil {
		return nil, err
	}

	if len(connExecResult) > 0 && len(connExecResult[0].Rows) > 0 {
		slotCheckRow := connExecResult[0].Rows[0]
		confirmedLSNFromDB, err := ParseLSN(string(slotCheckRow[0]))
		if err != nil {
			return nil, fmt.Errorf("unable to decode LSN from postgres: %w", err)
		}
		outputPlugin := string(slotCheckRow[1])
		// handling a case when replication slot already exists but with different output plugin created manually
		if outputPlugin != decodingPlugin {
			return nil, fmt.Errorf("replication slot %s already exists with different output plugin: %s", config.ReplicationSlotName, outputPlugin)
		}
		if confirmedLSNFromDB > 0 {
			stream.ackedLSNMu.Lock()
			stream.ackedLSN = confirmedLSNFromDB
			stream.ackedLSNMu.Unlock()
		}
		stream.logger.Debugf("starting stream from LSN %s", confirmedLSNFromDB.String())
		if err = stream.startLr(ctx, confirmedLSNFromDB); err != nil {
			return nil, err
		}
		go func() {
			defer stream.shutSig.TriggerHasStopped()
			if err := stream.streamMessages(confirmedLSNFromDB); err != nil {
				stream.errors <- fmt.Errorf("logical replication stream error: %w", err)
			}
		}()
		cleanups = nil
		return stream, nil
	}

	var snapshotter *Snapshotter
	if config.StreamOldData {
		// Create a temporary replication slot that just creates a snapshot and freezes the LSN for the snapshot.
		// We make this temporary so that if the snapshotting phase fails, we restart the snapshotting phase
		// instead of resuming from the start of the stream (with an incomplete snapshot).
		_, snapshotName, err := CreateReplicationSlot(
			ctx, stream.pgConn,
			stream.slotName+"_tmp",
			decodingPlugin,
			CreateReplicationSlotOptions{
				Temporary:      true,
				SnapshotAction: "EXPORT_SNAPSHOT",
			},
		)
		if err != nil {
			return nil, fmt.Errorf("unable to create temporary replication slot for snapshot: %w", err)
		}
		stream.logger.Tracef("exported snapshot named: %s", snapshotName)
		snapshotter, err = NewSnapshotter(config.DBRawDSN, stream.logger, snapshotName)
		if err != nil {
			return nil, err
		}
	}

	go func() {
		defer stream.shutSig.TriggerHasStopped()
		ctx, done := stream.shutSig.SoftStopCtx(context.Background())
		defer done()
		var startLSN LSN
		if snapshotter != nil {
			if err = stream.processSnapshot(ctx, snapshotter); err != nil {
				stream.errors <- fmt.Errorf("failed to process snapshot: %w", err)
				return
			}
			// TODO: Do we want to ensure all snapshot messages are ack'd before moving
			// onto the replication stream?

			// Now that the snapshot has been processed, we can copy the replication
			// slot, represerving the LSN but making it not temporary.
			// This action also expires the snapshot.
			startLSN, err = CopyReplicationSlot(
				ctx,
				stream.pgConn,
				stream.slotName+"_tmp",
				stream.slotName,
				config.TemporaryReplicationSlot,
			)
			if err == nil {
				// Drop our temporary name, we don't need it anymore.
				err = DropReplicationSlot(
					ctx,
					stream.pgConn,
					stream.slotName+"_tmp",
					DropReplicationSlotOptions{Wait: false},
				)
			}
			if err != nil {
				stream.errors <- fmt.Errorf("failed to create streaming replication slot: %w", err)
				return
			}
		} else {
			startLSN, _, err = CreateReplicationSlot(
				ctx,
				stream.pgConn,
				stream.slotName,
				decodingPlugin,
				CreateReplicationSlotOptions{
					Temporary:      config.TemporaryReplicationSlot,
					SnapshotAction: "NOEXPORT_SNAPSHOT",
				},
			)
			if err != nil {
				stream.errors <- fmt.Errorf("failed to create replication slot: %w", err)
				return
			}
		}
		stream.ackedLSNMu.Lock()
		stream.ackedLSN = startLSN
		stream.ackedLSNMu.Unlock()
		if err := stream.startLr(ctx, startLSN); err != nil {
			stream.errors <- fmt.Errorf("failed to start logical replication: %w", err)
			return
		}
		if err := stream.streamMessages(startLSN); err != nil {
			stream.errors <- fmt.Errorf("logical replication stream error: %w", err)
		}
	}()

	// Success! No need to cleanup
	cleanups = nil
	return stream, nil
}

// GetProgress returns the progress of the stream.
// including the % of snapshot messages processed and the WAL lag in bytes.
func (s *Stream) GetProgress() *Report {
	return s.monitor.Report()
}

func (s *Stream) startLr(ctx context.Context, lsnStart LSN) error {
	err := StartReplication(
		ctx,
		s.pgConn,
		s.slotName,
		lsnStart,
		StartReplicationOptions{
			PluginArgs: s.decodingPluginArguments,
		},
	)
	if err != nil {
		return err
	}
	s.logger.Debugf("Started logical replication on slot slot-name: %v", s.slotName)
	return nil
}

// AckLSN acknowledges the LSN up to which the stream has processed the messages.
// This makes Postgres to remove the WAL files that are no longer needed.
func (s *Stream) AckLSN(ctx context.Context, lsn string) error {
	parsed, err := ParseLSN(lsn)
	if err != nil {
		return fmt.Errorf("unable to parse LSN: %w", err)
	}
	s.ackedLSNMu.Lock()
	defer s.ackedLSNMu.Unlock()
	if s.shutSig.IsHardStopSignalled() {
		return fmt.Errorf("unable to ack LSN %s stream shutting down", lsn)
	}
	s.ackedLSN = parsed
	return nil
}

func (s *Stream) getAckedLSN() LSN {
	s.ackedLSNMu.Lock()
	ackedLSN := s.ackedLSN
	s.ackedLSNMu.Unlock()
	return ackedLSN
}

func (s *Stream) commitAckedLSN(ctx context.Context, lsn LSN) error {
	err := SendStandbyStatusUpdate(
		ctx,
		s.pgConn,
		StandbyStatusUpdate{
			WALWritePosition: lsn + 1,
			ReplyRequested:   true,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to send standby status message at LSN %s: %w", lsn, err)
	}
	return nil
}

func (s *Stream) streamMessages(currentLSN LSN) error {
	relations := map[uint32]*RelationMessage{}
	typeMap := pgtype.NewMap()
	// If we don't stream commit messages we could not ack them, which means postgres will replay the whole transaction
	// so if we're at the end of a stream and we get an ack for the last message in a txn, we need to ack the txn not the
	// last message.
	lastEmittedLSN := currentLSN
	lastEmittedCommitLSN := currentLSN

	commitLSN := func(force bool) (committed bool, err error) {
		ctx, done := s.shutSig.HardStopCtx(context.Background())
		defer done()
		ackedLSN := s.getAckedLSN()
		if ackedLSN == lastEmittedLSN {
			ackedLSN = lastEmittedCommitLSN
		}
		if force || ackedLSN > currentLSN {
			if err := s.commitAckedLSN(ctx, ackedLSN); err != nil {
				return false, err
			}
			// Update the currentLSN
			currentLSN = ackedLSN
			return true, nil
		}
		return false, nil
	}
	defer func() {
		if _, err := commitLSN(false); err != nil {
			s.logger.Errorf("unable to acknowledge LSN on stream shutdown: %v", err)
		}
	}()

	nextStandbyMessageDeadline := time.Now().Add(s.standbyMessageTimeout)
	ctx, done := s.shutSig.SoftStopCtx(context.Background())
	defer done()
	for !s.shutSig.IsSoftStopSignalled() {
		if committed, err := commitLSN(time.Now().After(nextStandbyMessageDeadline)); err != nil {
			return err
		} else if committed {
			nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)
		}
		recvCtx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
		rawMsg, err := s.pgConn.ReceiveMessage(recvCtx)
		cancel() // don't leak goroutine
		hitStandbyTimeout := errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil
		if err != nil {
			if hitStandbyTimeout || pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("failed to receive messages from Postgres: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("received error message from Postgres: %v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			s.logger.Warnf("received unexpected message: %T", rawMsg)
			continue
		}

		if len(msg.Data) == 0 {
			s.logger.Warn("received malformatted with no data")
			continue
		}
		switch msg.Data[0] {
		case PrimaryKeepaliveMessageByteID:
			pkm, err := ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("failed to parse PrimaryKeepaliveMessage: %w", err)
			}
			if pkm.ReplyRequested {
				nextStandbyMessageDeadline = time.Time{}
			}

		// XLogDataByteID is the message type for the actual WAL data
		// It will cause the stream to process WAL changes and create the corresponding messages
		case XLogDataByteID:
			xld, err := ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("failed to parse XLogData: %w", err)
			}
			msgLSN := xld.WALStart + LSN(len(xld.WALData))
			result, err := s.processChange(ctx, msgLSN, xld, relations, typeMap)
			if err != nil {
				return fmt.Errorf("decoding postgres changes failed: %w", err)
			}
			// See the explaination above about lastEmittedCommitLSN but if this is a commit message, we want to
			// only remap the commit of the last message in a transaction, so only update the remapped value if
			// it was a suppressed commit, otherwise we just provide a noop mapping of commit LSN
			if result == changeResultSuppressedCommitMessage {
				lastEmittedCommitLSN = msgLSN
			} else if result == changeResultEmittedMessage {
				lastEmittedLSN = msgLSN
				lastEmittedCommitLSN = msgLSN
			}
		default:
			return fmt.Errorf("unknown message type: %c", msg.Data[0])
		}
	}
	// clean shutdown, return nil
	return nil
}

type processChangeResult int

const (
	changeResultNoMessage               = 0
	changeResultSuppressedCommitMessage = 1
	changeResultEmittedMessage          = 2
)

// Handle handles the pgoutput output
func (s *Stream) processChange(ctx context.Context, msgLSN LSN, xld XLogData, relations map[uint32]*RelationMessage, typeMap *pgtype.Map) (processChangeResult, error) {
	logicalMsg, err := Parse(xld.WALData)
	if err != nil {
		return changeResultNoMessage, err
	}
	// parse changes inside the transaction
	message, err := toStreamMessage(logicalMsg, relations, typeMap, s.unchangedToastValue)
	if err != nil {
		return changeResultNoMessage, err
	}
	if message == nil {
		// In the case of heartbeats we can treat that the same as suppressed commit messages and advance the LSN that way.
		// this is only needed for low frequency tables to continue to progress the LSN.
		if logicalMsg, ok := logicalMsg.(*LogicalDecodingMessage); ok && logicalMsg.Prefix == "redpanda_connect_"+s.slotName {
			return changeResultSuppressedCommitMessage, nil
		}
		return changeResultNoMessage, nil
	}

	if !s.includeTxnMarkers {
		switch message.Operation {
		case CommitOpType:
			return changeResultSuppressedCommitMessage, nil
		case BeginOpType:
			return changeResultNoMessage, nil
		}
	}

	lsn := msgLSN.String()
	message.LSN = &lsn
	select {
	case s.messages <- *message:
		return changeResultEmittedMessage, nil
	case <-ctx.Done():
		return changeResultNoMessage, ctx.Err()
	}
}

func (s *Stream) processSnapshot(ctx context.Context, snapshotter *Snapshotter) error {
	if err := snapshotter.prepare(ctx); err != nil {
		return fmt.Errorf("failed to prepare database snapshot - snapshot may be expired: %w", err)
	}
	defer func() {
		s.logger.Debugf("Finished snapshot processing")
		if err := snapshotter.releaseSnapshot(); err != nil {
			s.logger.Warnf("Failed to release database snapshot: %v", err.Error())
		}
		if err := snapshotter.closeConn(); err != nil {
			s.logger.Warnf("Failed to close database connection: %v", err.Error())
		}
	}()

	s.logger.Debugf("Starting snapshot processing")
	var wg errgroup.Group
	wg.SetLimit(s.maxParallelSnapshotTables)

	for _, table := range s.tables {
		tableName := table
		wg.Go(func() (err error) {
			s.logger.Debugf("Processing snapshot for table: %v", table)

			unquotedTable, err := sanitize.UnquotePostgresIdentifier(table.Table)
			if err != nil {
				return fmt.Errorf("unexpected failure to unquote table name: %w", err)
			}
			unquotedSchema, err := sanitize.UnquotePostgresIdentifier(table.Schema)
			if err != nil {
				return fmt.Errorf("unexpected failure to unquote schema name: %w", err)
			}

			avgRowSizeBytes, numRows, err := snapshotter.tableStats(ctx, table)
			if err != nil {
				return fmt.Errorf("failed to calculate average row size for table %v: %w", table, err)
			}

			availableMemory := getAvailableMemory()
			batchSize := snapshotter.calculateBatchSize(availableMemory, uint64(avgRowSizeBytes))
			if s.snapshotBatchSize > 0 {
				batchSize = s.snapshotBatchSize
			}

			s.logger.Debugf("Querying snapshot batch_side: %v, available_memory: %v, avg_row_size: %v, num rows: %v", batchSize, availableMemory, avgRowSizeBytes, numRows)

			lastPrimaryKey, primaryKeyColumns, err := s.getPrimaryKeyColumn(ctx, table)
			if err != nil {
				return fmt.Errorf("failed to get primary key column for table %v: %w", table, err)
			}

			if len(lastPrimaryKey) == 0 {
				return fmt.Errorf("failed to get primary key column for table %s", table)
			}

			var lastPkVals = map[string]any{}

			var offset int
			for {
				var snapshotRows *sql.Rows
				queryStart := time.Now()
				if offset == 0 {
					snapshotRows, err = snapshotter.querySnapshotData(ctx, table, nil, primaryKeyColumns, batchSize)
				} else {
					snapshotRows, err = snapshotter.querySnapshotData(ctx, table, lastPkVals, primaryKeyColumns, batchSize)
				}
				if err != nil {
					return fmt.Errorf("failed to query snapshot data for table %v: %w", table, err)
				}

				queryDuration := time.Since(queryStart)
				s.logger.Tracef("Query duration: %v %s \n", queryDuration, tableName)

				if snapshotRows.Err() != nil {
					return fmt.Errorf("failed to get snapshot data for table %v: %w", table, snapshotRows.Err())
				}

				columnTypes, err := snapshotRows.ColumnTypes()
				if err != nil {
					return fmt.Errorf("failed to get column types for table %v: %w", table, err)
				}

				columnNames, err := snapshotRows.Columns()
				if err != nil {
					return fmt.Errorf("failed to get column names for table %v: %w", table, err)
				}

				var rowsCount = 0
				rowsStart := time.Now()
				totalScanDuration := time.Duration(0)
				sendDuration := time.Duration(0)

				for snapshotRows.Next() {
					rowsCount += 1

					scanStart := time.Now()
					scanArgs, valueGetters := snapshotter.prepareScannersAndGetters(columnTypes)
					err := snapshotRows.Scan(scanArgs...)
					scanEnd := time.Since(scanStart)
					totalScanDuration += scanEnd

					if err != nil {
						return fmt.Errorf("failed to scan row for table %v: %v", table, err.Error())
					}

					var data = make(map[string]any)
					for i, getter := range valueGetters {
						col := columnNames[i]
						var val any
						if val, err = getter(scanArgs[i]); err != nil {
							return fmt.Errorf("unable to decode column %s: %w", col, err)
						}
						data[col] = val
						normalized := sanitize.QuotePostgresIdentifier(col)
						if _, ok := lastPrimaryKey[normalized]; ok {
							lastPkVals[normalized] = val
						}
					}

					snapshotChangePacket := StreamMessage{
						LSN:       nil,
						Operation: ReadOpType,
						Table:     unquotedTable,
						Schema:    unquotedSchema,
						Data:      data,
					}

					if rowsCount%100 == 0 {
						s.monitor.UpdateSnapshotProgressForTable(table, rowsCount+offset)
					}

					waitingFromBenthos := time.Now()
					select {
					case s.messages <- snapshotChangePacket:
					case <-s.shutSig.SoftStopChan():
						return nil
					}
					sendDuration += time.Since(waitingFromBenthos)
				}

				if snapshotRows.Err() != nil {
					return fmt.Errorf("failed to close snapshot data iterator for table %v: %w", table, snapshotRows.Err())
				}

				batchEnd := time.Since(rowsStart)
				s.logger.Debugf("Batch duration: %v %s \n", batchEnd, tableName)
				s.logger.Debugf("Scan duration %v %s\n", totalScanDuration, tableName)
				s.logger.Debugf("Send duration %v %s\n", sendDuration, tableName)

				offset += batchSize

				if rowsCount < batchSize {
					break
				}
			}
			return nil
		})
	}
	return wg.Wait()
}

// Messages is a channel that can be used to consume messages from the plugin. It will contain LSN nil for snapshot messages
func (s *Stream) Messages() chan StreamMessage {
	return s.messages
}

// Errors is a channel that can be used to see if and error has occured internally and the stream should be restarted
func (s *Stream) Errors() chan error {
	return s.errors
}

func (s *Stream) getPrimaryKeyColumn(ctx context.Context, table TableFQN) (map[string]any, []string, error) {
	/// Query to get all primary key columns in their correct order
	q, err := sanitize.SQLQuery(`
        SELECT a.attname
        FROM   pg_index i
        JOIN   pg_attribute a ON a.attrelid = i.indrelid
            AND a.attnum = ANY(i.indkey)
        WHERE  i.indrelid = $1::regclass
        AND    i.indisprimary
        ORDER BY array_position(i.indkey, a.attnum);
    `, table.String())

	if err != nil {
		return nil, nil, fmt.Errorf("failed to sanitize query: %w", err)
	}

	reader := s.pgConn.Exec(ctx, q)
	data, err := reader.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read query results: %w", err)
	}

	if len(data) == 0 || len(data[0].Rows) == 0 {
		return nil, nil, fmt.Errorf("no primary key found for table %s", table)
	}

	// Extract all primary key column names
	pkColumns := make([]string, len(data[0].Rows))
	for i, row := range data[0].Rows {
		// Postgres gives us back normalized identifiers here - we need to quote them.
		pkColumns[i] = sanitize.QuotePostgresIdentifier(string(row[0]))
	}

	var pksMap = make(map[string]any)
	for _, pk := range pkColumns {
		pksMap[pk] = nil
	}

	return pksMap, pkColumns, nil
}

// Stop closes the stream (hopefully gracefully)
func (s *Stream) Stop(ctx context.Context) error {
	s.shutSig.TriggerSoftStop()
	var wg errgroup.Group
	stopNowCtx, done := s.shutSig.HardStopCtx(ctx)
	defer done()
	wg.Go(func() error {
		return s.pgConn.Close(stopNowCtx)
	})
	wg.Go(func() error {
		return s.monitor.Stop()
	})
	wg.Go(func() error {
		if s.heartbeat != nil {
			return s.heartbeat.Stop()
		}
		return nil
	})
	select {
	case <-ctx.Done():
	case <-s.shutSig.HasStoppedChan():
		return wg.Wait()
	}
	s.shutSig.TriggerHardStop()
	err := wg.Wait()
	select {
	case <-time.After(time.Second):
		if err == nil {
			return errors.New("unable to cleanly shutdown postgres logical replication stream")
		}
	case <-s.shutSig.HasStoppedChan():
	}
	return err
}
