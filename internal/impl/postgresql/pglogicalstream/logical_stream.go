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
	"slices"
	"strings"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/sync/errgroup"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/sanitize"
	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/watermark"
)

// Stream is a structure that represents a logical replication stream
// It includes the connection to the database, the context for the stream, and snapshotting functionality
type Stream struct {
	pgConn *pgconn.PgConn

	shutSig *shutdown.Signaller

	clientXLogPos *watermark.Value[LSN]

	standbyMessageTimeout      time.Duration
	nextStandbyMessageDeadline time.Time
	messages                   chan StreamMessage
	errors                     chan error

	snapshotName string
	slotName     string
	schema       string
	// includes schema
	tableQualifiedName         []string
	snapshotBatchSize          int
	decodingPlugin             DecodingPlugin
	decodingPluginArguments    []string
	snapshotMemorySafetyFactor float64
	logger                     *service.Logger
	monitor                    *Monitor
	batchTransactions          bool
	snapshotter                *Snapshotter
	maxParallelSnapshotTables  int
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

	tableNames := slices.Clone(config.DBTables)
	for i, table := range tableNames {
		tableNames[i] = fmt.Sprintf("%s.%s", config.DBSchema, table)
	}
	stream := &Stream{
		pgConn:                     dbConn,
		messages:                   make(chan StreamMessage),
		errors:                     make(chan error, 1),
		slotName:                   config.ReplicationSlotName,
		snapshotMemorySafetyFactor: config.SnapshotMemorySafetyFactor,
		batchTransactions:          config.BatchTransactions,
		snapshotBatchSize:          config.BatchSize,
		schema:                     config.DBSchema,
		tableQualifiedName:         tableNames,
		maxParallelSnapshotTables:  config.MaxParallelSnapshotTables,
		logger:                     config.Logger,
		decodingPlugin:             decodingPluginFromString(config.DecodingPlugin),
		shutSig:                    shutdown.NewSignaller(),
	}

	var version int
	version, err = getPostgresVersion(config.DBRawDSN)
	if err != nil {
		return nil, err
	}

	snapshotter, err := NewSnapshotter(config.DBRawDSN, stream.logger, version)
	if err != nil {
		return nil, err
	}
	stream.snapshotter = snapshotter
	cleanups = append(cleanups, func() {
		if err := snapshotter.closeConn(); err != nil {
			config.Logger.Warnf("unable to properly cleanup snapshotter connection on stream creation failure: %s", err)
		}
	})

	var pluginArguments []string
	if stream.decodingPlugin == "pgoutput" {
		pluginArguments = []string{
			"proto_version '1'",
			// Sprintf is safe because we validate ReplicationSlotName is alphanumeric in the config
			fmt.Sprintf("publication_names 'pglog_stream_%s'", config.ReplicationSlotName),
		}

		if version > 14 {
			pluginArguments = append(pluginArguments, "messages 'true'")
		}
	} else if stream.decodingPlugin == "wal2json" {
		tablesFilterRule := strings.Join(tableNames, ", ")
		pluginArguments = []string{
			"\"pretty-print\" 'true'",
			// TODO: Validate this is escaped properly
			fmt.Sprintf(`"add-tables" '%s'`, tablesFilterRule),
		}
	} else {
		return nil, fmt.Errorf("unknown decoding plugin: %q", stream.decodingPlugin)
	}

	stream.decodingPluginArguments = pluginArguments

	pubName := "pglog_stream_" + config.ReplicationSlotName
	stream.logger.Infof("Creating publication %s for tables: %s", pubName, tableNames)
	if err = CreatePublication(ctx, stream.pgConn, pubName, tableNames); err != nil {
		return nil, err
	}
	cleanups = append(cleanups, func() {
		// TODO: Drop publication if it was created (meaning it's not existing state we might want to keep).
	})

	sysident, err := IdentifySystem(ctx, stream.pgConn)
	if err != nil {
		return nil, err
	}

	var freshlyCreatedSlot = false
	var confirmedLSNFromDB string
	var outputPlugin string
	// check is replication slot exist to get last restart SLN

	s, err := sanitize.SQLQuery("SELECT confirmed_flush_lsn, plugin FROM pg_replication_slots WHERE slot_name = $1", config.ReplicationSlotName)
	if err != nil {
		return nil, err
	}
	connExecResult, err := stream.pgConn.Exec(ctx, s).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(connExecResult) == 0 || len(connExecResult[0].Rows) == 0 {
		// here we create a new replication slot because there is no slot found
		var createSlotResult CreateReplicationSlotResult
		createSlotResult, err = CreateReplicationSlot(
			ctx,
			stream.pgConn,
			stream.slotName,
			stream.decodingPlugin.String(),
			CreateReplicationSlotOptions{
				Temporary:      config.TemporaryReplicationSlot,
				SnapshotAction: "export",
			},
			version,
			stream.snapshotter,
		)
		if err != nil {
			return nil, err
		}
		stream.snapshotName = createSlotResult.SnapshotName
		freshlyCreatedSlot = true
		cleanups = append(cleanups, func() {
			err := DropReplicationSlot(ctx, stream.pgConn, stream.slotName, DropReplicationSlotOptions{Wait: true})
			if err != nil {
				config.Logger.Warnf("unable to properly cleanup replication slot on stream creation failure: %s", err)
			}
		})
	} else {
		slotCheckRow := connExecResult[0].Rows[0]
		confirmedLSNFromDB = string(slotCheckRow[0])
		outputPlugin = string(slotCheckRow[1])
	}

	if !freshlyCreatedSlot && outputPlugin != stream.decodingPlugin.String() {
		return nil, fmt.Errorf("replication slot %s already exists with different output plugin: %s", config.ReplicationSlotName, outputPlugin)
	}

	var lsnrestart LSN
	if freshlyCreatedSlot {
		lsnrestart = sysident.XLogPos
	} else {
		lsnrestart, _ = ParseLSN(confirmedLSNFromDB)
	}
	stream.clientXLogPos = watermark.New(lsnrestart)

	stream.standbyMessageTimeout = config.PgStandbyTimeout
	stream.nextStandbyMessageDeadline = time.Now().Add(stream.standbyMessageTimeout)

	monitor, err := NewMonitor(config.DBRawDSN, stream.logger, tableNames, stream.slotName, config.WalMonitorInterval)
	if err != nil {
		return nil, err
	}
	stream.monitor = monitor
	cleanups = append(cleanups, func() {
		if err := monitor.Stop(); err != nil {
			config.Logger.Warnf("unable to properly cleanup monitor on stream creation failure: %s", err)
		}
	})

	stream.logger.Debugf("starting stream from LSN %s with clientXLogPos %s and snapshot name %s", lsnrestart.String(), stream.clientXLogPos.Get().String(), stream.snapshotName)
	// TODO(le-vlad): if snapshot processing is restarted we will just skip right to streaming...
	if !freshlyCreatedSlot || !config.StreamOldData {
		if err = stream.startLr(ctx, lsnrestart); err != nil {
			return nil, err
		}

		go func() {
			defer stream.shutSig.TriggerHasStopped()
			if err := stream.streamMessages(); err != nil {
				stream.errors <- fmt.Errorf("logical replication stream error: %w", err)
			}
		}()
	} else {
		go func() {
			defer stream.shutSig.TriggerHasStopped()
			if err := stream.processSnapshot(); err != nil {
				stream.errors <- fmt.Errorf("failed to process snapshot: %w", err)
				return
			}
			ctx, _ := stream.shutSig.SoftStopCtx(context.Background())
			if err := stream.startLr(ctx, lsnrestart); err != nil {
				stream.errors <- fmt.Errorf("failed to start logical replication: %w", err)
				return
			}
			if err := stream.streamMessages(); err != nil {
				stream.errors <- fmt.Errorf("logical replication stream error: %w", err)
			}
		}()
	}

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
	if s.shutSig.IsHardStopSignalled() {
		return fmt.Errorf("unable to ack LSN %s stream shutting down", lsn)
	}
	clientXLogPos, err := ParseLSN(lsn)
	if err != nil {
		return err
	}

	err = SendStandbyStatusUpdate(
		ctx,
		s.pgConn,
		StandbyStatusUpdate{
			WALApplyPosition: clientXLogPos + 1,
			WALWritePosition: clientXLogPos + 1,
			WALFlushPosition: clientXLogPos + 1,
			ReplyRequested:   true,
		},
	)

	if err != nil {
		return fmt.Errorf("failed to send Standby status message at LSN %s: %w", clientXLogPos.String(), err)
	}

	// Update client XLogPos after we ack the message
	s.clientXLogPos.Set(clientXLogPos)
	s.logger.Debugf("Sent Standby status message at LSN#%s", clientXLogPos.String())
	s.nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)

	return nil
}

func (s *Stream) streamMessages() error {
	var handler PluginHandler
	switch s.decodingPlugin {
	case "wal2json":
		handler = NewWal2JsonPluginHandler(s.messages, s.monitor)
	case "pgoutput":
		handler = NewPgOutputPluginHandler(s.messages, s.batchTransactions, s.monitor, s.clientXLogPos)
	default:
		return fmt.Errorf("invalid decoding plugin: %q", s.decodingPlugin)
	}

	ctx, _ := s.shutSig.SoftStopCtx(context.Background())
	for !s.shutSig.IsSoftStopSignalled() {
		if time.Now().After(s.nextStandbyMessageDeadline) {
			pos := s.clientXLogPos.Get()
			err := SendStandbyStatusUpdate(
				ctx,
				s.pgConn,
				StandbyStatusUpdate{
					WALWritePosition: pos,
				},
			)
			if err != nil {
				return fmt.Errorf("unable to send standby status message at LSN %s: %w", pos, err)
			}
			s.logger.Debugf("Sent Standby status message at LSN#%s", pos.String())
			s.nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)
		}
		recvCtx, cancel := context.WithDeadline(ctx, s.nextStandbyMessageDeadline)
		rawMsg, err := s.pgConn.ReceiveMessage(recvCtx)
		cancel() // don't leak goroutine
		hitStandbyTimeout := errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil
		if err != nil {
			if hitStandbyTimeout || pgconn.Timeout(err) {
				s.logger.Info("continue")
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
				s.nextStandbyMessageDeadline = time.Time{}
			}

		// XLogDataByteID is the message type for the actual WAL data
		// It will cause the stream to process WAL changes and create the corresponding messages
		case XLogDataByteID:
			xld, err := ParseXLogData(msg.Data[1:])
			if err != nil {
				return fmt.Errorf("failed to parse XLogData: %w", err)
			}
			clientXLogPos := xld.WALStart + LSN(len(xld.WALData))
			commit, err := handler.Handle(ctx, clientXLogPos, xld)
			if err != nil {
				return fmt.Errorf("decoding postgres changes failed: %w", err)
			} else if commit {
				// This is a hack and we probably should not do it
				if err = s.AckLSN(ctx, clientXLogPos.String()); err != nil {
					s.logger.Warnf("Failed to ack commit message LSN: %v", err)
				}
			}
		}
	}
	// clean shutdown, return nil
	return nil
}

func (s *Stream) processSnapshot() error {
	if err := s.snapshotter.prepare(); err != nil {
		return fmt.Errorf("failed to prepare database snapshot - snapshot may be expired: %w", err)
	}
	defer func() {
		if err := s.snapshotter.releaseSnapshot(); err != nil {
			s.logger.Warnf("Failed to release database snapshot: %v", err.Error())
		}
		if err := s.snapshotter.closeConn(); err != nil {
			s.logger.Warnf("Failed to close database connection: %v", err.Error())
		}
	}()

	s.logger.Debugf("Starting snapshot processing")
	var wg errgroup.Group
	wg.SetLimit(s.maxParallelSnapshotTables)

	for _, table := range s.tableQualifiedName {
		tableName := table
		wg.Go(func() (err error) {
			s.logger.Debugf("Processing snapshot for table: %v", table)

			var (
				avgRowSizeBytes sql.NullInt64
				offset          = 0
			)

			avgRowSizeBytes, err = s.snapshotter.findAvgRowSize(table)
			if err != nil {
				return fmt.Errorf("failed to calculate average row size for table %v: %w", table, err)
			}

			availableMemory := getAvailableMemory()
			batchSize := s.snapshotter.calculateBatchSize(availableMemory, uint64(avgRowSizeBytes.Int64))
			if s.snapshotBatchSize > 0 {
				batchSize = s.snapshotBatchSize
			}

			s.logger.Debugf("Querying snapshot batch_side: %v, available_memory: %v, avg_row_size: %v", batchSize, availableMemory, avgRowSizeBytes.Int64)

			tablePk, err := s.getPrimaryKeyColumn(table)
			if err != nil {
				return fmt.Errorf("failed to get primary key column for table %v: %w", table, err)
			}

			var lastPkVal any

			for {
				var snapshotRows *sql.Rows
				queryStart := time.Now()
				if snapshotRows, err = s.snapshotter.querySnapshotData(table, lastPkVal, tablePk, batchSize); err != nil {
					return fmt.Errorf("failed to query snapshot data for table %v: %w", table, err)
				}

				queryDuration := time.Since(queryStart)
				s.logger.Debugf("Query duration: %v %s \n", queryDuration, tableName)

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
				totalWaitingFromBenthos := time.Duration(0)

				tableWithoutSchema := strings.Split(table, ".")[1]
				for snapshotRows.Next() {
					rowsCount += 1

					scanStart := time.Now()
					scanArgs, valueGetters := s.snapshotter.prepareScannersAndGetters(columnTypes)
					err := snapshotRows.Scan(scanArgs...)
					scanEnd := time.Since(scanStart)
					totalScanDuration += scanEnd

					if err != nil {
						return fmt.Errorf("failed to scan row for table %v: %v", table, err.Error())
					}

					var data = make(map[string]any)
					for i, getter := range valueGetters {
						data[columnNames[i]] = getter(scanArgs[i])
						if columnNames[i] == tablePk {
							lastPkVal = getter(scanArgs[i])
						}
					}

					snapshotChangePacket := StreamMessage{
						Lsn: nil,
						Changes: []StreamMessageChanges{
							{
								Table:     tableWithoutSchema,
								Operation: "insert",
								Schema:    s.schema,
								Data:      data,
							},
						},
					}

					if rowsCount%100 == 0 {
						s.monitor.UpdateSnapshotProgressForTable(tableWithoutSchema, rowsCount+offset)
					}

					tableProgress := s.monitor.GetSnapshotProgressForTable(tableWithoutSchema)
					snapshotChangePacket.Changes[0].TableSnapshotProgress = &tableProgress
					snapshotChangePacket.Mode = StreamModeSnapshot

					waitingFromBenthos := time.Now()
					select {
					case s.messages <- snapshotChangePacket:
					case <-s.shutSig.SoftStopChan():
						return nil
					}
					totalWaitingFromBenthos += time.Since(waitingFromBenthos)

				}

				batchEnd := time.Since(rowsStart)
				s.logger.Debugf("Batch duration: %v %s \n", batchEnd, tableName)
				s.logger.Debugf("Scan duration %v %s\n", totalScanDuration, tableName)
				s.logger.Debugf("Waiting from benthos duration %v %s\n", totalWaitingFromBenthos, tableName)

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

func (s *Stream) getPrimaryKeyColumn(tableName string) (string, error) {
	// TODO(le-vlad): support composite primary keys
	q, err := sanitize.SQLQuery(`
		SELECT a.attname
		FROM   pg_index i
		JOIN   pg_attribute a ON a.attrelid = i.indrelid
							AND a.attnum = ANY(i.indkey)
		WHERE  i.indrelid = $1::regclass
		AND    i.indisprimary;
	`, tableName)
	if err != nil {
		return "", err
	}

	reader := s.pgConn.Exec(context.Background(), q)
	data, err := reader.ReadAll()
	if err != nil {
		return "", err
	}

	pkResultRow := data[0].Rows[0]
	pkColName := string(pkResultRow[0])
	return pkColName, nil
}

// Stop closes the stream (hopefully gracefully)
func (s *Stream) Stop(ctx context.Context) error {
	s.shutSig.TriggerSoftStop()
	var wg errgroup.Group
	stopNowCtx, _ := s.shutSig.HardStopCtx(ctx)
	wg.Go(func() error {
		return s.pgConn.Close(stopNowCtx)
	})
	wg.Go(func() error {
		return s.monitor.Stop()
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
