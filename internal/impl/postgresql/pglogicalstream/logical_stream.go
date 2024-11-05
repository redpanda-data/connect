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
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redpanda-data/benthos/v4/public/service"
	"golang.org/x/sync/errgroup"
)

// Stream is a structure that represents a logical replication stream
// It includes the connection to the database, the context for the stream, and snapshotting functionality
type Stream struct {
	pgConn       *pgconn.PgConn
	streamCtx    context.Context
	streamCancel context.CancelFunc

	standbyCtxCancel context.CancelFunc

	clientXLogPos LSN
	lsnrestart    LSN

	standbyMessageTimeout      time.Duration
	nextStandbyMessageDeadline time.Time
	messages                   chan StreamMessage
	snapshotName               string
	slotName                   string
	schema                     string
	tableNames                 []string
	snapshotBatchSize          int
	decodingPlugin             DecodingPlugin
	decodingPluginArguments    []string
	snapshotMemorySafetyFactor float64
	logger                     *service.Logger
	monitor                    *Monitor
	streamUncommitted          bool
	snapshotter                *Snapshotter
	transactionAckChan         chan string
	transactionBeginChan       chan bool

	lsnAckBuffer []string

	m       sync.Mutex
	stopped bool

	consumedCallback chan bool
}

// NewPgStream creates a new instance of the Stream struct
func NewPgStream(ctx context.Context, config *Config) (*Stream, error) {
	if config.ReplicationSlotName == "" {
		return nil, errors.New("missing replication slot name")
	}

	dbConn, err := pgconn.ConnectConfig(ctx, config.DBConfig.Copy())
	if err != nil {
		return nil, err
	}

	if err = dbConn.Ping(ctx); err != nil {
		return nil, err
	}

	tableNames := slices.Clone(config.DBTables)
	stream := &Stream{
		pgConn:                     dbConn,
		messages:                   make(chan StreamMessage),
		slotName:                   config.ReplicationSlotName,
		snapshotMemorySafetyFactor: config.SnapshotMemorySafetyFactor,
		streamUncommitted:          config.StreamUncommitted,
		snapshotBatchSize:          config.BatchSize,
		schema:                     config.DBSchema,
		tableNames:                 tableNames,
		consumedCallback:           make(chan bool),
		transactionAckChan:         make(chan string),
		transactionBeginChan:       make(chan bool),
		lsnAckBuffer:               []string{},
		logger:                     config.Logger,
		m:                          sync.Mutex{},
		decodingPlugin:             decodingPluginFromString(config.DecodingPlugin),
	}

	for i, table := range tableNames {
		tableNames[i] = fmt.Sprintf("%s.%s", config.DBSchema, table)
	}

	var version int
	version, err = getPostgresVersion(config.DBRawDSN)
	if err != nil {
		return nil, err
	}

	snapshotter, err := NewSnapshotter(config.DBRawDSN, stream.logger, version)
	if err != nil {
		stream.logger.Errorf("Failed to open SQL connection to prepare snapshot: %v", err.Error())
		if err = stream.cleanUpOnFailure(ctx); err != nil {
			stream.logger.Errorf("Failed to clean up resources on accident: %v", err.Error())
		}
		return nil, err
	}
	stream.snapshotter = snapshotter

	var pluginArguments []string
	if stream.decodingPlugin == "pgoutput" {
		pluginArguments = []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names 'pglog_stream_%s'", config.ReplicationSlotName),
		}

		if version > 14 {
			pluginArguments = append(pluginArguments, "messages 'true'")
		}
	} else if stream.decodingPlugin == "wal2json" {
		tablesFilterRule := strings.Join(tableNames, ", ")
		pluginArguments = []string{
			"\"pretty-print\" 'true'",
			"\"add-tables\"" + " " + fmt.Sprintf("'%s'", tablesFilterRule),
		}
	} else {
		return nil, fmt.Errorf("unknown decoding plugin: %q", stream.decodingPlugin)
	}

	stream.decodingPluginArguments = pluginArguments

	pubName := "pglog_stream_" + config.ReplicationSlotName
	stream.logger.Infof("Creating publication %s for tables: %s", pubName, tableNames)
	if err = CreatePublication(ctx, stream.pgConn, pubName, tableNames, true); err != nil {
		return nil, err
	}
	sysident, err := IdentifySystem(ctx, stream.pgConn)
	if err != nil {
		return nil, err
	}

	var freshlyCreatedSlot = false
	var confirmedLSNFromDB string
	var outputPlugin string
	// check is replication slot exist to get last restart SLN

	// TODO: There should be a helper method for this that also validates the parameters to fmt here are not possible to cause SQL injection.
	// this means we either escape or we validate it's only alphanumeric and `-_`
	connExecResult, err := stream.pgConn.Exec(
		ctx,
		fmt.Sprintf("SELECT confirmed_flush_lsn, plugin FROM pg_replication_slots WHERE slot_name = '%s'",
			config.ReplicationSlotName),
	).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(connExecResult) == 0 || len(connExecResult[0].Rows) == 0 {
		// here we create a new replication slot because there is no slot found
		var createSlotResult CreateReplicationSlotResult
		createSlotResult, err = CreateReplicationSlot(ctx, stream.pgConn, stream.slotName, stream.decodingPlugin.String(),
			CreateReplicationSlotOptions{Temporary: config.TemporaryReplicationSlot,
				SnapshotAction: "export",
			}, version, stream.snapshotter)
		if err != nil {
			return nil, err
		}
		stream.snapshotName = createSlotResult.SnapshotName
		freshlyCreatedSlot = true
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

	stream.lsnrestart = lsnrestart

	if freshlyCreatedSlot {
		stream.clientXLogPos = sysident.XLogPos
	} else {
		stream.clientXLogPos = lsnrestart
	}

	stream.standbyMessageTimeout = time.Second * 10
	stream.nextStandbyMessageDeadline = time.Now().Add(stream.standbyMessageTimeout)
	stream.streamCtx, stream.streamCancel = context.WithCancel(context.Background())

	monitor, err := NewMonitor(config.DBRawDSN, stream.logger, tableNames, stream.slotName)
	if err != nil {
		return nil, err
	}
	stream.monitor = monitor

	stream.logger.Debugf("Starting stream from LSN %s with clientXLogPos %s and snapshot name %s", stream.lsnrestart.String(), stream.clientXLogPos.String(), stream.snapshotName)
	if !freshlyCreatedSlot || !config.StreamOldData {
		if err = stream.startLr(); err != nil {
			return nil, err
		}

		go stream.streamMessagesAsync()
	} else {
		// New messages will be streamed after the snapshot has been processed.
		// stream.startLr() and stream.streamMessagesAsync() will be called inside stream.processSnapshot()
		go func() {
			if err := stream.processSnapshot(ctx); err != nil {
				stream.logger.Errorf("Failed to process snapshot: %v", err.Error())
			}
		}()
	}

	return stream, err
}

// GetProgress returns the progress of the stream.
// including the % of snapsho messages processed and the WAL lag in bytes.
func (s *Stream) GetProgress() *Report {
	return s.monitor.Report()
}

// ConsumedCallback returns a channel that is used to tell the plugin to commit consumed offset
func (s *Stream) ConsumedCallback() chan bool {
	return s.consumedCallback
}

func (s *Stream) startLr() error {
	if err := StartReplication(context.Background(), s.pgConn, s.slotName, s.lsnrestart, StartReplicationOptions{PluginArgs: s.decodingPluginArguments}); err != nil {
		return err
	}

	s.logger.Infof("Started logical replication on slot slot-name: %v", s.slotName)
	return nil
}

// AckLSN acknowledges the LSN up to which the stream has processed the messages.
// This makes Postgres to remove the WAL files that are no longer needed.
func (s *Stream) AckLSN(lsn string) error {
	clientXLogPos, err := ParseLSN(lsn)
	if err != nil {
		s.logger.Errorf("Failed to parse LSN for Acknowledge: %v", err)
		if err = s.Stop(); err != nil {
			s.logger.Errorf("Failed to stop the stream: %v", err)
		}

		return err
	}

	err = SendStandbyStatusUpdate(context.Background(), s.pgConn, StandbyStatusUpdate{
		WALApplyPosition: clientXLogPos,
		WALWritePosition: clientXLogPos,
		WALFlushPosition: clientXLogPos,
		ReplyRequested:   true,
	})

	if err != nil {
		s.logger.Errorf("Failed to send Standby status message at LSN#%s: %v", s.clientXLogPos.String(), err)
		return err
	}

	// Update client XLogPos after we ack the message
	s.clientXLogPos = clientXLogPos
	s.logger.Debugf("Sent Standby status message at LSN#%s", s.clientXLogPos.String())
	s.nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)

	return nil
}

func (s *Stream) streamMessagesAsync() {
	relations := map[uint32]*RelationMessage{}
	typeMap := pgtype.NewMap()
	pgoutputChanges := []StreamMessageChanges{}

	for {
		select {
		case <-s.streamCtx.Done():
			s.logger.Warn("Stream was cancelled...exiting...")
			return
		default:
			if time.Now().After(s.nextStandbyMessageDeadline) {
				if s.pgConn.IsClosed() {
					s.logger.Warn("Postgres connection is closed...stop reading from replication slot")
					return
				}

				err := SendStandbyStatusUpdate(context.Background(), s.pgConn, StandbyStatusUpdate{
					WALWritePosition: s.clientXLogPos,
				})

				if err != nil {
					s.logger.Errorf("Failed to send Standby status message at LSN#%s: %v", s.clientXLogPos.String(), err)
					if err = s.Stop(); err != nil {
						s.logger.Errorf("Failed to stop the stream: %v", err)
					}
					return
				}
				s.logger.Debugf("Sent Standby status message at LSN#%s", s.clientXLogPos.String())
				s.nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)
			}

			ctx, cancel := context.WithDeadline(context.Background(), s.nextStandbyMessageDeadline)
			rawMsg, err := s.pgConn.ReceiveMessage(ctx)
			s.standbyCtxCancel = cancel

			if err != nil && (errors.Is(err, context.Canceled) || s.stopped) {
				s.logger.Warn("Service was interrupted....stop reading from replication slot")
				return
			}

			if err != nil {
				if pgconn.Timeout(err) {
					continue
				}

				s.logger.Errorf("Failed to receive messages from PostgreSQL: %v", err)
				if err = s.Stop(); err != nil {
					s.logger.Errorf("Failed to stop the stream: %v", err)
				}
				return
			}

			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				s.logger.Errorf("Received error message from Postgres: %v", errMsg)
				if err = s.Stop(); err != nil {
					s.logger.Errorf("Failed to stop the stream: %v", err)
				}
				return
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				s.logger.Warnf("Received unexpected message: %T\n", rawMsg)
				continue
			}

			switch msg.Data[0] {
			case PrimaryKeepaliveMessageByteID:
				pkm, err := ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					s.logger.Errorf("Failed to parse PrimaryKeepaliveMessage: %v", err)
					if err = s.Stop(); err != nil {
						s.logger.Errorf("Failed to stop the stream: %v", err)
					}
				}

				if pkm.ReplyRequested {
					s.nextStandbyMessageDeadline = time.Time{}
				}

			case XLogDataByteID:
				xld, err := ParseXLogData(msg.Data[1:])
				if err != nil {
					s.logger.Errorf("Failed to parse XLogData: %v", err)
					if err = s.Stop(); err != nil {
						s.logger.Errorf("Failed to stop the stream: %v", err)
					}
				}
				clientXLogPos := xld.WALStart + LSN(len(xld.WALData))
				metrics := s.monitor.Report()
				if s.decodingPlugin == "wal2json" {
					message, err := decodeWal2JsonChanges(clientXLogPos.String(), xld.WALData)
					if err != nil {
						s.logger.Errorf("decodeWal2JsonChanges failed: %w", err)
						if err = s.Stop(); err != nil {
							s.logger.Errorf("Failed to stop the stream: %v", err)
						}
						return
					}

					if message == nil || len(message.Changes) == 0 {
						// automatic ack for empty changes
						// basically mean that the client is up-to-date,
						// but we still need to acknowledge the LSN for standby
						if err = s.AckLSN(clientXLogPos.String()); err != nil {
							// stop reading from replication slot
							// if we can't acknowledge the LSN
							if err = s.Stop(); err != nil {
								s.logger.Errorf("Failed to stop the stream: %v", err)
							}
							return
						}
					} else {
						message.WALLagBytes = &metrics.WalLagInBytes
						s.messages <- *message
					}
				}

				if s.decodingPlugin == "pgoutput" {
					if s.streamUncommitted {
						// parse changes inside the transaction
						message, err := decodePgOutput(xld.WALData, relations, typeMap)
						if err != nil {
							s.logger.Errorf("decodePgOutput failed: %w", err)
							if err = s.Stop(); err != nil {
								s.logger.Errorf("Failed to stop the stream: %v", err)
							}
							return
						}

						isCommit, _, err := isCommitMessage(xld.WALData)
						if err != nil {
							s.logger.Errorf("Failed to parse WAL data: %w", err)
							if err = s.Stop(); err != nil {
								s.logger.Errorf("Failed to stop the stream: %v", err)
							}
							return
						}

						// when receiving a commit message, we need to acknowledge the LSN
						// but we must wait for benthos to flush the messages before we can do that
						if isCommit {
							s.transactionAckChan <- clientXLogPos.String()
							<-s.consumedCallback
						} else {
							if message == nil && !isCommit {
								// 0 changes happened in the transaction
								// or we received a change that are not supported/needed by the replication stream
								if err = s.AckLSN(clientXLogPos.String()); err != nil {
									// stop reading from replication slot
									// if we can't acknowledge the LSN
									if err = s.Stop(); err != nil {
										s.logger.Errorf("Failed to stop the stream: %v", err)
									}
									return
								}
							} else if message != nil {
								lsn := clientXLogPos.String()
								s.messages <- StreamMessage{
									Lsn: &lsn,
									Changes: []StreamMessageChanges{
										*message,
									},
									IsStreaming: true,
									WALLagBytes: &metrics.WalLagInBytes,
								}
							}
						}
					} else {
						// message changes must be collected in the buffer in the context of the same transaction
						// as single transaction can contain multiple changes
						// and LSN ack will cause potential loss of changes
						isBegin, err := isBeginMessage(xld.WALData)
						if err != nil {
							s.logger.Errorf("Failed to parse WAL data: %w", err)
							if err = s.Stop(); err != nil {
								s.logger.Errorf("Failed to stop the stream: %v", err)
							}
							return
						}

						if isBegin {
							pgoutputChanges = []StreamMessageChanges{}
						}

						// parse changes inside the transaction
						message, err := decodePgOutput(xld.WALData, relations, typeMap)
						if err != nil {
							s.logger.Errorf("decodePgOutput failed: %w", err)
							if err = s.Stop(); err != nil {
								s.logger.Errorf("Failed to stop the stream: %v", err)
							}
							return
						}

						if message != nil {
							pgoutputChanges = append(pgoutputChanges, *message)
						}

						isCommit, _, err := isCommitMessage(xld.WALData)
						if err != nil {
							s.logger.Errorf("Failed to parse WAL data: %w", err)
							if err = s.Stop(); err != nil {
								s.logger.Errorf("Failed to stop the stream: %v", err)
							}
							return
						}

						if isCommit {
							if len(pgoutputChanges) == 0 {
								// 0 changes happened in the transaction
								// or we received a change that are not supported/needed by the replication stream
								if err = s.AckLSN(clientXLogPos.String()); err != nil {
									// stop reading from replication slot
									// if we can't acknowledge the LSN
									if err = s.Stop(); err != nil {
										s.logger.Errorf("Failed to stop the stream: %v", err)
									}
									return
								}
							} else {
								// send all collected changes
								lsn := clientXLogPos.String()
								s.messages <- StreamMessage{
									Lsn:         &lsn,
									Changes:     pgoutputChanges,
									IsStreaming: true,
									WALLagBytes: &metrics.WalLagInBytes,
								}
							}
						}
					}
				}
			}
		}
	}
}

// AckTxChan returns the transaction ack channel
func (s *Stream) AckTxChan() chan string {
	return s.transactionAckChan
}

func (s *Stream) processSnapshot(ctx context.Context) error {
	if err := s.snapshotter.prepare(); err != nil {
		s.logger.Errorf("Failed to prepare database snapshot. Probably snapshot is expired...: %v", err.Error())
		if err = s.cleanUpOnFailure(ctx); err != nil {
			s.logger.Errorf("Failed to clean up resources on accident: %v", err.Error())
		}
		return err
	}
	defer func() {
		if err := s.snapshotter.releaseSnapshot(); err != nil {
			s.logger.Errorf("Failed to release database snapshot: %v", err.Error())
		}
		if err := s.snapshotter.closeConn(); err != nil {
			s.logger.Errorf("Failed to close database connection: %v", err.Error())
		}
	}()

	s.logger.Infof("Starting snapshot processing")

	var wg errgroup.Group

	for _, table := range s.tableNames {
		tableName := table
		wg.Go(func() (err error) {
			s.logger.Infof("Processing snapshot for table: %v", table)

			defer func() {
				if err != nil {
					if cleanupErr := s.cleanUpOnFailure(ctx); cleanupErr != nil {
						s.logger.Errorf("Failed to clean up resources on accident: %v", cleanupErr.Error())
					}
				}
			}()

			var (
				avgRowSizeBytes sql.NullInt64
				offset          = 0
			)

			avgRowSizeBytes, err = s.snapshotter.findAvgRowSize(table)
			if err != nil {
				s.logger.Errorf("Failed to calculate average row size for table %v: %v", table, err.Error())

				return err
			}

			availableMemory := getAvailableMemory()
			batchSize := s.snapshotter.calculateBatchSize(availableMemory, uint64(avgRowSizeBytes.Int64))
			if s.snapshotBatchSize > 0 {
				batchSize = s.snapshotBatchSize
			}

			s.logger.Infof("Querying snapshot batch_side: %v, available_memory: %v, avg_row_size: %v", batchSize, availableMemory, avgRowSizeBytes.Int64)

			tablePk, err := s.getPrimaryKeyColumn(table)
			if err != nil {
				s.logger.Errorf("Failed to get primary key column for table %v: %v", table, err.Error())
				return err
			}

			for {
				var snapshotRows *sql.Rows
				queryStart := time.Now()
				if snapshotRows, err = s.snapshotter.querySnapshotData(table, tablePk, batchSize, offset); err != nil {
					s.logger.Errorf("Failed to query snapshot data for table %v: %v", table, err.Error())
					s.logger.Errorf("Failed to query snapshot for table %v: %v", table, err.Error())
					return err
				}

				queryDuration := time.Since(queryStart)
				s.logger.Debugf("Query duration: %v %s \n", queryDuration, tableName)

				if snapshotRows.Err() != nil {
					s.logger.Errorf("Failed to get snapshot data for table %v: %v", table, snapshotRows.Err().Error())
					s.logger.Errorf("Failed to query snapshot for table %v: %v", table, err.Error())
					return err
				}

				columnTypes, err := snapshotRows.ColumnTypes()
				if err != nil {
					s.logger.Errorf("Failed to get column types for table %v: %v", table, err.Error())
					return err
				}

				columnNames, err := snapshotRows.Columns()
				if err != nil {
					s.logger.Errorf("Failed to get column names for table %v: %v", table, err.Error())
					return err
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
						s.logger.Errorf("Failed to scan row for table %v: %v", table, err.Error())
						return err
					}

					var data = make(map[string]any)
					for i, getter := range valueGetters {
						data[columnNames[i]] = getter(scanArgs[i])
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
					snapshotChangePacket.IsStreaming = false

					waitingFromBenthos := time.Now()
					s.messages <- snapshotChangePacket
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

	if err := wg.Wait(); err != nil {
		return err
	}

	if err := s.startLr(); err != nil {
		s.logger.Errorf("Failed to start logical replication after snapshot: %v", err.Error())
		return err
	}
	go s.streamMessagesAsync()
	return nil
}

// Messages is a channel that can be used to consume messages from the plugin. It will contain LSN nil for snapshot messages
func (s *Stream) Messages() chan StreamMessage {
	return s.messages
}

// cleanUpOnFailure drops replication slot and publication if database snapshotting was failed for any reason
func (s *Stream) cleanUpOnFailure(ctx context.Context) error {
	s.logger.Warnf("Cleaning up resources on accident: %v", s.slotName)
	err := DropReplicationSlot(ctx, s.pgConn, s.slotName, DropReplicationSlotOptions{Wait: true})
	if err != nil {
		s.logger.Errorf("Failed to drop replication slot: %s", err.Error())
	}
	return s.pgConn.Close(ctx)
}

func (s *Stream) getPrimaryKeyColumn(tableName string) (string, error) {
	q := fmt.Sprintf(`
		SELECT a.attname
		FROM   pg_index i
		JOIN   pg_attribute a ON a.attrelid = i.indrelid
							AND a.attnum = ANY(i.indkey)
		WHERE  i.indrelid = '%s'::regclass
		AND    i.indisprimary;
	`, tableName)

	reader := s.pgConn.Exec(context.Background(), q)
	data, err := reader.ReadAll()
	if err != nil {
		return "", err
	}

	pkResultRow := data[0].Rows[0]
	pkColName := string(pkResultRow[0])
	return pkColName, nil
}

// Stop closes the stream conect and prevents from replication slot read
func (s *Stream) Stop() error {
	if s == nil {
		return nil
	}
	s.m.Lock()
	s.stopped = true
	s.m.Unlock()
	s.monitor.Stop()

	if s.pgConn != nil {
		if s.streamCtx != nil {
			s.streamCancel()
			// s.standbyCtxCancel is initialized later when starting reading from the replication slot.
			// In case we failed to start replication of the process was shut down before starting the replication slot
			// we need to check if the context is not nil before calling cancel
			if s.standbyCtxCancel != nil {
				s.standbyCtxCancel()
			}
		}
		return s.pgConn.Close(context.Background())
	}

	return nil
}
