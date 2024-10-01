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
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/postgresql/pglogicalstream/internal/helpers"
)

var pluginArguments = []string{"\"pretty-print\" 'true'"}

type Stream struct {
	pgConn *pgconn.PgConn
	// extra copy of db config is required to establish a new db connection
	// which is required to take snapshot data
	dbConfig     pgconn.Config
	streamCtx    context.Context
	streamCancel context.CancelFunc

	standbyCtxCancel context.CancelFunc

	clientXLogPos LSN
	lsnrestart    LSN

	standbyMessageTimeout      time.Duration
	nextStandbyMessageDeadline time.Time
	messages                   chan StreamMessage
	snapshotMessages           chan StreamMessage
	snapshotName               string
	slotName                   string
	schema                     string
	tableNames                 []string
	separateChanges            bool
	snapshotBatchSize          int
	decodingPlugin             DecodingPlugin
	snapshotMemorySafetyFactor float64
	logger                     *service.Logger

	m       sync.Mutex
	stopped bool
}

func NewPgStream(config Config) (*Stream, error) {
	var (
		cfg *pgconn.Config
		err error
	)

	sslVerifyFull := ""
	if config.TlsVerify == TlsRequireVerify {
		sslVerifyFull = "&sslmode=verify-full"
	}

	if cfg, err = pgconn.ParseConfig(fmt.Sprintf("postgres://%s:%s@%s:%d/%s?replication=database%s",
		config.DbUser,
		config.DbPassword,
		config.DbHost,
		config.DbPort,
		config.DbName,
		sslVerifyFull,
	)); err != nil {
		return nil, err
	}

	if config.TlsVerify == TlsRequireVerify {
		cfg.TLSConfig = &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         config.DbHost,
		}
	} else {
		cfg.TLSConfig = nil
	}

	dbConn, err := pgconn.ConnectConfig(context.Background(), cfg)
	if err != nil {
		return nil, err
	}

	var tableNames []string
	for _, table := range config.DbTables {
		tableNames = append(tableNames, table)
	}

	stream := &Stream{
		pgConn:                     dbConn,
		dbConfig:                   *cfg,
		messages:                   make(chan StreamMessage),
		snapshotMessages:           make(chan StreamMessage, 100),
		slotName:                   config.ReplicationSlotName,
		schema:                     config.DbSchema,
		snapshotMemorySafetyFactor: config.SnapshotMemorySafetyFactor,
		separateChanges:            config.SeparateChanges,
		snapshotBatchSize:          config.BatchSize,
		tableNames:                 tableNames,
		logger:                     config.logger,
		m:                          sync.Mutex{},
		decodingPlugin:             DecodingPluginFromString(config.DecodingPlugin),
	}

	if stream.decodingPlugin == "pgoutput" {
		pluginArguments = []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names 'pglog_stream_%s'", config.ReplicationSlotName),
			"messages 'true'",
		}
	}

	result := stream.pgConn.Exec(context.Background(), fmt.Sprintf("DROP PUBLICATION IF EXISTS pglog_stream_%s;", config.ReplicationSlotName))
	_, err = result.ReadAll()
	if err != nil {
		return nil, err
	}

	for i, table := range tableNames {
		tableNames[i] = fmt.Sprintf("%s.%s", config.DbSchema, table)
	}

	tablesSchemaFilter := fmt.Sprintf("FOR TABLE %s", strings.Join(tableNames, ","))
	stream.logger.Infof("Create publication for table schemas with query %s", fmt.Sprintf("CREATE PUBLICATION pglog_stream_%s %s;", config.ReplicationSlotName, tablesSchemaFilter))
	result = stream.pgConn.Exec(context.Background(), fmt.Sprintf("CREATE PUBLICATION pglog_stream_%s %s;", config.ReplicationSlotName, tablesSchemaFilter))
	_, err = result.ReadAll()
	if err != nil {
		return nil, err
	}

	stream.logger.Infof("Created Postgresql publication %v %v", "publication_name", config.ReplicationSlotName)

	sysident, err := IdentifySystem(context.Background(), stream.pgConn)
	if err != nil {
		return nil, err
	}

	stream.logger.Infof("System identification result SystemID: %v Timeline: %v XLogPos: %v DBName: %v", sysident.SystemID, sysident.Timeline, sysident.XLogPos, sysident.DBName)

	var freshlyCreatedSlot = false
	var confirmedLSNFromDB string
	// check is replication slot exist to get last restart SLN
	connExecResult := stream.pgConn.Exec(context.TODO(), fmt.Sprintf("SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s'", config.ReplicationSlotName))
	if slotCheckResults, err := connExecResult.ReadAll(); err != nil {
		return nil, err
	} else {
		if len(slotCheckResults) == 0 || len(slotCheckResults[0].Rows) == 0 {
			// here we create a new replication slot because there is no slot found
			var createSlotResult CreateReplicationSlotResult
			createSlotResult, err = CreateReplicationSlot(context.Background(), stream.pgConn, stream.slotName, stream.decodingPlugin.String(),
				CreateReplicationSlotOptions{Temporary: false,
					SnapshotAction: "export",
				})
			if err != nil {
				return nil, err
			}
			stream.snapshotName = createSlotResult.SnapshotName
			freshlyCreatedSlot = true
		} else {
			slotCheckRow := slotCheckResults[0].Rows[0]
			confirmedLSNFromDB = string(slotCheckRow[0])
			stream.logger.Infof("Replication slot restart LSN extracted from DB: LSN %v", confirmedLSNFromDB)
		}
	}

	// TODO:: check decoding plugin and replication slot plugin should match

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

	if !freshlyCreatedSlot || config.StreamOldData == false {
		if err = stream.startLr(); err != nil {
			return nil, err
		}

		go stream.streamMessagesAsync()
	} else {
		// New messages will be streamed after the snapshot has been processed.
		// stream.startLr() and stream.streamMessagesAsync() will be called inside stream.processSnapshot()
		go stream.processSnapshot()
	}

	return stream, err
}

func (s *Stream) startLr() error {
	var err error
	err = StartReplication(context.Background(), s.pgConn, s.slotName, s.lsnrestart, StartReplicationOptions{PluginArgs: pluginArguments})
	if err != nil {
		return err
	}
	s.logger.Infof("Started logical replication on slot slot-name: %v", s.slotName)

	return nil
}

func (s *Stream) AckLSN(lsn string) error {
	var err error
	s.clientXLogPos, err = ParseLSN(lsn)
	if err != nil {
		panic(fmt.Errorf("failed to parse LSN for Acknowledge %w", err)) // TODO
	}

	err = SendStandbyStatusUpdate(context.Background(), s.pgConn, StandbyStatusUpdate{
		WALApplyPosition: s.clientXLogPos,
		WALWritePosition: s.clientXLogPos,
		ReplyRequested:   true,
	})

	if err != nil {
		s.logger.Errorf("Failed to send Standby status message at LSN#%s: %v", s.clientXLogPos.String(), err)
		return err
	}

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
					s.Stop()
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
				s.Stop()
				return
			}

			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				s.logger.Errorf("Received error message from Postgres: %v", errMsg)
				s.Stop()
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
					panic(fmt.Errorf("parsePrimaryKeepaliveMessage failed: %w", err)) // TODO
				}

				if pkm.ReplyRequested {
					s.nextStandbyMessageDeadline = time.Time{}
				}

			case XLogDataByteID:
				xld, err := ParseXLogData(msg.Data[1:])
				if err != nil {
					panic(fmt.Errorf("parseXLogData failed: %w", err))
				}
				clientXLogPos := xld.WALStart + LSN(len(xld.WALData))

				if s.decodingPlugin == "wal2json" {
					message, err := DecodeWal2JsonChanges(clientXLogPos.String(), xld.WALData)
					if err != nil {
						s.logger.Errorf("decodeWal2JsonChanges failed: %w", err)
						s.Stop()
						return
					}

					if message == nil || len(message.Changes) == 0 {
						// automatic ack for empty changes
						// basically mean that the client is up-to-date,
						// but we still need to acknowledge the LSN for standby
						if err = s.AckLSN(clientXLogPos.String()); err != nil {
							// stop reading from replication slot
							// if we can't acknowledge the LSN
							s.Stop()
							return
						}
					} else {
						s.messages <- *message
					}
				}

				if s.decodingPlugin == "pgoutput" {
					// message changes must be collected in the buffer in the context of the same transaction
					// as single transaction can contain multiple changes
					// and LSN ack will cause potential loss of changes
					isBegin, err := IsBeginMessage(xld.WALData)
					if err != nil {
						s.logger.Errorf("Failed to parse WAL data: %w", err)
						s.Stop()
						return
					}

					if isBegin {
						pgoutputChanges = []StreamMessageChanges{}
					}

					// parse changes inside the transaction
					message, err := DecodePgOutput(xld.WALData, relations, typeMap)
					if err != nil {
						s.logger.Errorf("decodePgOutput failed: %w", err)
						s.Stop()
						return
					}

					if message != nil {
						pgoutputChanges = append(pgoutputChanges, *message)
					}

					isCommit, err := IsCommitMessage(xld.WALData)
					if err != nil {
						s.logger.Errorf("Failed to parse WAL data: %w", err)
						s.Stop()
						return
					}

					if isCommit {
						if len(pgoutputChanges) == 0 {
							// 0 changes happened in the transaction
							// or we received a change that are not supported/needed by the replication stream
							if err = s.AckLSN(clientXLogPos.String()); err != nil {
								// stop reading from replication slot
								// if we can't acknowledge the LSN
								s.Stop()
								return
							}
						} else {
							// send all collected changes
							lsn := clientXLogPos.String()
							s.messages <- StreamMessage{Lsn: &lsn, Changes: pgoutputChanges}
						}
					}
				}
			}
		}
	}
}

func (s *Stream) processSnapshot() {
	snapshotter, err := NewSnapshotter(s.dbConfig, s.snapshotName, s.logger)
	if err != nil {
		s.logger.Errorf("Failed to open SQL connection to prepare snapshot: %v", err.Error())
		if err = s.cleanUpOnFailure(); err != nil {
			s.logger.Errorf("Failed to clean up resources on accident: %v", err.Error())
		}

		os.Exit(1)
	}
	if err = snapshotter.Prepare(); err != nil {
		s.logger.Errorf("Failed to prepare database snapshot. Probably snapshot is expired...: %v", err.Error())
		if err = s.cleanUpOnFailure(); err != nil {
			s.logger.Errorf("Failed to clean up resources on accident: %v", err.Error())
		}

		os.Exit(1)
	}
	defer func() {
		if err = snapshotter.ReleaseSnapshot(); err != nil {
			s.logger.Errorf("Failed to release database snapshot: %v", err.Error())
		}
		if err = snapshotter.CloseConn(); err != nil {
			s.logger.Errorf("Failed to close database connection: %v", err.Error())
		}
	}()

	for _, table := range s.tableNames {
		s.logger.Infof("Processing snapshot for table: %v", table)

		var (
			avgRowSizeBytes sql.NullInt64
			offset          = 0
		)

		avgRowSizeBytes, err = snapshotter.FindAvgRowSize(table)
		if err != nil {
			s.logger.Errorf("Failed to calculate average row size for table %v: %v", table, err.Error())
			if err = s.cleanUpOnFailure(); err != nil {
				s.logger.Errorf("Failed to clean up resources on accident: %v", err.Error())
			}

			os.Exit(1)
		}

		batchSize := snapshotter.CalculateBatchSize(helpers.GetAvailableMemory(), uint64(avgRowSizeBytes.Int64))
		s.logger.Infof("Querying snapshot batch_side: %v, available_memory: %v, avg_row_size: %v", batchSize, helpers.GetAvailableMemory(), avgRowSizeBytes.Int64)

		tablePk, err := s.getPrimaryKeyColumn(table)
		if err != nil {
			s.logger.Errorf("Failed to get primary key column for table %v: %v", table, err.Error())
			if err = s.cleanUpOnFailure(); err != nil {
				s.logger.Errorf("Failed to clean up resources on accident: %v", err.Error())
			}

			os.Exit(1)
		}

		for {
			var snapshotRows *sql.Rows
			if snapshotRows, err = snapshotter.QuerySnapshotData(table, tablePk, batchSize, offset); err != nil {
				panic(fmt.Errorf("can't query snapshot data: %w", err)) // TODO
			}

			columnTypes, err := snapshotRows.ColumnTypes()
			if err != nil {
				s.logger.Errorf("Failed to get column types for table %v: %v", table, err.Error())
				if err = s.cleanUpOnFailure(); err != nil {
					s.logger.Errorf("Failed to clean up resources on accident: %v", err.Error())
				}
				os.Exit(1)
			}

			var columnTypesString = make([]string, len(columnTypes))
			columnNames, err := snapshotRows.Columns()
			if err != nil {
				s.logger.Errorf("Failed to get column names for table %v: %v", table, err.Error())
				if err = s.cleanUpOnFailure(); err != nil {
					s.logger.Errorf("Failed to clean up resources on accident: %v", err.Error())
				}
				os.Exit(1)
			}

			for i := range columnNames {
				columnTypesString[i] = columnTypes[i].DatabaseTypeName()
			}

			count := len(columnTypes)
			var rowsCount = 0
			for snapshotRows.Next() {
				rowsCount += 1
				scanArgs := make([]interface{}, count)
				for i, v := range columnTypes {
					switch v.DatabaseTypeName() {
					case "VARCHAR", "TEXT", "UUID", "TIMESTAMP":
						scanArgs[i] = new(sql.NullString)
						break
					case "BOOL":
						scanArgs[i] = new(sql.NullBool)
						break
					case "INT4":
						scanArgs[i] = new(sql.NullInt64)
						break
					default:
						scanArgs[i] = new(sql.NullString)
					}
				}

				err := snapshotRows.Scan(scanArgs...)

				if err != nil {
					s.logger.Errorf("Failed to scan row for table %v: %v", table, err.Error())
					if err = s.cleanUpOnFailure(); err != nil {
						s.logger.Errorf("Failed to clean up resources on accident: %v", err.Error())
					}
					os.Exit(1)
				}

				var columnValues = make([]interface{}, len(columnTypes))
				for i := range columnTypes {
					if z, ok := (scanArgs[i]).(*sql.NullBool); ok {
						columnValues[i] = z.Bool
						continue
					}
					if z, ok := (scanArgs[i]).(*sql.NullString); ok {
						columnValues[i] = z.String
						continue
					}
					if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
						columnValues[i] = z.Int64
						continue
					}
					if z, ok := (scanArgs[i]).(*sql.NullFloat64); ok {
						columnValues[i] = z.Float64
						continue
					}
					if z, ok := (scanArgs[i]).(*sql.NullInt32); ok {
						columnValues[i] = z.Int32
						continue
					}

					columnValues[i] = scanArgs[i]
				}

				snapshotChangePacket := StreamMessage{
					Lsn: nil,
					Changes: []StreamMessageChanges{
						{
							Table:     table,
							Operation: "insert",
							Schema:    s.schema,
							Data: func() map[string]any {
								var data = make(map[string]any)
								for i, cn := range columnNames {
									data[cn] = columnValues[i]
								}

								return data
							}(),
						},
					},
				}

				s.snapshotMessages <- snapshotChangePacket
			}

			offset += batchSize

			if batchSize != rowsCount {
				break
			}
		}

	}

	if err = s.startLr(); err != nil {
		s.logger.Errorf("Failed to start logical replication after snapshot: %v", err.Error())
		os.Exit(1)
	}
	go s.streamMessagesAsync()
}

func (s *Stream) SnapshotMessageC() chan StreamMessage {
	return s.snapshotMessages
}

func (s *Stream) LrMessageC() chan StreamMessage {
	return s.messages
}

// cleanUpOnFailure drops replication slot and publication if database snapshotting was failed for any reason
func (s *Stream) cleanUpOnFailure() error {
	s.logger.Warnf("Cleaning up resources on accident: %v", s.slotName)
	err := DropReplicationSlot(context.Background(), s.pgConn, s.slotName, DropReplicationSlotOptions{Wait: true})
	if err != nil {
		s.logger.Errorf("Failed to drop replication slot: %s", err.Error())
	}
	return s.pgConn.Close(context.TODO())
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

func (s *Stream) Stop() error {
	s.m.Lock()
	s.stopped = true
	s.m.Unlock()

	if s.pgConn != nil {
		if s.streamCtx != nil {
			s.streamCancel()
			s.standbyCtxCancel()
		}
		return s.pgConn.Close(context.Background())
	}

	return nil
}
