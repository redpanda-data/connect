package peerdb

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

func peerDBInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Summary("Reads from a Postgres DB using CDC (Change Data Capture) with pglogrepl.").
		Description("Connects to a Postgres database and replicates changes from the specified tables using logical replication.").
		Field(service.NewStringField("host").Description("Postgres host to connect to.").Example("localhost")).
		Field(service.NewIntField("port").Description("Postgres port to connect to.").Example(5432)).
		Field(service.NewStringField("user").Description("Postgres user.").Example("postgres")).
		Field(service.NewStringField("password").Description("Postgres password.").Example("password")).
		Field(service.NewStringField("database").Description("Postgres database name.").Example("mydb")).
		Field(service.NewStringField("replication_slot").Description("Replication slot name.").Example("benthos_slot")).
		Field(service.NewStringField("publication").Description("Publication name.").Example("benthos_pub")).
		Field(service.NewStringListField("tables").Description("List of schema-qualified tables to replicate, in the format of 'schema.table'.").Example([]string{"public.users", "myschema.orders"})).
		Field(service.NewIntField("batch_size").Description("Batch size.").Default(100).Example(100)).
		Field(service.NewAutoRetryNacksToggleField()).
		Field(service.NewIntField("idle_timeout_seconds").Description("Idle timeout in seconds. If no messages are received within this duration, the current batch will be returned.").Default(5)).
		Field(service.NewStringField("cache").Description("A cache resource to use for persisting the last committed source LSN.")).
		Field(service.NewStringField("cache_key").Description("The key identifier used when storing the last committed source LSN.").Default("last_committed_lsn_unique"))
}

func init() {
	err := service.RegisterBatchInput(
		"peerdb", peerDBInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newPeerDBInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type relationMessageMap map[uint32]*pglogrepl.RelationMessage

type peerDBInput struct {
	host               string
	port               int
	user               string
	password           string
	database           string
	replicationSlot    string
	publication        string
	tables             []string
	batchSize          int
	idleTimeoutSeconds int
	activeReplConn     bool

	// postgres specific state.
	cache            string
	cacheKey         string
	replConn         *pgx.Conn
	commitLock       *pglogrepl.BeginMessage
	relMsgMap        relationMessageMap
	clientXLogPos    pglogrepl.LSN
	commitXLogPos    pglogrepl.LSN
	replicatedRelIds map[uint32]struct{}

	mgr    *service.Resources
	logger *service.Logger
	mut    sync.Mutex
}

func newPeerDBInput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
	p := &peerDBInput{
		logger: mgr.Logger(),
		mgr:    mgr,
	}

	var err error
	if p.host, err = conf.FieldString("host"); err != nil {
		return nil, err
	}
	if p.port, err = conf.FieldInt("port"); err != nil {
		return nil, err
	}
	if p.user, err = conf.FieldString("user"); err != nil {
		return nil, err
	}
	if p.password, err = conf.FieldString("password"); err != nil {
		return nil, err
	}
	if p.database, err = conf.FieldString("database"); err != nil {
		return nil, err
	}
	if p.replicationSlot, err = conf.FieldString("replication_slot"); err != nil {
		return nil, err
	}
	if p.publication, err = conf.FieldString("publication"); err != nil {
		return nil, err
	}
	if p.tables, err = conf.FieldStringList("tables"); err != nil {
		return nil, err
	}
	if p.batchSize, err = conf.FieldInt("batch_size"); err != nil {
		return nil, err
	}
	if p.batchSize <= 0 {
		return nil, fmt.Errorf("batch_size must be greater than 0, got %d", p.batchSize)
	}
	if p.idleTimeoutSeconds, err = conf.FieldInt("idle_timeout_seconds"); err != nil {
		return nil, err
	}
	if p.idleTimeoutSeconds < 1 {
		return nil, fmt.Errorf("idle_timeout_seconds must be greater than 0, got %d", p.idleTimeoutSeconds)
	}

	if p.cache, err = conf.FieldString("cache"); err != nil {
		return nil, err
	}
	if p.cacheKey, err = conf.FieldString("cache_key"); err != nil {
		return nil, err
	}

	p.activeReplConn = false

	return service.AutoRetryNacksBatchedToggled(conf, p)
}

func (p *peerDBInput) Connect(ctx context.Context) error {
	p.mut.Lock()
	defer p.mut.Unlock()

	escapedPassword := url.QueryEscape(p.password)
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s", p.user, escapedPassword, p.host, p.port, p.database)

	// create a separate connection pool for non-replication queries as replication connections cannot
	// be used for extended query protocol, i.e. prepared statements
	connConfig, err := pgx.ParseConfig(connStr)
	if err != nil {
		p.logger.With("err", err).Error("failed to parse connection string")
		return fmt.Errorf("failed to parse connection string: %w", err)
	}

	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		p.logger.With("err", err).Error("failed to connect to postgres for non-replication queries")
		return fmt.Errorf("failed to connect to postgres for non-replication queries: %w", err)
	}

	defer func() {
		if conn != nil {
			conn.Close(ctx)
		}
	}()

	replConfig := connConfig.Copy()
	runtimeParams := replConfig.Config.RuntimeParams
	runtimeParams["idle_in_transaction_session_timeout"] = "0"
	runtimeParams["statement_timeout"] = "0"
	runtimeParams["replication"] = "database"
	runtimeParams["bytea_output"] = "hex"

	replConn, err := pgx.ConnectConfig(ctx, replConfig)
	if err != nil {
		p.logger.With("err", err).Error("failed to connect to postgres for replication")
		return fmt.Errorf("failed to connect to postgres for replication: %w", err)
	}

	p.replConn = replConn

	var pos pglogrepl.LSN = 0
	var cacheErr error
	err = p.mgr.AccessCache(ctx, p.cache, func(c service.Cache) {
		posBytes, err := c.Get(ctx, p.cacheKey)
		if err != nil && !errors.Is(err, service.ErrKeyNotFound) {
			cacheErr = err
			return
		}
		if len(posBytes) > 0 {
			pos = pglogrepl.LSN(binary.BigEndian.Uint64(posBytes))
		}
	})

	if err != nil {
		return fmt.Errorf("failed to access cache: %w", err)
	}

	if cacheErr != nil {
		return fmt.Errorf("failed to access cache: %w", cacheErr)
	}

	p.clientXLogPos = pos
	p.commitXLogPos = pos
	p.commitLock = nil
	p.relMsgMap = make(relationMessageMap)

	// Get relation IDs of tables to replicate
	relIds, err := p.getReplicatedRelationIds(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to get replicated relation IDs: %w", err)
	}
	p.replicatedRelIds = relIds

	return nil
}

func (p *peerDBInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	p.mut.Lock()
	defer p.mut.Unlock()

	if p.replConn == nil {
		return nil, nil, service.ErrNotConnected
	}

	conn := p.replConn.PgConn()
	err := p.startReplication(ctx, conn)
	if err != nil {
		p.logger.With("err", err).Error("failed to start replication")
		return nil, nil, fmt.Errorf("failed to start replication: %w", err)
	}

	ackFunc := func(ctx context.Context, err error) error {
		if err != nil {
			p.logger.With("err", err).Error("consumer failed to consume batch")
			return fmt.Errorf("consumer failed to consume batch: %w", err)
		}

		err = p.updateCachedLSN(ctx, p.commitXLogPos)
		if err != nil {
			p.logger.With("err", err).Error("failed to update cached LSN")
			return fmt.Errorf("failed to update cached LSN: %w", err)
		}

		return nil
	}

	idleTimeoutDur := time.Duration(p.idleTimeoutSeconds) * time.Second
	lastReceivedMsgTime := time.Now()

	batch := make(service.MessageBatch, 0)
	for {
		if time.Since(lastReceivedMsgTime) > idleTimeoutDur && p.commitLock == nil {
			if len(batch) == 0 {
				return nil, nil, service.ErrEndOfInput
			}
			return batch, ackFunc, nil
		}

		idleTimeoutAwareCtx, cancel := context.WithTimeout(ctx, idleTimeoutDur)
		defer cancel()

		rawMsg, err := conn.ReceiveMessage(idleTimeoutAwareCtx)
		if err != nil {
			if strings.Contains(err.Error(), "timeout:") {
				if p.commitLock != nil {
					p.logger.Info("hit idle timeout but waiting for commit message to complete batch")
					continue
				}

				if len(batch) == 0 {
					return nil, nil, service.ErrEndOfInput
				}

				return batch, ackFunc, nil
			} else {
				p.logger.With("err", err).Error("failed to receive message")
				return nil, nil, fmt.Errorf("failed to receive message: %w", err)
			}
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			p.logger.With("err", errMsg).Error("received Postgres WAL error")
			return nil, nil, fmt.Errorf("received Postgres WAL error: %v", errMsg)
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				p.logger.With("err", err).Error("failed to parse primary keepalive message")
				return nil, nil, fmt.Errorf("ParsePrimaryKeepaliveMessage failed: %w", err)
			}

			if pkm.ReplyRequested {
				err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: p.clientXLogPos})
				if err != nil {
					p.logger.With("err", err).Error("failed to send standby status update")
					return nil, nil, fmt.Errorf("failed to send standby status update: %w", err)
				}
			}
		case pglogrepl.XLogDataByteID:
			lastReceivedMsgTime = time.Now()
			xlog, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				p.logger.With("err", err).Error("failed to parse xlog data")
				return nil, nil, fmt.Errorf("ParseXLogData failed: %w", err)
			}

			message, err := p.processMessage(xlog)
			if err != nil {
				p.logger.With("err", err).Error("failed to process message")
				return nil, nil, fmt.Errorf("processMessage failed: %w", err)
			}
			if message != nil {
				batch = append(batch, message)
			}
		}

		if len(batch) >= p.batchSize && p.commitLock == nil {
			return batch, ackFunc, nil
		}
	}

}

func (p *peerDBInput) updateCachedLSN(ctx context.Context, pos pglogrepl.LSN) error {
	var cacheErr error
	err := p.mgr.AccessCache(ctx, p.cache, func(c service.Cache) {
		p.logger.With("pos", pos).Debugf("updating cached LSN to %v", pos)
		cacheErr = c.Set(ctx, p.cacheKey, []byte(pos.String()), nil)
	})

	if cacheErr != nil {
		p.logger.With("err", cacheErr).Error("failed to update cached LSN")
		return fmt.Errorf("failed to update cached LSN: %w", cacheErr)
	}

	return err
}

func (p *peerDBInput) Close(ctx context.Context) error {
	p.mut.Lock()
	defer p.mut.Unlock()

	if p.replConn != nil {
		p.logger.Debugf("Closing replication connection")
		err := p.replConn.Close(ctx)
		p.replConn = nil
		if err != nil {
			return err
		}
	}

	p.activeReplConn = false

	return nil
}

func (p *peerDBInput) processMessage(
	xld pglogrepl.XLogData,
) (*service.Message, error) {
	logicalMsg, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return nil, fmt.Errorf("error parsing logical message: %w", err)
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.BeginMessage:
		p.logger.Debug(fmt.Sprintf("BeginMessage => FinalLSN: %v, XID: %v", msg.FinalLSN, msg.Xid))
		p.logger.Debug("awaiting commit message for this batch...")
		p.commitLock = msg
	case *pglogrepl.InsertMessage:
		if _, ok := p.replicatedRelIds[msg.RelationID]; !ok {
			return nil, nil
		}
		relMsg, ok := p.relMsgMap[msg.RelationID]
		if !ok {
			return nil, fmt.Errorf("relation message not found for relation id %d", msg.RelationID)
		}
		return p.processTuple(msg.Tuple, relMsg)
	case *pglogrepl.UpdateMessage:
		if _, ok := p.replicatedRelIds[msg.RelationID]; !ok {
			return nil, nil
		}
		relMsg, ok := p.relMsgMap[msg.RelationID]
		if !ok {
			return nil, fmt.Errorf("relation message not found for relation id %d", msg.RelationID)
		}
		return p.processTuple(msg.NewTuple, relMsg)
	case *pglogrepl.DeleteMessage:
		if _, ok := p.replicatedRelIds[msg.RelationID]; !ok {
			return nil, nil
		}
		relMsg, ok := p.relMsgMap[msg.RelationID]
		if !ok {
			return nil, fmt.Errorf("relation message not found for relation id %d", msg.RelationID)
		}
		return p.processTuple(msg.OldTuple, relMsg)
	case *pglogrepl.CommitMessage:
		p.logger.Debug(fmt.Sprintf("CommitMessage => CommitLSN: %v, TransactionEndLSN: %v", msg.CommitLSN, msg.TransactionEndLSN))
		p.commitLock = nil
		p.commitXLogPos = msg.CommitLSN
	case *pglogrepl.RelationMessage:
		p.relMsgMap[msg.RelationID] = msg
	case *pglogrepl.TruncateMessage:
		p.logger.Warn("TruncateMessage not yet supported")
	default:
		p.logger.Warn(fmt.Sprintf("unknown message type: %T", logicalMsg))
	}

	return nil, nil
}

func (p *peerDBInput) processTuple(
	tuple *pglogrepl.TupleData,
	rel *pglogrepl.RelationMessage,
) (*service.Message, error) {
	if tuple == nil {
		return nil, nil
	}

	pgMessage := newPgMessage(len(tuple.Columns))
	for idx, tcol := range tuple.Columns {
		rcol := rel.Columns[idx]
		switch tcol.DataType {
		case 'n':
			pgMessage.addColumn(rcol.Name, nil)
		case 't':
			// bytea also appears here as a hex
			pgMessage.addColumn(rcol.Name, tcol.Data)
		case 'b':
			return nil, fmt.Errorf("binary encoding not supported, received for %s type %d", rcol.Name, rcol.DataType)
		case 'u':
			p.logger.Warn("unchanged toast columns are not supported, please set replica identity to 'full'")
		default:
			return nil, fmt.Errorf("unknown column data type: %s", string(tcol.DataType))
		}
	}

	return pgMessage.message()
}

func (p *peerDBInput) startReplication(ctx context.Context, conn *pgconn.PgConn) error {
	if p.activeReplConn {
		return nil
	}

	pluginArguments := []string{"proto_version '1'"}
	pubicationOpt := "publication_names " + quoteLiteral(p.publication)
	pluginArguments = append(pluginArguments, pubicationOpt)
	replicationOpts := pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments}
	err := pglogrepl.StartReplication(ctx, conn, p.replicationSlot, p.clientXLogPos, replicationOpts)
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}

	p.activeReplConn = true

	return nil
}

// implement getReplicatedRelationIds
func (p *peerDBInput) getReplicatedRelationIds(ctx context.Context, conn *pgx.Conn) (map[uint32]struct{}, error) {
	relIds := make(map[uint32]struct{})
	for _, table := range p.tables {
		schema, tableName, err := splitTableName(table)
		if err != nil {
			return nil, fmt.Errorf("failed to split table name: %w", err)
		}

		query := `SELECT oid FROM pg_class WHERE relname = $1 AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $2)`
		var relID uint32
		err = conn.QueryRow(ctx, query, tableName, schema).Scan(&relID)
		if err != nil {
			return nil, fmt.Errorf("failed to get relation ID for %s.%s: %w", schema, tableName, err)
		}
		relIds[relID] = struct{}{}
	}
	return relIds, nil
}

// QuoteLiteral quotes a 'literal' (e.g. a parameter, often used to pass literal
// to DDL and other statements that do not accept parameters) to be used as part
// of an SQL statement.  For example:
//
//	exp_date := pq.QuoteLiteral("2023-01-05 15:00:00Z")
//	err := db.Exec(fmt.Sprintf("CREATE ROLE my_user VALID UNTIL %s", exp_date))
//
// Any single quotes in name will be escaped. Any backslashes (i.e. "\") will be
// replaced by two backslashes (i.e. "\\") and the C-style escape identifier
// that PostgreSQL provides ('E') will be prepended to the string.
func quoteLiteral(literal string) string {
	// This follows the PostgreSQL internal algorithm for handling quoted literals
	// from libpq, which can be found in the "PQEscapeStringInternal" function,
	// which is found in the libpq/fe-exec.c source file:
	// https://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/interfaces/libpq/fe-exec.c
	//
	// substitute any single-quotes (') with two single-quotes ('')
	literal = strings.ReplaceAll(literal, `'`, `''`)
	// determine if the string has any backslashes (\) in it.
	// if it does, replace any backslashes (\) with two backslashes (\\)
	// then, we need to wrap the entire string with a PostgreSQL
	// C-style escape. Per how "PQEscapeStringInternal" handles this case, we
	// also add a space before the "E"
	if strings.Contains(literal, `\`) {
		literal = strings.ReplaceAll(literal, `\`, `\\`)
		literal = ` E'` + literal + `'`
	} else {
		// otherwise, we can just wrap the literal with a pair of single quotes
		literal = `'` + literal + `'`
	}
	return literal
}

func splitTableName(table string) (string, string, error) {
	parts := strings.Split(table, ".")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("table name must be in the format schema.table, got %s", table)
	}
	return parts[0], parts[1], nil
}

type pgMessage struct {
	colNameToValue map[string]string
}

func newPgMessage(numCols int) *pgMessage {
	return &pgMessage{
		colNameToValue: make(map[string]string, numCols),
	}
}

func (p *pgMessage) addColumn(name string, value []byte) {
	p.colNameToValue[name] = string(value)
}

func (p *pgMessage) bytes() ([]byte, error) {
	encoded, err := json.Marshal(p.colNameToValue)
	if err != nil {
		return nil, err
	}

	return encoded, nil
}

func (p *pgMessage) message() (*service.Message, error) {
	bytes, err := p.bytes()
	if err != nil {
		return nil, err
	}

	return service.NewMessage(bytes), nil
}
