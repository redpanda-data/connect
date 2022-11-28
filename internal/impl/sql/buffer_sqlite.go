package sql

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/cenkalti/backoff/v4"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

// SQLiteBufferConfig returns a config spec for an SQLite buffer.
func SQLiteBufferConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Utility").
		Summary("Stores messages in an SQLite database and acknowledges them at the input level.").
		Description(`
Stored messages are then consumed as a stream from the database and deleted only once they are successfully sent at the output level. If the service is restarted Benthos will make a best attempt to finish delivering messages that are already read from the database, and when it starts again it will consume from the oldest message that has not yet been delivered.

## Delivery Guarantees

Messages are not acknowledged at the input level until they have been added to the SQLite database, and they are not removed from the SQLite database until they have been successfully delivered. This means at-least-once delivery guarantees are preserved in cases where the service is shut down unexpectedly. However, since this process relies on interaction with the disk (wherever the SQLite DB is stored) these delivery guarantees are not resilient to disk corruption or loss.

## Batching

Messages that are logically batched at the point where they are added to the buffer will continue to be associated with that batch when they are consumed. This buffer is also more efficient when storing messages within batches, and therefore it is recommended to use batching at the input level in high-throughput use cases even if they are not required for processing.
`).
		Field(service.NewStringField("path").
			Description(`The path of the database file, which will be created if it does not already exist.`)).
		Field(service.NewProcessorListField("pre_processors").
			Description(`An optional list of processors to apply to messages before they are stored within the buffer. These processors are useful for compressing, archiving or otherwise reducing the data in size before it's stored on disk.`).
			Optional()).
		Field(service.NewProcessorListField("post_processors").
			Description("An optional list of processors to apply to messages after they are consumed from the buffer. These processors are useful for undoing any compression, archiving, etc that may have been done by your `pre_processors`.").
			Optional()).
		Example("Batching for optimisation", "Batching at the input level greatly increases the throughput of this buffer. If logical batches aren't needed for processing add a [`split` processor](/docs/components/processors/split) to the `post_processors`.", `
input:
  batched:
    child:
      sql_select:
        driver: postgres
        dsn: postgres://foouser:foopass@localhost:5432/testdb?sslmode=disable
        table: footable
        columns: [ '*' ]
    policy:
      count: 100
      period: 500ms

buffer:
  sqlite:
    path: ./foo.db
    post_processors:
      - split: {}
`)
}

func init() {
	err := service.RegisterBatchBuffer(
		"sqlite", SQLiteBufferConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchBuffer, error) {
			return NewSQLiteBufferFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

var maxRequeue = math.MaxInt

// NewSQLiteBufferFromConfig creates a new SQLite buffer from a parsed config.
func NewSQLiteBufferFromConfig(conf *service.ParsedConfig, res *service.Resources) (*SQLiteBuffer, error) {
	path, err := conf.FieldString("path")
	if err != nil {
		return nil, err
	}

	var preProcs, postProcs []*service.OwnedProcessor
	if conf.Contains("pre_processors") {
		if preProcs, err = conf.FieldProcessorList("pre_processors"); err != nil {
			return nil, err
		}
	}
	if conf.Contains("post_processors") {
		if postProcs, err = conf.FieldProcessorList("post_processors"); err != nil {
			return nil, err
		}
	}

	return newSQLiteBuffer(path, preProcs, postProcs)
}

//------------------------------------------------------------------------------

// SQLiteBuffer stores messages for consumption through an SQLite DB.
type SQLiteBuffer struct {
	db        *sql.DB
	preProcs  []*service.OwnedProcessor
	postProcs []*service.OwnedProcessor

	pending     []ackableBatch
	cond        *sync.Cond
	nextIndex   int
	requeueFrom int
	endOfInput  bool
	closed      bool
}

func newSQLiteBuffer(path string, preProcs, postProcs []*service.OwnedProcessor) (*SQLiteBuffer, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	if _, err = db.Exec(`
PRAGMA synchronous = 0;

CREATE TABLE IF NOT EXISTS messages (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  content  TEXT NOT NULL,
  requeue  INTEGER NOT NULL
)
`); err != nil {
		return nil, err
	}

	return &SQLiteBuffer{
		db:        db,
		preProcs:  preProcs,
		postProcs: postProcs,
		cond:      sync.NewCond(&sync.Mutex{}),
	}, nil
}

//------------------------------------------------------------------------------

// returns nil, nil when the rows are empty.
func (m *SQLiteBuffer) tryGetBatch(ctx context.Context) (service.MessageBatch, int, error) {
	var index int
	var requeueFrom int
	var contentBytes []byte

	if err := queryRowRetries(ctx, squirrel.Select("id", "content", "requeue").
		From("messages").
		Where(squirrel.Or{
			squirrel.GtOrEq{"id": m.nextIndex},
			squirrel.And{
				squirrel.Gt{"requeue": m.requeueFrom},
				squirrel.NotEq{"requeue": maxRequeue},
			},
		}).
		OrderBy("requeue, id").
		Limit(1).
		RunWith(m.db), &index, &contentBytes, &requeueFrom); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
		}
		return nil, 0, err
	}

	if requeueFrom != maxRequeue {
		m.requeueFrom = requeueFrom
	}
	m.nextIndex = index + 1

	batch, _, err := readBatch(contentBytes)
	return batch, index, err
}

func (m *SQLiteBuffer) requeue(ctx context.Context, index int) error {
	if m.db == nil {
		return errors.New("connection closed")
	}
	_, err := execRetries(ctx, squirrel.Update("messages").
		Set("requeue", time.Now().UnixNano()).
		Where(squirrel.Eq{"id": index}).
		RunWith(m.db))
	m.cond.Broadcast()
	return err
}

type ackableBatch struct {
	b   service.MessageBatch
	aFn service.AckFunc
}

func (m *SQLiteBuffer) toAckableBatches(batches []service.MessageBatch, index int) []ackableBatch {
	endAckFn := func(ctx context.Context, err error) (ackErr error) {
		m.cond.L.Lock()
		defer m.cond.L.Unlock()
		if err != nil {
			ackErr = m.requeue(ctx, index)
		} else {
			_, ackErr = execRetries(ctx, squirrel.Delete("messages").
				Where(squirrel.Eq{"id": index}).
				RunWith(m.db))
		}
		return
	}

	if len(batches) == 1 {
		return []ackableBatch{
			{b: batches[0], aFn: endAckFn},
		}
	}

	pendingResponses := int64(len(batches))
	aBatches := make([]ackableBatch, len(batches))
	var ackOnce sync.Once
	for i := range batches {
		aBatches[i] = ackableBatch{b: batches[i], aFn: func(ctx context.Context, err error) error {
			if atomic.AddInt64(&pendingResponses, -1) == 0 || err != nil {
				var ackErr error
				ackOnce.Do(func() {
					ackErr = endAckFn(ctx, err)
				})
				return ackErr
			}
			return nil
		}}
	}
	return aBatches
}

// ReadBatch attempts to pop a row from the DB.
func (m *SQLiteBuffer) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	ctx, done := context.WithCancel(ctx)
	defer done()

	go func() {
		<-ctx.Done()
		m.cond.Broadcast()
	}()

	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	for len(m.pending) == 0 {
		if m.closed {
			return nil, nil, service.ErrEndOfBuffer
		}
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}

		nextBatch, outIndex, err := m.tryGetBatch(ctx)
		if err != nil {
			return nil, nil, err
		}
		if len(nextBatch) > 0 {
			resBatches := []service.MessageBatch{nextBatch}
			for _, proc := range m.postProcs {
				var tmpResBatch []service.MessageBatch
				for _, batch := range resBatches {
					resBatches, err := proc.ProcessBatch(ctx, batch)
					if err != nil {
						return nil, nil, err
					}
					tmpResBatch = append(tmpResBatch, resBatches...)
				}
				resBatches = tmpResBatch
			}
			if m.pending = m.toAckableBatches(resBatches, outIndex); len(m.pending) > 0 {
				break
			}
			continue
		}
		if m.endOfInput {
			return nil, nil, service.ErrEndOfBuffer
		}

		// None of our exit conditions triggered, so exit
		m.cond.Wait()
	}

	tmp := m.pending[0]
	m.pending = m.pending[1:]
	return tmp.b, tmp.aFn, nil
}

// WriteBatch adds a new message to the DB.
func (m *SQLiteBuffer) WriteBatch(ctx context.Context, msgBatch service.MessageBatch, aFn service.AckFunc) error {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	if m.closed {
		return component.ErrTypeClosed
	}

	msgBatches := []service.MessageBatch{msgBatch}
	for _, proc := range m.preProcs {
		var tmpResBatch []service.MessageBatch
		for _, batch := range msgBatches {
			resBatches, err := proc.ProcessBatch(ctx, batch)
			if err != nil {
				return err
			}
			tmpResBatch = append(tmpResBatch, resBatches...)
		}
		msgBatches = tmpResBatch
	}

	builder := squirrel.Insert("messages").Columns("content", "requeue")
	for _, batch := range msgBatches {
		contentBytes, err := appendBatchV0(nil, batch)
		if err != nil {
			return err
		}
		builder = builder.Values(contentBytes, maxRequeue)
	}

	if _, err := execRetries(ctx, builder.RunWith(m.db)); err != nil {
		return err
	}
	if err := aFn(ctx, nil); err != nil {
		return err
	}

	m.cond.Broadcast()
	return nil
}

// EndOfInput signals to the buffer that the input is finished and therefore
// once the DB is drained it should close.
func (m *SQLiteBuffer) EndOfInput() {
	go func() {
		m.cond.L.Lock()
		defer m.cond.L.Unlock()

		m.endOfInput = true
		m.cond.Broadcast()
	}()
}

// Close the underlying DB connection.
func (m *SQLiteBuffer) Close(ctx context.Context) error {
	m.cond.L.Lock()
	m.closed = true
	err := m.db.Close()
	m.cond.L.Unlock()
	return err
}

//------------------------------------------------------------------------------

type retryable interface {
	ExecContext(ctx context.Context) (sql.Result, error)
	QueryContext(ctx context.Context) (*sql.Rows, error)
	QueryRowContext(ctx context.Context) squirrel.RowScanner
}

func getBackoff() backoff.BackOff {
	boff := backoff.NewExponentialBackOff()
	boff.InitialInterval = time.Millisecond * 1
	boff.MaxInterval = time.Millisecond * 50
	boff.MaxElapsedTime = time.Second
	return boff
}

func retryableErr(err error) bool {
	if err == nil {
		return false
	}
	if strings.Contains(err.Error(), "SQLITE_BUSY") {
		return true
	}
	return false
}

func execRetries(ctx context.Context, r retryable) (res sql.Result, err error) {
	boff := getBackoff()
	for {
		if res, err = r.ExecContext(ctx); err == nil || !retryableErr(err) {
			return
		}
		next := boff.NextBackOff()
		if next == backoff.Stop {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(next):
		}
	}
}

func queryRowRetries(ctx context.Context, r retryable, v ...interface{}) (err error) {
	boff := getBackoff()
	for {
		if err = r.QueryRowContext(ctx).Scan(v...); err == nil || !retryableErr(err) {
			return
		}
		next := boff.NextBackOff()
		if next == backoff.Stop {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(next):
		}
	}
}

var errFailedParse = errors.New("the data appears to be corrupt")

func appendUint32(buffer []byte, i uint32) []byte {
	return append(buffer,
		byte(i>>24),
		byte(i>>16),
		byte(i>>8),
		byte(i))
}

func readUint32(b []byte) (i uint32, remaining []byte, err error) {
	if len(b) < 4 {
		return 0, nil, errFailedParse
	}
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]), b[4:], nil
}

func appendBatchV0(buffer []byte, batch service.MessageBatch) ([]byte, error) {
	// First value indicates the marshal version, which starts at 0.
	buffer = appendUint32(buffer, 0)

	// Second value indicates the number of messages in the batch.
	buffer = appendUint32(buffer, uint32(len(batch)))

	for _, msg := range batch {
		var err error
		if buffer, err = appendMessageV0(buffer, msg); err != nil {
			return nil, err
		}
	}
	return buffer, nil
}

func appendMessageV0(buffer []byte, msg *service.Message) ([]byte, error) {
	metaObj := map[string]any{}
	_ = msg.MetaWalkMut(func(key string, value any) error {
		metaObj[key] = value
		return nil
	})

	metaBytes, err := msgpack.Marshal(metaObj)
	if err != nil {
		return nil, err
	}

	// First value indicates length of serialized metadata.
	buffer = appendUint32(buffer, uint32(len(metaBytes)))
	// Followed by metadata.
	buffer = append(buffer, metaBytes...)

	msgBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	// Second value indicates length of content.
	buffer = appendUint32(buffer, uint32(len(msgBytes)))
	// Followed by content.
	buffer = append(buffer, msgBytes...)
	return buffer, nil
}

func readBatch(b []byte) (service.MessageBatch, []byte, error) {
	var ver uint32
	var err error
	if ver, b, err = readUint32(b); err != nil {
		return nil, nil, err
	}
	// Only supported version thus far.
	if ver != 0 {
		return nil, nil, errFailedParse
	}
	return readBatchV0(b)
}

func readBatchV0(b []byte) (service.MessageBatch, []byte, error) {
	var parts uint32
	var err error
	if parts, b, err = readUint32(b); err != nil {
		return nil, nil, err
	}

	batch := make(service.MessageBatch, parts)
	for i := uint32(0); i < parts; i++ {
		if batch[i], b, err = readMessageV0(b); err != nil {
			return nil, nil, err
		}
	}
	return batch, b, nil
}

func readMessageV0(b []byte) (*service.Message, []byte, error) {
	var contentLen uint32
	var err error

	// Metadata bytes.
	if contentLen, b, err = readUint32(b); err != nil {
		return nil, nil, err
	}
	metaBytes := b[:contentLen]
	b = b[contentLen:]

	// Content bytes.
	if contentLen, b, err = readUint32(b); err != nil {
		return nil, nil, err
	}
	contentBytes := b[:contentLen]
	b = b[contentLen:]

	msg := service.NewMessage(contentBytes)

	metaObj := map[string]any{}
	if err := msgpack.Unmarshal(metaBytes, &metaObj); err != nil {
		return nil, nil, err
	}
	for k, v := range metaObj {
		msg.MetaSetMut(k, v)
	}
	return msg, b, nil
}
