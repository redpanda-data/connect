package replication

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"
)

// StreamConfig controls change table polling.
type StreamConfig struct {
	PollBatchSize     int
	HeartbeatInterval time.Duration
	MinBackoff        time.Duration
	MaxBackoff        time.Duration
}

const pollSQL = `SELECT ID, OP, SCHEMA_NAME, TABLE_NAME, OP_TIME,
       COALESCE(PK_JSON, ''), COALESCE(OLD_VALUES, ''), COALESCE(NEW_VALUES, '')
FROM _RPCN_CDC.CHANGES
WHERE ID > ?
ORDER BY ID
LIMIT ?`

// Stream polls _RPCN_CDC.CHANGES and returns ChangeEvents in order.
type Stream struct {
	db     *sql.DB
	cfg    StreamConfig
	lastID atomic.Uint64
}

// NewStream creates a Stream. Defaults apply for zero-value StreamConfig fields.
func NewStream(db *sql.DB, cfg StreamConfig) *Stream {
	if cfg.PollBatchSize <= 0 {
		cfg.PollBatchSize = 2048
	}
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = 100 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 5 * time.Second
	}
	return &Stream{db: db, cfg: cfg}
}

// StartFrom sets the initial position for resuming from a checkpoint.
func (s *Stream) StartFrom(pos LogPos) {
	s.lastID.Store(uint64(pos))
}

// LastPos returns the highest LogPos seen so far.
func (s *Stream) LastPos() LogPos {
	return LogPos(s.lastID.Load())
}

// Poll fetches the next batch of change events since LastPos.
// Returns empty slice (no error) when there are no new rows.
func (s *Stream) Poll(ctx context.Context) ([]ChangeEvent, error) {
	raw := s.lastID.Load()
	// Guard against overflow: HANA IDENTITY columns are BIGINT so valid IDs
	// fit in int64. If somehow a value exceeds MaxInt64, clamp to MaxInt64 so
	// the query is still valid rather than silently wrapping to a negative ID.
	var lastID int64
	if raw > uint64(1<<63-1) {
		lastID = 1<<63 - 1
	} else {
		lastID = int64(raw)
	}
	rows, err := s.db.QueryContext(ctx, pollSQL, lastID, s.cfg.PollBatchSize)
	if err != nil {
		return nil, fmt.Errorf("polling _RPCN_CDC.CHANGES after ID %d: %w", lastID, err)
	}
	defer rows.Close()

	events := make([]ChangeEvent, 0, s.cfg.PollBatchSize)
	var maxID uint64
	for rows.Next() {
		var (
			id                       int64
			op, schema, table        string
			opTime                   time.Time
			pkJSON, oldJSON, newJSON string
		)
		if err := rows.Scan(&id, &op, &schema, &table, &opTime, &pkJSON, &oldJSON, &newJSON); err != nil {
			return nil, fmt.Errorf("scanning change row: %w", err)
		}
		ev := ChangeEvent{
			Schema:    schema,
			Table:     table,
			LogPos:    NewLogPos(uint64(id)),
			Timestamp: opTime,
		}
		switch op {
		case "I":
			ev.Operation = OpTypeInsert
			ev.Data = unmarshalMap(newJSON)
		case "U":
			ev.Operation = OpTypeUpdate
			ev.Data = unmarshalMap(newJSON)
			ev.BeforeData = unmarshalMap(oldJSON)
		case "D":
			ev.Operation = OpTypeDelete
			ev.Data = unmarshalMap(oldJSON) // before-image stored in OLD_VALUES for DELETE
		default:
			ev.Operation = OpType(op)
			ev.Data = unmarshalMap(newJSON)
		}
		events = append(events, ev)
		if uint64(id) > maxID {
			maxID = uint64(id)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating change rows: %w", err)
	}
	if maxID > 0 {
		s.lastID.Store(maxID)
	}
	return events, nil
}

// unmarshalMap unmarshals a JSON object string into a map; returns nil for empty input.
func unmarshalMap(js string) map[string]any {
	if js == "" || js == "null" {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(js), &m); err != nil {
		return nil
	}
	return m
}
