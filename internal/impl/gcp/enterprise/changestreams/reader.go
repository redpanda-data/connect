// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md
//
// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package changestreams

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"
)

// ReadResult is the result of the read change records from the partition.
type ReadResult struct {
	PartitionToken string          `json:"partition_token"`
	ChangeRecords  []*ChangeRecord `spanner:"ChangeRecord" json:"change_record"`
}

type partitionState int

const (
	partitionStateUnknown partitionState = iota
	partitionStateReading
	partitionStateFinished
)

type tokenTimestamp struct {
	token          string
	startTimestamp time.Time
}

// Reader is the change stream reader.
type Reader struct {
	client            *spanner.Client
	handler           Handler
	streamID          string
	startTimestamp    time.Time
	endTimestamp      time.Time
	heartbeatInterval time.Duration
	dialect           dialect

	ch       chan tokenTimestamp
	states   map[string]partitionState
	statesMu sync.Mutex
}

// Config is the configuration for the reader.
type Config struct {
	ProjectID         string
	InstanceID        string
	DatabaseID        string
	StreamID          string
	StartTimestamp    time.Time // If StartTimestamp is a zero value of time.Time, reader reads from the current timestamp.
	EndTimestamp      time.Time // If EndTimestamp is a zero value of time.Time, reader reads until it is cancelled.
	HeartbeatInterval time.Duration

	SpannerClientConfig  spanner.ClientConfig
	SpannerClientOptions []option.ClientOption
}

// Handler is responsible for handling the read results.
type Handler interface {
	OnReadResult(result *ReadResult) error
	OnReadError(err error)
}

// NewReaderWithConfig creates a new reader with a given configuration.
func NewReaderWithConfig(ctx context.Context, conf Config, h Handler) (*Reader, error) {
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", conf.ProjectID, conf.InstanceID, conf.DatabaseID)
	client, err := spanner.NewClientWithConfig(ctx, dbPath, conf.SpannerClientConfig, conf.SpannerClientOptions...)
	if err != nil {
		return nil, err
	}

	dialect, err := detectDialect(ctx, client)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to detect dialect: %w", err)
	}

	heartbeatInterval := conf.HeartbeatInterval
	if heartbeatInterval == 0 {
		heartbeatInterval = 10 * time.Second
	}

	ch := make(chan tokenTimestamp, 1)
	ch <- tokenTimestamp{
		token:          "",
		startTimestamp: conf.StartTimestamp,
	}

	return &Reader{
		client:            client,
		handler:           h,
		streamID:          conf.StreamID,
		startTimestamp:    conf.StartTimestamp,
		endTimestamp:      conf.EndTimestamp,
		heartbeatInterval: heartbeatInterval,

		dialect: dialect,
		ch:      ch,
		states:  make(map[string]partitionState),
	}, nil
}

// Close closes the reader.
func (r *Reader) Close() {
	r.client.Close()
}

// Start starts the reader.
//
// It reads from the change stream and calls the handler for each read result.
// It stops when the context is cancelled.
func (r *Reader) Start(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case token := <-r.ch:
			go func() {
				if err := r.read(ctx, token.token, token.startTimestamp); err != nil {
					r.handler.OnReadError(err)
				}
			}()
		}
	}
}

func (r *Reader) read(ctx context.Context, partitionToken string, startTimestamp time.Time) error {
	if !r.markStateReading(partitionToken) {
		return nil
	}

	var stmt spanner.Statement
	if r.isPostgres() {
		stmt = spanner.Statement{
			SQL: fmt.Sprintf(`SELECT * FROM spanner.read_json_%s($1, $2, $3, $4, null)`, r.streamID),
			Params: map[string]interface{}{
				"p1": startTimestamp,
				"p2": r.endTimestamp,
				"p3": partitionToken,
				"p4": r.heartbeatInterval / time.Millisecond,
			},
		}
		if r.endTimestamp.IsZero() {
			// Must be converted to NULL.
			stmt.Params["p2"] = nil
		}
		if partitionToken == "" {
			// Must be converted to NULL.
			stmt.Params["p3"] = nil
		}
	} else {
		stmt = spanner.Statement{
			SQL: fmt.Sprintf(`SELECT ChangeRecord FROM READ_%s(@start_timestamp, @end_timestamp, @partition_token, @heartbeat_millis_second)`, r.streamID),
			Params: map[string]interface{}{
				"start_timestamp":         startTimestamp,
				"end_timestamp":           r.endTimestamp,
				"partition_token":         partitionToken,
				"heartbeat_millis_second": r.heartbeatInterval / time.Millisecond,
			},
		}
		if r.endTimestamp.IsZero() {
			// Must be converted to NULL.
			stmt.Params["end_timestamp"] = nil
		}
		if partitionToken == "" {
			// Must be converted to NULL.
			stmt.Params["partition_token"] = nil
		}
	}

	if err := r.client.Single().Query(ctx, stmt).Do(func(row *spanner.Row) error {
		rr := ReadResult{PartitionToken: partitionToken}
		if r.isPostgres() {
			changeRecord, err := decodePostgresRow(row)
			if err != nil {
				return err
			}
			rr.ChangeRecords = []*ChangeRecord{&changeRecord}
		} else {
			if err := row.ToStruct(&rr); err != nil {
				return err
			}
		}
		return r.onReadResult(ctx, &rr)
	}); err != nil {
		return err
	}

	r.markStateFinished(partitionToken)
	return nil
}

func (r *Reader) markStateReading(partitionToken string) bool {
	r.statesMu.Lock()
	defer r.statesMu.Unlock()

	if _, ok := r.states[partitionToken]; ok {
		// Already started by another parent.
		return false
	}
	r.states[partitionToken] = partitionStateReading
	return true
}

func (r *Reader) markStateFinished(partitionToken string) {
	r.statesMu.Lock()
	defer r.statesMu.Unlock()

	r.states[partitionToken] = partitionStateFinished
}

func (r *Reader) onReadResult(ctx context.Context, rr *ReadResult) error {
	err := r.handler.OnReadResult(rr)

	for _, cr := range rr.ChangeRecords {
		for _, cpr := range cr.ChildPartitionsRecords {
			for _, cp := range cpr.ChildPartitions {
				if r.canReadChild(cp) {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case r.ch <- tokenTimestamp{
						token:          cp.Token,
						startTimestamp: cpr.StartTimestamp,
					}:
						// ok
					}
				}
			}
		}
	}

	return err
}

func (r *Reader) canReadChild(partition *ChildPartition) bool {
	r.statesMu.Lock()
	defer r.statesMu.Unlock()

	for _, parent := range partition.ParentPartitionTokens {
		if r.states[parent] != partitionStateFinished {
			return false
		}
	}
	return true
}

func (r *Reader) isPostgres() bool {
	return r.dialect == dialectPostgreSQL
}
