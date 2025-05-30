// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package changestreams

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"google.golang.org/api/iterator"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams/metadata"
)

type readResult struct {
	ChangeRecords []*ChangeRecord `spanner:"ChangeRecord" json:"change_record"`
}

type querier interface {
	query(ctx context.Context, pm metadata.PartitionMetadata, cb func(ctx context.Context, cr ChangeRecord) error) error
}

type clientQuerier struct {
	client     *spanner.Client
	dialect    dialect
	streamName string
	priority   spannerpb.RequestOptions_Priority
	log        *service.Logger
}

// query executes a change stream query for the specified stream and partition.
// It processes each record from the change stream and calls the callback function.
func (q clientQuerier) query(
	ctx context.Context,
	pm metadata.PartitionMetadata,
	cb func(ctx context.Context, cr ChangeRecord) error,
) error {
	var stmt spanner.Statement
	if q.isPostgres() {
		stmt = spanner.Statement{
			SQL: fmt.Sprintf(`SELECT * FROM spanner.read_json_%s($1, $2, $3, $4, null)`, q.streamName),
			Params: map[string]any{
				"p1": pm.Watermark,
				"p2": pm.EndTimestamp,
				"p3": pm.PartitionToken,
				"p4": pm.HeartbeatMillis,
			},
		}
		// Convert to NULL
		if pm.EndTimestamp.IsZero() {
			stmt.Params["p2"] = nil
		}
		if pm.PartitionToken == "" {
			stmt.Params["p3"] = nil
		}
	} else {
		stmt = spanner.Statement{
			SQL: fmt.Sprintf(`SELECT ChangeRecord FROM READ_%s(@start_timestamp, @end_timestamp, @partition_token, @heartbeat_millis)`, q.streamName),
			Params: map[string]any{
				"start_timestamp":  pm.Watermark,
				"end_timestamp":    pm.EndTimestamp,
				"partition_token":  pm.PartitionToken,
				"heartbeat_millis": pm.HeartbeatMillis,
			},
		}
		// Convert to NULL
		if pm.EndTimestamp.IsZero() {
			stmt.Params["end_timestamp"] = nil
		}
		if pm.PartitionToken == "" {
			stmt.Params["partition_token"] = nil
		}
	}
	q.log.Tracef("Executing query: %s with params: %v", stmt.SQL, stmt.Params)

	iter := q.client.Single().QueryWithOptions(ctx, stmt, spanner.QueryOptions{Priority: q.priority})
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return fmt.Errorf("get change stream results: %w", err)
		}

		if q.isPostgres() {
			cr, err := decodePostgresRow(row)
			if err != nil {
				return fmt.Errorf("decode postgres row: %w", err)
			}
			if err := cb(ctx, cr); err != nil {
				return err
			}
		} else {
			var rr readResult
			if err := row.ToStruct(&rr); err != nil {
				return fmt.Errorf("row to struct: %w", err)
			}
			for _, cr := range rr.ChangeRecords {
				if err := cb(ctx, *cr); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (q clientQuerier) isPostgres() bool {
	return q.dialect == dialectPostgreSQL
}
