// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package changestreams

import (
	"encoding/json"
	"fmt"

	"cloud.google.com/go/spanner"
)

var emptyChangeRecord = ChangeRecord{}

func decodePostgresRow(row *spanner.Row) (ChangeRecord, error) {
	var col spanner.NullJSON
	if err := row.Column(0, &col); err != nil {
		return emptyChangeRecord, fmt.Errorf("extract column from row: %w", err)
	}

	b, err := col.MarshalJSON()
	if err != nil {
		return emptyChangeRecord, fmt.Errorf("marshal JSON column: %w", err)
	}

	var pgcr struct {
		DataChangeRecord      *DataChangeRecord      `json:"data_change_record"`
		HeartbeatRecord       *HeartbeatRecord       `json:"heartbeat_record"`
		ChildPartitionsRecord *ChildPartitionsRecord `json:"child_partitions_record"`
	}
	if err := json.Unmarshal(b, &pgcr); err != nil {
		return emptyChangeRecord, fmt.Errorf("unmarshal JSON data: %w", err)
	}

	var cr ChangeRecord
	if pgcr.DataChangeRecord != nil {
		cr.DataChangeRecords = []*DataChangeRecord{pgcr.DataChangeRecord}
	}
	if pgcr.HeartbeatRecord != nil {
		cr.HeartbeatRecords = []*HeartbeatRecord{pgcr.HeartbeatRecord}
	}
	if pgcr.ChildPartitionsRecord != nil {
		cr.ChildPartitionsRecords = []*ChildPartitionsRecord{pgcr.ChildPartitionsRecord}
	}
	return cr, nil
}
