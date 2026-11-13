// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package incrementalsnapshot

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// RequireNewImage rejects stream views whose records carry no new image:
// the incremental window drops a snapshot item on the promise that the
// stream will deliver the item's current value.
func RequireNewImage(tableName string, spec *dynamodbtypes.StreamSpecification) error {
	if spec == nil || !aws.ToBool(spec.StreamEnabled) {
		return fmt.Errorf("table %s: snapshot_mode incremental requires a stream", tableName)
	}
	switch spec.StreamViewType {
	case dynamodbtypes.StreamViewTypeNewImage, dynamodbtypes.StreamViewTypeNewAndOldImages:
		return nil
	default:
		return fmt.Errorf("table %s: snapshot_mode incremental requires stream view NEW_IMAGE or NEW_AND_OLD_IMAGES, got %s", tableName, spec.StreamViewType)
	}
}

// RequireSignalStreamView checks that the signal table's stream carries the
// inserted item, which is where signals are read from.
func RequireSignalStreamView(tableName string, spec *dynamodbtypes.StreamSpecification) error {
	got := "no stream enabled"
	if spec != nil && aws.ToBool(spec.StreamEnabled) {
		switch spec.StreamViewType {
		case dynamodbtypes.StreamViewTypeNewImage, dynamodbtypes.StreamViewTypeNewAndOldImages:
			return nil
		default:
			got = string(spec.StreamViewType)
		}
	}
	return fmt.Errorf("signal_table_name: table %s needs a stream with view NEW_IMAGE or NEW_AND_OLD_IMAGES, got %s", tableName, got)
}

// DedupeTables returns tables without repeats, keeping first occurrences in
// order.
func DedupeTables(tables []string) []string {
	seen := make(map[string]struct{}, len(tables))
	out := make([]string, 0, len(tables))
	for _, t := range tables {
		if _, exists := seen[t]; exists {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}
