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
)

// filteredCallback returns a CallbackFunc that filters out DataChangeRecords
// that don't match the provided filter.
func filteredCallback(cb CallbackFunc, filter func(dcr *DataChangeRecord) bool) CallbackFunc {
	return func(ctx context.Context, partitionToken string, dcr *DataChangeRecord) error {
		if dcr != nil && !filter(dcr) {
			return nil
		}
		return cb(ctx, partitionToken, dcr)
	}
}

func modTypeFilter(allowedModTypes []string) func(dcr *DataChangeRecord) bool {
	m := map[string]struct{}{}
	for _, modType := range allowedModTypes {
		m[modType] = struct{}{}
	}
	return func(dcr *DataChangeRecord) bool {
		_, ok := m[dcr.ModType]
		return ok
	}
}
