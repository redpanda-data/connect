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
	"fmt"

	"github.com/stretchr/testify/mock"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/enterprise/changestreams/metadata"
)

type mockQuerier struct {
	mock.Mock
}

func (m *mockQuerier) query(ctx context.Context, pm metadata.PartitionMetadata, cb func(ctx context.Context, cr ChangeRecord) error) error {
	args := m.Called(ctx, pm, cb)
	return args.Error(0)
}

func (m *mockQuerier) ExpectQuery(partitionToken string) *mock.Call {
	return m.On("query", mock.Anything, mock.MatchedBy(func(actual metadata.PartitionMetadata) bool {
		return actual.PartitionToken == partitionToken
	}), mock.Anything)
}

func (m *mockQuerier) ExpectQueryWithRecords(partitionToken string, records ...ChangeRecord) *mock.Call {
	return m.ExpectQuery(partitionToken).Return(nil).Run(func(args mock.Arguments) {
		ctx := args.Get(0).(context.Context)
		cb := args.Get(2).(func(ctx context.Context, cr ChangeRecord) error)
		for _, record := range records {
			if err := cb(ctx, record); err != nil {
				// We can't return an error from a Run function.
				panic(fmt.Sprintf("error in callback: %v", err))
			}
		}
	})
}
