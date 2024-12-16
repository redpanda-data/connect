// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcp

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"cloud.google.com/go/spanner/spannertest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/gcp/spannercdc"
)

var testSpannerStreamInputYAML = `
stream_dsn: "projects/test-project/instances/test-instance/databases/test-db"
stream_id: "OutboxStream"
use_in_memory_partition: true
partition_dsn: "projects/test/instances/test/databases/test-events-md" # optional default ""
partition_table: "meta_partitions_table" # optional default ""
allowed_mod_types:
  - "INSERT"
`

func TestGCPSpannerChangeStreamInput_Read(t *testing.T) {
	spec := newSpannerCDCInputConfig()

	parsed, err := spec.ParseYAML(testSpannerStreamInputYAML, nil)
	require.NoError(t, err)

	proc, err := newSpannerStreamInput(parsed, nil)
	require.NoError(t, err)

	mockStreamReader := &mockStreamReader{}
	proc.reader = mockStreamReader

	dataChangeRecord := &spannercdc.DataChangeRecord{
		CommitTimestamp:                      time.Now(),
		RecordSequence:                       "0000001",
		ServerTransactionID:                  uuid.NewString(),
		IsLastRecordInTransactionInPartition: true,
		TableName:                            "test_table",
		ColumnTypes: []*spannercdc.ColumnType{
			{Name: "ID", Type: spannercdc.Type{Code: spannercdc.TypeCodeINT64}},
			{Name: "Value", Type: spannercdc.Type{Code: spannercdc.TypeCodeSTRING}},
		},
		Mods: []*spannercdc.Mod{
			{
				Keys:      map[string]interface{}{},
				NewValues: map[string]interface{}{},
				OldValues: map[string]interface{}{},
			},
		},
		ModType:                         spannercdc.ModTypeINSERT,
		NumberOfRecordsInTransaction:    1,
		NumberOfPartitionsInTransaction: 2,
	}
	proc.changeChannel <- dataChangeRecord
	ctx := context.Background()

	msg, _, err := proc.Read(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)

	expectedMsg, err := json.Marshal(dataChangeRecord)
	require.NoError(t, err)

	gotMsg, err := msg.AsBytes()
	require.NoError(t, err)
	require.Equal(t, expectedMsg, gotMsg)
}

func TestGCPSpannerChangeStreamInput_Connect(t *testing.T) {
	spec := newSpannerCDCInputConfig()
	ctx, cancel := context.WithCancel(context.Background())

	parsed, err := spec.ParseYAML(testSpannerStreamInputYAML, nil)
	require.NoError(t, err)

	proc, err := newSpannerStreamInput(parsed, nil)
	require.NoError(t, err)

	srv, err := spannertest.NewServer("localhost:0")
	require.NoError(t, err)
	t.Setenv("SPANNER_EMULATOR_HOST", srv.Addr)

	t.Cleanup(func() {
		srv.Close()
	})

	mockStreamReader := &mockStreamReader{}
	proc.reader = mockStreamReader

	mockStreamReader.On("Stream", mock.AnythingOfType("*context.cancelCtx"), mock.Anything).Once().Return(nil)
	defer mockStreamReader.AssertExpectations(t)

	err = proc.Connect(ctx)
	require.NoError(t, err)
	cancel()
	time.Sleep(time.Millisecond * 100)
}

func TestGCPSpannerChangeStreamInput_Close(t *testing.T) {
	spec := newSpannerCDCInputConfig()
	ctx, cancel := context.WithCancel(context.Background())

	parsed, err := spec.ParseYAML(testSpannerStreamInputYAML, nil)
	require.NoError(t, err)

	proc, err := newSpannerStreamInput(parsed, nil)
	require.NoError(t, err)

	srv, err := spannertest.NewServer("localhost:0")
	require.NoError(t, err)
	t.Setenv("SPANNER_EMULATOR_HOST", srv.Addr)

	t.Cleanup(func() {
		srv.Close()
	})

	mockStreamReader := &mockStreamReader{}
	defer mockStreamReader.AssertExpectations(t)
	proc.reader = mockStreamReader

	mockStreamReader.On("Stream", mock.AnythingOfType("*context.cancelCtx"), mock.Anything).Once().Return(nil)

	mockStreamReader.On("Close", mock.Anything).Once().Return(nil)

	err = proc.Connect(ctx)
	require.NoError(t, err)
	cancel()
	time.Sleep(time.Millisecond * 100)

	err = proc.Close(context.Background())
	require.NoError(t, err)
}
