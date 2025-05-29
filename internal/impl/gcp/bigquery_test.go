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

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type mockBQIterator struct {
	err error

	rows []string

	idx int
	// the index at which to return an error
	errIdx int
}

func (ti *mockBQIterator) Next(dst any) error {
	if ti.err != nil && ti.idx == ti.errIdx {
		return ti.err
	}

	if ti.idx >= len(ti.rows) {
		return iterator.Done
	}

	row := ti.rows[ti.idx]

	ti.idx++

	return json.Unmarshal([]byte(row), dst)
}

type mockBQClient struct {
	mock.Mock
}

func (client *mockBQClient) RunQuery(ctx context.Context, options *bqQueryBuilderOptions) (bigqueryIterator, error) {
	args := client.Called(ctx, options)

	var iter bigqueryIterator
	if mi := args.Get(0); mi != nil {
		iter = mi.(bigqueryIterator)
	}

	return iter, args.Error(1)
}

func (*mockBQClient) Close() error {
	return nil
}

func TestParseQueryPriority(t *testing.T) {
	spec := service.NewConfigSpec().Field(service.NewStringField("foo").Default(""))

	conf, err := spec.ParseYAML(`foo: batch`, nil)
	require.NoError(t, err)
	priority, err := parseQueryPriority(conf, "foo")
	require.NoError(t, err)
	require.Equal(t, bigquery.BatchPriority, priority)

	conf, err = spec.ParseYAML(`foo: interactive`, nil)
	require.NoError(t, err)
	priority, err = parseQueryPriority(conf, "foo")
	require.NoError(t, err)
	require.Equal(t, bigquery.InteractivePriority, priority)
}

func TestParseQueryPriority_Empty(t *testing.T) {
	spec := service.NewConfigSpec().Field(service.NewStringField("foo").Default(""))

	conf, err := spec.ParseYAML("", nil)
	require.NoError(t, err)
	priority, err := parseQueryPriority(conf, "foo")
	require.NoError(t, err)
	require.Equal(t, priority, bigquery.QueryPriority(""))
}

func TestParseQueryPriority_Unrecognised(t *testing.T) {
	spec := service.NewConfigSpec().Field(service.NewStringField("foo").Default(""))

	conf, err := spec.ParseYAML("foo: blahblah", nil)
	require.NoError(t, err)
	priority, err := parseQueryPriority(conf, "foo")
	require.ErrorContains(t, err, "unrecognised query priority")
	require.Equal(t, priority, bigquery.QueryPriority(""))
}
