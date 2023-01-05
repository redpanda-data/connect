package gcp

import (
	"context"
	"encoding/json"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"

	"github.com/benthosdev/benthos/v4/public/service"
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
	args := client.Mock.Called(ctx, options)

	var iter bigqueryIterator
	if mi := args.Get(0); mi != nil {
		iter = mi.(bigqueryIterator)
	}

	return iter, args.Error(1)
}

func (client *mockBQClient) Close() error {
	return nil
}

func TestParseQueryPriority(t *testing.T) {
	spec := service.NewConfigSpec().Field(service.NewStringField("foo").Default(""))

	conf, err := spec.ParseYAML(`foo: batch`, nil)
	require.NoError(t, err)
	priority, err := parseQueryPriority(conf, "foo")
	require.NoError(t, err)
	require.Equal(t, priority, bigquery.BatchPriority)

	conf, err = spec.ParseYAML(`foo: interactive`, nil)
	require.NoError(t, err)
	priority, err = parseQueryPriority(conf, "foo")
	require.NoError(t, err)
	require.Equal(t, priority, bigquery.InteractivePriority)
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
