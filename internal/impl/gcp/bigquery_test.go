package gcp

import (
	"context"
	"encoding/json"

	"github.com/stretchr/testify/mock"
	"google.golang.org/api/iterator"
)

type mockBQIterator struct {
	err error

	rows []string

	idx int
	// the index at which to return an error
	errIdx int
}

func (ti *mockBQIterator) Next(dst interface{}) error {
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
