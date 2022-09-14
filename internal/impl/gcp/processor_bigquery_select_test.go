package gcp

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

	"github.com/benthosdev/benthos/v4/public/service"
)

var testBQProcessorYAML = `
project: job-project
table: bigquery-public-data.samples.shakespeare
columns:
  - word
  - sum(word_count) as total_count
where: length(word) >= ?
suffix: |
  GROUP BY word
  ORDER BY total_count DESC
  LIMIT 10
args_mapping: |
  root = [ this.term ]
`

func TestGCPBigQuerySelectProcessor(t *testing.T) {
	spec := newBigQuerySelectProcessorConfig()

	parsed, err := spec.ParseYAML(testBQProcessorYAML, nil)
	require.NoError(t, err)

	proc, err := newBigQuerySelectProcessor(parsed, &bigQueryProcessorOptions{
		clientOptions: []option.ClientOption{option.WithoutAuthentication()},
	})
	require.NoError(t, err)

	mockClient := &mockBQClient{}
	proc.client = mockClient

	expected := []map[string]any{
		{"total_count": 25568, "word": "the"},
		{"total_count": 19649, "word": "and"},
	}

	expectedMsg, err := json.Marshal(expected)
	require.NoError(t, err)

	var rows []string
	for _, v := range expected {
		row, err := json.Marshal(v)
		require.NoError(t, err)

		rows = append(rows, string(row))
	}

	iter := &mockBQIterator{
		rows: rows,
	}

	mockClient.On("RunQuery", mock.Anything, mock.Anything).Return(iter, nil)

	inbatch := service.MessageBatch{
		service.NewMessage([]byte(`{"term": "test1"}`)),
		service.NewMessage([]byte(`{"term": "test2"}`)),
	}

	batches, err := proc.ProcessBatch(context.Background(), inbatch)
	require.NoError(t, err)
	require.Len(t, batches, 1)

	// Assert that we generated the right parameters for each BQ query
	mockClient.AssertNumberOfCalls(t, "RunQuery", 2)
	call1 := mockClient.Calls[0]
	args1 := call1.Arguments[1].(*bqQueryBuilderOptions).args
	require.ElementsMatch(t, args1, []string{"test1"})
	call2 := mockClient.Calls[1]
	args2 := call2.Arguments[1].(*bqQueryBuilderOptions).args
	require.ElementsMatch(t, args2, []string{"test2"})

	outbatch := batches[0]
	require.Len(t, outbatch, 2)

	msg1, err := outbatch[0].AsBytes()
	require.NoError(t, err)
	require.JSONEq(t, string(expectedMsg), string(msg1))

	msg2, err := outbatch[0].AsBytes()
	require.NoError(t, err)
	require.JSONEq(t, string(expectedMsg), string(msg2))

	mockClient.AssertExpectations(t)
}

func TestGCPBigQuerySelectProcessor_IteratorError(t *testing.T) {
	spec := newBigQuerySelectProcessorConfig()

	parsed, err := spec.ParseYAML(testBQProcessorYAML, nil)
	require.NoError(t, err)

	proc, err := newBigQuerySelectProcessor(parsed, &bigQueryProcessorOptions{
		clientOptions: []option.ClientOption{option.WithoutAuthentication()},
	})
	require.NoError(t, err)

	mockClient := &mockBQClient{}
	proc.client = mockClient

	testErr := errors.New("simulated err")
	iter := &mockBQIterator{
		rows:   []string{`{"total_count": 25568, "word": "the"}`},
		err:    testErr,
		errIdx: 1,
	}

	mockClient.On("RunQuery", mock.Anything, mock.Anything).Return(iter, nil)

	inmsg := []byte(`{"term": "test1"}`)
	inbatch := service.MessageBatch{
		service.NewMessage(inmsg),
	}

	batches, err := proc.ProcessBatch(context.Background(), inbatch)
	require.NoError(t, err)
	require.Len(t, batches, 1)

	// Assert that we generated the right parameters for each BQ query
	mockClient.AssertNumberOfCalls(t, "RunQuery", 1)
	call1 := mockClient.Calls[0]
	args1 := call1.Arguments[1].(*bqQueryBuilderOptions).args
	require.ElementsMatch(t, args1, []string{"test1"})

	outbatch := batches[0]
	require.Len(t, outbatch, 1)

	msg1, err := outbatch[0].AsBytes()
	require.NoError(t, err)
	require.JSONEq(t, string(inmsg), string(msg1))

	msgErr := outbatch[0].GetError()
	require.Contains(t, msgErr.Error(), testErr.Error())

	mockClient.AssertExpectations(t)
}
