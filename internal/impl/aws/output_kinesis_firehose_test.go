package aws

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

type mockKinesisFirehose struct {
	firehoseAPI
	fn func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error)
}

func (m *mockKinesisFirehose) PutRecordBatch(ctx context.Context, input *firehose.PutRecordBatchInput, optFns ...func(*firehose.Options)) (*firehose.PutRecordBatchOutput, error) {
	return m.fn(input)
}

func testKFO(t *testing.T, m *mockKinesisFirehose) *kinesisFirehoseWriter {
	t.Helper()

	conf, err := config.LoadDefaultConfig(context.TODO(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
		config.WithRegion("us-east-1"),
	)
	require.NoError(t, err)

	return &kinesisFirehoseWriter{
		conf: kfoConfig{
			Stream: "foo",
			backoffCtor: func() backoff.BackOff {
				return backoff.NewExponentialBackOff()
			},
			aconf: conf,
		},
		firehose: m,
	}

}

func TestKinesisFirehoseWriteSinglePartMessage(t *testing.T) {
	k := testKFO(t, &mockKinesisFirehose{
		fn: func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
			if exp, act := 1, len(input.Records); exp != act {
				return nil, fmt.Errorf("expected input to have records with length %d, got %d", exp, act)
			}
			return &firehose.PutRecordBatchOutput{}, nil
		},
	})

	msg := service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"bar","id":123}`)),
	}
	require.NoError(t, k.WriteBatch(context.Background(), msg))
}

func TestKinesisFirehoseWriteMultiPartMessage(t *testing.T) {
	parts := []struct {
		data []byte
		key  string
	}{
		{[]byte(`{"foo":"bar","id":123}`), "123"},
		{[]byte(`{"foo":"baz","id":456}`), "456"},
	}

	k := testKFO(t, &mockKinesisFirehose{
		fn: func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
			if exp, act := len(parts), len(input.Records); exp != act {
				return nil, fmt.Errorf("expected input to have records with length %d, got %d", exp, act)
			}
			return &firehose.PutRecordBatchOutput{}, nil
		},
	})

	var msg service.MessageBatch
	for _, p := range parts {
		msg = append(msg, service.NewMessage(p.data))
	}
	require.NoError(t, k.WriteBatch(context.Background(), msg))
}

func TestKinesisFirehoseWriteChunk(t *testing.T) {
	batchLengths := []int{}
	n := 1200

	k := testKFO(t,
		&mockKinesisFirehose{
			fn: func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				batchLengths = append(batchLengths, len(input.Records))
				return &firehose.PutRecordBatchOutput{}, nil
			},
		},
	)

	msg := service.MessageBatch{}
	for i := 0; i < n; i++ {
		part := service.NewMessage([]byte(`{"foo":"bar","id":123}`))
		msg = append(msg, part)
	}

	if err := k.WriteBatch(context.Background(), msg); err != nil {
		t.Error(err)
	}
	if exp, act := n/kinesisMaxRecordsCount+1, len(batchLengths); act != exp {
		t.Errorf("Expected kinesis firehose PutRecordBatch to have call count %d, got %d", exp, act)
	}
	for i, act := range batchLengths {
		exp := n
		if exp > kinesisMaxRecordsCount {
			exp = kinesisMaxRecordsCount
			n -= kinesisMaxRecordsCount
		}
		if act != exp {
			t.Errorf("Expected kinesis firehose PutRecordBatch call %d to have batch size %d, got %d", i, exp, act)
		}
	}
}

func TestKinesisFirehoseWriteChunkWithThrottling(t *testing.T) {
	t.Parallel()
	batchLengths := []int{}
	n := 1200

	k := testKFO(t,
		&mockKinesisFirehose{
			fn: func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				count := len(input.Records)
				batchLengths = append(batchLengths, count)
				var failed int32
				output := firehose.PutRecordBatchOutput{
					RequestResponses: make([]types.PutRecordBatchResponseEntry, count),
				}
				for i := 0; i < count; i++ {
					var entry types.PutRecordBatchResponseEntry
					if i >= 300 {
						failed++
						entry.ErrorCode = aws.String("ServiceUnavailableException")
						entry.ErrorMessage = aws.String("Mocked ProvisionedThroughputExceededException")
					}
					output.RequestResponses[i] = entry
				}
				output.FailedPutCount = &failed
				return &output, nil
			},
		},
	)

	msg := service.MessageBatch{}
	for i := 0; i < n; i++ {
		part := service.NewMessage([]byte(`{"foo":"bar","id":123}`))
		msg = append(msg, part)
	}

	expectedLengths := []int{
		500, 500, 500, 300,
	}

	if err := k.WriteBatch(context.Background(), msg); err != nil {
		t.Error(err)
	}
	if exp, act := len(expectedLengths), len(batchLengths); act != exp {
		t.Errorf("Expected kinesis firehose PutRecordBatch to have call count %d, got %d", exp, act)
	}
	for i, act := range batchLengths {
		if exp := expectedLengths[i]; act != exp {
			t.Errorf("Expected kinesis firehose PutRecordBatch call %d to have batch size %d, got %d", i, exp, act)
		}
	}
}

func TestKinesisFirehoseWriteError(t *testing.T) {
	t.Parallel()
	var calls int

	k := testKFO(t,
		&mockKinesisFirehose{
			fn: func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				calls++
				return nil, errors.New("blah")
			},
		},
	)
	k.conf.backoffCtor = func() backoff.BackOff {
		return backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 2)
	}

	msg := service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"bar"}`)),
	}

	if exp, err := "blah", k.WriteBatch(context.Background(), msg); err.Error() != exp {
		t.Errorf("Expected err to equal %s, got %v", exp, err)
	}
	if exp, act := 3, calls; act != exp {
		t.Errorf("Expected firehose PutRecordbatch to have call count %d, got %d", exp, act)
	}
}

func TestKinesisFirehoseWriteMessageThrottling(t *testing.T) {
	t.Parallel()
	var calls [][]types.Record

	k := testKFO(t,
		&mockKinesisFirehose{
			fn: func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				records := make([]types.Record, len(input.Records))
				copy(records, input.Records)
				calls = append(calls, records)
				var failed int32
				var output firehose.PutRecordBatchOutput
				for i := 0; i < len(input.Records); i++ {
					entry := types.PutRecordBatchResponseEntry{}
					if i > 0 {
						failed++
						entry.ErrorCode = aws.String("ServiceUnavailableException")
					}
					output.RequestResponses = append(output.RequestResponses, entry)
				}
				output.FailedPutCount = &failed
				return &output, nil
			},
		},
	)

	msg := service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"bar","id":123}`)),
		service.NewMessage([]byte(`{"foo":"baz","id":456}`)),
		service.NewMessage([]byte(`{"foo":"qux","id":789}`)),
	}

	if err := k.WriteBatch(context.Background(), msg); err != nil {
		t.Error(err)
	}
	if exp, act := len(msg), len(calls); act != exp {
		t.Errorf("Expected kinesis firehose PutRecordBatch to have call count %d, got %d", exp, act)
	}
	for i, c := range calls {
		if exp, act := len(msg)-i, len(c); act != exp {
			t.Errorf("Expected kinesis firehose PutRecordBatch call %d input to have Records with length %d, got %d", i, exp, act)
		}
	}
}

func TestKinesisFirehoseWriteBackoffMaxRetriesExceeded(t *testing.T) {
	t.Parallel()
	var calls int

	k := testKFO(t,
		&mockKinesisFirehose{
			fn: func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				calls++
				var output firehose.PutRecordBatchOutput
				output.FailedPutCount = aws.Int32(1)
				output.RequestResponses = append(output.RequestResponses, types.PutRecordBatchResponseEntry{
					ErrorCode: aws.String("ServiceUnavailableException"),
				})
				return &output, nil
			},
		},
	)
	k.conf.backoffCtor = func() backoff.BackOff {
		return backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 2)
	}

	msg := service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"bar","id":123}`)),
	}

	if err := k.WriteBatch(context.Background(), msg); err == nil {
		t.Error(errors.New("expected kinesis.Write to error"))
	}
	if exp := 3; calls != exp {
		t.Errorf("Expected kinesis firehose PutRecordBatch to have call count %d, got %d", exp, calls)
	}
}
