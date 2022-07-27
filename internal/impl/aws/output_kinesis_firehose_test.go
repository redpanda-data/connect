package aws

import (
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

type mockKinesisFirehose struct {
	firehoseiface.FirehoseAPI
	fn func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error)
}

func (m *mockKinesisFirehose) PutRecordBatch(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
	return m.fn(input)
}

func TestKinesisFirehoseWriteSinglePartMessage(t *testing.T) {
	k := kinesisFirehoseWriter{
		backoffCtor: func() backoff.BackOff {
			return backoff.NewExponentialBackOff()
		},
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		firehose: &mockKinesisFirehose{
			fn: func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				if exp, act := 1, len(input.Records); exp != act {
					return nil, fmt.Errorf("expected input to have records with length %d, got %d", exp, act)
				}
				return &firehose.PutRecordBatchOutput{}, nil
			},
		},
		log: log.Noop(),
	}

	msg := message.Batch{message.NewPart([]byte(`{"foo":"bar","id":123}`))}
	if err := k.Write(msg); err != nil {
		t.Error(err)
	}
}

func TestKinesisFirehoseWriteMultiPartMessage(t *testing.T) {
	parts := []struct {
		data []byte
		key  string
	}{
		{[]byte(`{"foo":"bar","id":123}`), "123"},
		{[]byte(`{"foo":"baz","id":456}`), "456"},
	}
	k := kinesisFirehoseWriter{
		backoffCtor: func() backoff.BackOff {
			return backoff.NewExponentialBackOff()
		},
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		firehose: &mockKinesisFirehose{
			fn: func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				if exp, act := len(parts), len(input.Records); exp != act {
					return nil, fmt.Errorf("expected input to have records with length %d, got %d", exp, act)
				}
				return &firehose.PutRecordBatchOutput{}, nil
			},
		},
		log: log.Noop(),
	}

	msg := message.QuickBatch(nil)
	for _, p := range parts {
		part := message.NewPart(p.data)
		msg = append(msg, part)
	}

	if err := k.Write(msg); err != nil {
		t.Error(err)
	}
}

func TestKinesisFirehoseWriteChunk(t *testing.T) {
	batchLengths := []int{}
	n := 1200
	k := kinesisFirehoseWriter{
		backoffCtor: func() backoff.BackOff {
			return backoff.NewExponentialBackOff()
		},
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		firehose: &mockKinesisFirehose{
			fn: func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				batchLengths = append(batchLengths, len(input.Records))
				return &firehose.PutRecordBatchOutput{}, nil
			},
		},
		log: log.Noop(),
	}

	msg := message.QuickBatch(nil)
	for i := 0; i < n; i++ {
		part := message.NewPart([]byte(`{"foo":"bar","id":123}`))
		msg = append(msg, part)
	}

	if err := k.Write(msg); err != nil {
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
	k := kinesisFirehoseWriter{
		backoffCtor: func() backoff.BackOff {
			return backoff.NewExponentialBackOff()
		},
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		firehose: &mockKinesisFirehose{
			fn: func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				count := len(input.Records)
				batchLengths = append(batchLengths, count)
				var failed int64
				output := firehose.PutRecordBatchOutput{
					RequestResponses: make([]*firehose.PutRecordBatchResponseEntry, count),
				}
				for i := 0; i < count; i++ {
					var entry firehose.PutRecordBatchResponseEntry
					if i >= 300 {
						failed++
						entry.SetErrorCode(firehose.ErrCodeServiceUnavailableException)
						entry.SetErrorMessage("Mocked ProvisionedThroughputExceededException")
					}
					output.RequestResponses[i] = &entry
				}
				output.SetFailedPutCount(failed)
				return &output, nil
			},
		},
		log: log.Noop(),
	}

	msg := message.QuickBatch(nil)
	for i := 0; i < n; i++ {
		part := message.NewPart([]byte(`{"foo":"bar","id":123}`))
		msg = append(msg, part)
	}

	expectedLengths := []int{
		500, 500, 500, 300,
	}

	if err := k.Write(msg); err != nil {
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
	k := kinesisFirehoseWriter{
		backoffCtor: func() backoff.BackOff {
			return backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 2)
		},
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		firehose: &mockKinesisFirehose{
			fn: func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				calls++
				return nil, errors.New("blah")
			},
		},
		log: log.Noop(),
	}

	msg := message.Batch{
		message.NewPart([]byte(`{"foo":"bar"}`)),
	}

	if exp, err := "blah", k.Write(msg); err.Error() != exp {
		t.Errorf("Expected err to equal %s, got %v", exp, err)
	}
	if exp, act := 3, calls; act != exp {
		t.Errorf("Expected firehose PutRecordbatch to have call count %d, got %d", exp, act)
	}
}

func TestKinesisFirehoseWriteMessageThrottling(t *testing.T) {
	t.Parallel()
	var calls [][]*firehose.Record
	k := kinesisFirehoseWriter{
		backoffCtor: func() backoff.BackOff {
			return backoff.NewExponentialBackOff()
		},
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		firehose: &mockKinesisFirehose{
			fn: func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				records := make([]*firehose.Record, len(input.Records))
				copy(records, input.Records)
				calls = append(calls, records)
				var failed int64
				var output firehose.PutRecordBatchOutput
				for i := 0; i < len(input.Records); i++ {
					entry := firehose.PutRecordBatchResponseEntry{}
					if i > 0 {
						failed++
						entry.SetErrorCode(firehose.ErrCodeServiceUnavailableException)
					}
					output.RequestResponses = append(output.RequestResponses, &entry)
				}
				output.SetFailedPutCount(failed)
				return &output, nil
			},
		},
		log: log.Noop(),
	}

	msg := message.Batch{
		message.NewPart([]byte(`{"foo":"bar","id":123}`)),
		message.NewPart([]byte(`{"foo":"baz","id":456}`)),
		message.NewPart([]byte(`{"foo":"qux","id":789}`)),
	}

	if err := k.Write(msg); err != nil {
		t.Error(err)
	}
	if exp, act := msg.Len(), len(calls); act != exp {
		t.Errorf("Expected kinesis firehose PutRecordBatch to have call count %d, got %d", exp, act)
	}
	for i, c := range calls {
		if exp, act := msg.Len()-i, len(c); act != exp {
			t.Errorf("Expected kinesis firehose PutRecordBatch call %d input to have Records with length %d, got %d", i, exp, act)
		}
	}
}

func TestKinesisFirehoseWriteBackoffMaxRetriesExceeded(t *testing.T) {
	t.Parallel()
	var calls int
	k := kinesisFirehoseWriter{
		backoffCtor: func() backoff.BackOff {
			return backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 2)
		},
		session: session.Must(session.NewSession(&aws.Config{
			Credentials: credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		})),
		firehose: &mockKinesisFirehose{
			fn: func(input *firehose.PutRecordBatchInput) (*firehose.PutRecordBatchOutput, error) {
				calls++
				var output firehose.PutRecordBatchOutput
				output.SetFailedPutCount(int64(1))
				output.RequestResponses = append(output.RequestResponses, &firehose.PutRecordBatchResponseEntry{
					ErrorCode: aws.String(firehose.ErrCodeServiceUnavailableException),
				})
				return &output, nil
			},
		},
		log: log.Noop(),
	}

	msg := message.Batch{
		message.NewPart([]byte(`{"foo":"bar","id":123}`)),
	}

	if err := k.Write(msg); err == nil {
		t.Error(errors.New("expected kinesis.Write to error"))
	}
	if exp := 3; calls != exp {
		t.Errorf("Expected kinesis firehose PutRecordBatch to have call count %d, got %d", exp, calls)
	}
}
