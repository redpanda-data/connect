package aws

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

type mockKinesis struct {
	fn func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error)
}

func (m *mockKinesis) PutRecords(ctx context.Context, input *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error) {
	return m.fn(input)
}

func testKOWriter(t *testing.T, conf string) *kinesisWriter {
	t.Helper()

	pConf, err := koOutputSpec().ParseYAML(conf, nil)
	require.NoError(t, err)

	kConf, err := koConfigFromParsed(pConf)
	require.NoError(t, err)

	w, err := newKinesisWriter(kConf, service.MockResources())
	require.NoError(t, err)

	return w
}

func TestKinesisWriteSinglePartMessage(t *testing.T) {
	k := testKOWriter(t, `
stream: foo
partition_key: ${! json("id") }
`)
	k.kinesis = &mockKinesis{
		fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
			if exp, act := 1, len(input.Records); exp != act {
				return nil, fmt.Errorf("expected input to have records with length %d, got %d", exp, act)
			}
			if exp, act := "123", input.Records[0].PartitionKey; exp != *act {
				return nil, fmt.Errorf("expected record to have partition key %s, got %s", exp, *act)
			}
			return &kinesis.PutRecordsOutput{}, nil
		},
	}

	msg := service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"bar","id":123}`)),
	}

	assert.NoError(t, k.WriteBatch(context.Background(), msg))
}

func TestKinesisWriteMultiPartMessage(t *testing.T) {
	parts := []struct {
		data []byte
		key  string
	}{
		{[]byte(`{"foo":"bar","id":123}`), "123"},
		{[]byte(`{"foo":"baz","id":456}`), "456"},
	}

	k := testKOWriter(t, `
stream: foo
partition_key: ${! json("id") }
`)
	k.kinesis = &mockKinesis{
		fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
			if exp, act := len(parts), len(input.Records); exp != act {
				return nil, fmt.Errorf("expected input to have records with length %d, got %d", exp, act)
			}
			for i, p := range parts {
				if exp, act := p.key, input.Records[i].PartitionKey; exp != *act {
					return nil, fmt.Errorf("expected record %d to have partition key %s, got %s", i, exp, *act)
				}
			}
			return &kinesis.PutRecordsOutput{}, nil
		},
	}

	var msg service.MessageBatch
	for _, p := range parts {
		part := service.NewMessage(p.data)
		msg = append(msg, part)
	}

	if err := k.WriteBatch(context.Background(), msg); err != nil {
		t.Error(err)
	}
}

func TestKinesisWriteChunk(t *testing.T) {
	batchLengths := []int{}
	n := 1200

	k := testKOWriter(t, `
stream: foo
partition_key: ${! json("id") }
`)
	k.kinesis = &mockKinesis{
		fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
			batchLengths = append(batchLengths, len(input.Records))
			return &kinesis.PutRecordsOutput{}, nil
		},
	}

	var msg service.MessageBatch
	for i := 0; i < n; i++ {
		part := service.NewMessage([]byte(`{"foo":"bar","id":123}`))
		msg = append(msg, part)
	}

	if err := k.WriteBatch(context.Background(), msg); err != nil {
		t.Error(err)
	}
	if exp, act := n/kinesisMaxRecordsCount+1, len(batchLengths); act != exp {
		t.Errorf("Expected kinesis PutRecords to have call count %d, got %d", exp, act)
	}
	for i, act := range batchLengths {
		exp := n
		if exp > kinesisMaxRecordsCount {
			exp = kinesisMaxRecordsCount
			n -= kinesisMaxRecordsCount
		}
		if act != exp {
			t.Errorf("Expected kinesis PutRecords call %d to have batch size %d, got %d", i, exp, act)
		}
	}
}

func TestKinesisWriteChunkWithThrottling(t *testing.T) {
	t.Parallel()
	batchLengths := []int{}
	n := 1200

	k := testKOWriter(t, `
stream: foo
partition_key: ${! json("id") }
`)
	k.kinesis = &mockKinesis{
		fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
			count := len(input.Records)
			batchLengths = append(batchLengths, count)
			var failed int32
			output := kinesis.PutRecordsOutput{
				Records: make([]types.PutRecordsResultEntry, count),
			}
			for i := 0; i < count; i++ {
				var entry types.PutRecordsResultEntry
				if i >= 300 {
					failed++
					entry.ErrorCode = aws.String("ProvisionedThroughputExceededException")
				}
				output.Records[i] = entry
			}
			output.FailedRecordCount = aws.Int32(failed)
			return &output, nil
		},
	}

	var msg service.MessageBatch
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
		t.Errorf("Expected kinesis PutRecords to have call count %d, got %d", exp, act)
	}
	for i, act := range batchLengths {
		if exp := expectedLengths[i]; act != exp {
			t.Errorf("Expected kinesis PutRecords call %d to have batch size %d, got %d", i, exp, act)
		}
	}
}

func TestKinesisWriteError(t *testing.T) {
	t.Parallel()
	var calls int

	k := testKOWriter(t, `
stream: foo
partition_key: ${! json("id") }
max_retries: 2
`)
	k.kinesis = &mockKinesis{
		fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
			calls++
			return nil, errors.New("blah")
		},
	}

	msg := service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"bar"}`)),
	}

	if exp, err := "blah", k.WriteBatch(context.Background(), msg); err.Error() != exp {
		t.Errorf("Expected err to equal %s, got %v", exp, err)
	}
	if exp, act := 3, calls; act != exp {
		t.Errorf("Expected kinesis.PutRecords to have call count %d, got %d", exp, act)
	}
}

func TestKinesisWriteMessageThrottling(t *testing.T) {
	t.Parallel()
	var calls [][]types.PutRecordsRequestEntry

	k := testKOWriter(t, `
stream: foo
partition_key: ${! json("id") }
`)
	k.kinesis = &mockKinesis{
		fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
			records := make([]types.PutRecordsRequestEntry, len(input.Records))
			copy(records, input.Records)
			calls = append(calls, records)
			var failed int32
			var output kinesis.PutRecordsOutput
			for i := 0; i < len(input.Records); i++ {
				entry := types.PutRecordsResultEntry{}
				if i > 0 {
					failed++
					entry.ErrorCode = aws.String("ProvisionedThroughputExceededException")
				}
				output.Records = append(output.Records, entry)
			}
			output.FailedRecordCount = aws.Int32(failed)
			return &output, nil
		},
	}

	msg := service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"bar","id":123}`)),
		service.NewMessage([]byte(`{"foo":"baz","id":456}`)),
		service.NewMessage([]byte(`{"foo":"qux","id":789}`)),
	}

	if err := k.WriteBatch(context.Background(), msg); err != nil {
		t.Error(err)
	}
	if exp, act := len(msg), len(calls); act != exp {
		t.Errorf("Expected kinesis.PutRecords to have call count %d, got %d", exp, act)
	}
	for i, c := range calls {
		if exp, act := len(msg)-i, len(c); act != exp {
			t.Errorf("Expected kinesis.PutRecords call %d input to have Records with length %d, got %d", i, exp, act)
		}
	}
}

func TestKinesisWriteBackoffMaxRetriesExceeded(t *testing.T) {
	t.Parallel()
	var calls int

	k := testKOWriter(t, `
stream: foo
partition_key: ${! json("id") }
max_retries: 2
`)
	k.kinesis = &mockKinesis{
		fn: func(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
			calls++
			var output kinesis.PutRecordsOutput
			output.FailedRecordCount = aws.Int32(1)
			output.Records = append(output.Records, types.PutRecordsResultEntry{
				ErrorCode: aws.String("ProvisionedThroughputExceededException"),
			})
			return &output, nil
		},
	}

	msg := service.MessageBatch{
		service.NewMessage([]byte(`{"foo":"bar","id":123}`)),
	}

	if err := k.WriteBatch(context.Background(), msg); err == nil {
		t.Error(errors.New("expected kinesis.Write to error"))
	}
	if exp := 3; calls != exp {
		t.Errorf("Expected kinesis.PutRecords to have call count %d, got %d", exp, calls)
	}
}
