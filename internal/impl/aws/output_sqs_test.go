package aws

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestSQSHeaderCheck(t *testing.T) {
	type testCase struct {
		k, v     string
		expected bool
	}

	tests := []testCase{
		{
			k: "foo", v: "bar",
			expected: true,
		},
		{
			k: "foo.bar", v: "bar.baz",
			expected: true,
		},
		{
			k: "foo_bar", v: "bar_baz",
			expected: true,
		},
		{
			k: "foo-bar", v: "bar-baz",
			expected: true,
		},
		{
			k: ".foo", v: "bar",
			expected: false,
		},
		{
			k: "foo", v: ".bar",
			expected: true,
		},
		{
			k: "f..oo", v: "bar",
			expected: false,
		},
		{
			k: "foo", v: "ba..r",
			expected: true,
		},
		{
			k: "aws.foo", v: "bar",
			expected: false,
		},
		{
			k: "amazon.foo", v: "bar",
			expected: false,
		},
		{
			k: "foo.", v: "bar",
			expected: false,
		},
		{
			k: "foo", v: "bar.",
			expected: true,
		},
		{
			k: "fo$o", v: "bar",
			expected: false,
		},
		{
			k: "foo", v: "ba$r",
			expected: true,
		},
		{
			k: "foo_with_10_numbers", v: "bar",
			expected: true,
		},
		{
			k: "foo", v: "bar_with_10_numbers and a space",
			expected: true,
		},
		{
			k: "foo with space", v: "bar",
			expected: false,
		},
		{
			k: "iso_date", v: "1997-07-16T19:20:30.45+01:00",
			expected: true,
		},
		{
			k: "has_a_char_in_the_valid_range", v: "#x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF - Ѱ",
			expected: true,
		},
	}

	for i, test := range tests {
		if act, exp := isValidSQSAttribute(test.k, test.v), test.expected; act != exp {
			t.Errorf("Unexpected result for test '%v': %v != %v", i, act, exp)
		}
	}
}

type mockSqs struct {
	sqsiface.SQSAPI
	fn func(*sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error)
}

func (m *mockSqs) SendMessageBatch(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
	return m.fn(input)
}

type inMsg struct {
	id      string
	content string
}
type inEntries []inMsg

func TestSQSRetries(t *testing.T) {
	tCtx := context.Background()

	conf := output.NewAmazonSQSConfig()
	w, err := newSQSWriter(conf, mock.NewManager())
	require.NoError(t, err)

	var in []inEntries
	var out []*sqs.SendMessageBatchOutput
	w.sqs = &mockSqs{
		fn: func(smbi *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
			var e inEntries
			for _, entry := range smbi.Entries {
				e = append(e, inMsg{
					id:      *entry.Id,
					content: *entry.MessageBody,
				})
			}
			in = append(in, e)

			if len(out) == 0 {
				return nil, errors.New("ran out of mock outputs")
			}
			outBatch := out[0]
			out = out[1:]
			return outBatch, nil
		},
	}

	out = []*sqs.SendMessageBatchOutput{
		{
			Failed: []*sqs.BatchResultErrorEntry{
				{
					Code:        aws.String("xx"),
					Id:          aws.String("1"),
					Message:     aws.String("test error"),
					SenderFault: aws.Bool(false),
				},
			},
		},
		{},
	}

	inMsg := message.QuickBatch([][]byte{
		[]byte("hello world 1"),
		[]byte("hello world 2"),
		[]byte("hello world 3"),
	})
	require.NoError(t, w.WriteBatch(tCtx, inMsg))

	assert.Equal(t, []inEntries{
		{
			{id: "0", content: "hello world 1"},
			{id: "1", content: "hello world 2"},
			{id: "2", content: "hello world 3"},
		},
		{
			{id: "1", content: "hello world 2"},
		},
	}, in)
}

func TestSQSSendLimit(t *testing.T) {
	tCtx := context.Background()

	conf := output.NewAmazonSQSConfig()
	w, err := newSQSWriter(conf, mock.NewManager())
	require.NoError(t, err)

	var in []inEntries
	var out []*sqs.SendMessageBatchOutput
	w.sqs = &mockSqs{
		fn: func(smbi *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
			var e inEntries
			for _, entry := range smbi.Entries {
				e = append(e, inMsg{
					id:      *entry.Id,
					content: *entry.MessageBody,
				})
			}
			in = append(in, e)

			if len(out) == 0 {
				return nil, errors.New("ran out of mock outputs")
			}
			outBatch := out[0]
			out = out[1:]
			return outBatch, nil
		},
	}

	out = []*sqs.SendMessageBatchOutput{
		{}, {},
	}

	inMsg := message.QuickBatch(nil)
	for i := 0; i < 15; i++ {
		inMsg = append(inMsg, message.NewPart([]byte(fmt.Sprintf("hello world %v", i+1))))
	}
	require.NoError(t, w.WriteBatch(tCtx, inMsg))

	assert.Equal(t, []inEntries{
		{
			{id: "0", content: "hello world 1"},
			{id: "1", content: "hello world 2"},
			{id: "2", content: "hello world 3"},
			{id: "3", content: "hello world 4"},
			{id: "4", content: "hello world 5"},
			{id: "5", content: "hello world 6"},
			{id: "6", content: "hello world 7"},
			{id: "7", content: "hello world 8"},
			{id: "8", content: "hello world 9"},
			{id: "9", content: "hello world 10"},
		},
		{
			{id: "10", content: "hello world 11"},
			{id: "11", content: "hello world 12"},
			{id: "12", content: "hello world 13"},
			{id: "13", content: "hello world 14"},
			{id: "14", content: "hello world 15"},
		},
	}, in)
}
