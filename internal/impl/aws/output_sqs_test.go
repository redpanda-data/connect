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

package aws

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
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
		if act, exp := isValidSQSAttribute(test.k), test.expected; act != exp {
			t.Errorf("Unexpected result for test '%v': %v != %v", i, act, exp)
		}
	}
}

type mockSqs struct {
	sqsAPI
	fn func(*sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error)
}

func (m *mockSqs) SendMessageBatch(_ context.Context, input *sqs.SendMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	return m.fn(input)
}

type inMsg struct {
	id      string
	content string
}
type inEntries []inMsg

func TestSQSRetries(t *testing.T) {
	tCtx := t.Context()

	conf, err := config.LoadDefaultConfig(t.Context(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
	)
	require.NoError(t, err)
	url, err := service.NewInterpolatedString("http://foo.example.com")
	require.NoError(t, err)
	w, err := newSQSWriter(sqsoConfig{
		URL: url,
		backoffCtor: func() backoff.BackOff {
			return backoff.NewExponentialBackOff()
		},
		aconf:           conf,
		MaxRecordsCount: 10,
	}, service.MockResources())
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
			Failed: []types.BatchResultErrorEntry{
				{
					Code:        aws.String("xx"),
					Id:          aws.String("1"),
					Message:     aws.String("test error"),
					SenderFault: false,
				},
			},
		},
		{},
	}

	require.NoError(t, w.WriteBatch(tCtx, service.MessageBatch{
		service.NewMessage([]byte("hello world 1")),
		service.NewMessage([]byte("hello world 2")),
		service.NewMessage([]byte("hello world 3")),
	}))

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
	tCtx := t.Context()

	conf, err := config.LoadDefaultConfig(t.Context(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
	)
	require.NoError(t, err)

	url, err := service.NewInterpolatedString("http://foo.example.com")
	require.NoError(t, err)
	w, err := newSQSWriter(sqsoConfig{
		URL: url,
		backoffCtor: func() backoff.BackOff {
			return backoff.NewExponentialBackOff()
		},
		aconf:           conf,
		MaxRecordsCount: 10,
	}, service.MockResources())
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

	inMsg := service.MessageBatch{}
	for i := range 15 {
		inMsg = append(inMsg, service.NewMessage(fmt.Appendf(nil, "hello world %v", i+1)))
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

func TestSQSMultipleQueues(t *testing.T) {
	tCtx := t.Context()

	conf, err := config.LoadDefaultConfig(t.Context(),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
	)
	require.NoError(t, err)

	url, err := service.NewInterpolatedString("http://${!counter()%2}.example.com")
	require.NoError(t, err)
	w, err := newSQSWriter(sqsoConfig{
		URL: url,
		backoffCtor: func() backoff.BackOff {
			return backoff.NewExponentialBackOff()
		},
		aconf:           conf,
		MaxRecordsCount: 10,
	}, service.MockResources())
	require.NoError(t, err)

	in := map[string][]inEntries{}
	sendCalls := 0
	w.sqs = &mockSqs{
		fn: func(smbi *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
			var e inEntries
			for _, entry := range smbi.Entries {
				e = append(e, inMsg{
					id:      *entry.Id,
					content: *entry.MessageBody,
				})
			}
			if smbi.QueueUrl == nil {
				return nil, errors.New("nil queue URL")
			}
			in[*smbi.QueueUrl] = append(in[*smbi.QueueUrl], e)
			sendCalls++
			return &sqs.SendMessageBatchOutput{}, nil
		},
	}

	inMsg := service.MessageBatch{}
	for i := range 30 {
		inMsg = append(inMsg, service.NewMessage(fmt.Appendf(nil, "hello world %v", i+1)))
	}
	require.NoError(t, w.WriteBatch(tCtx, inMsg))

	assert.Equal(t, map[string][]inEntries{
		"http://0.example.com": {
			{
				{id: "1", content: "hello world 2"},
				{id: "3", content: "hello world 4"},
				{id: "5", content: "hello world 6"},
				{id: "7", content: "hello world 8"},
				{id: "9", content: "hello world 10"},
				{id: "11", content: "hello world 12"},
				{id: "13", content: "hello world 14"},
				{id: "15", content: "hello world 16"},
				{id: "17", content: "hello world 18"},
				{id: "19", content: "hello world 20"},
			},
			{
				{id: "21", content: "hello world 22"},
				{id: "23", content: "hello world 24"},
				{id: "25", content: "hello world 26"},
				{id: "27", content: "hello world 28"},
				{id: "29", content: "hello world 30"},
			},
		},
		"http://1.example.com": {
			{
				{id: "0", content: "hello world 1"},
				{id: "2", content: "hello world 3"},
				{id: "4", content: "hello world 5"},
				{id: "6", content: "hello world 7"},
				{id: "8", content: "hello world 9"},
				{id: "10", content: "hello world 11"},
				{id: "12", content: "hello world 13"},
				{id: "14", content: "hello world 15"},
				{id: "16", content: "hello world 17"},
				{id: "18", content: "hello world 19"},
			},
			{
				{id: "20", content: "hello world 21"},
				{id: "22", content: "hello world 23"},
				{id: "24", content: "hello world 25"},
				{id: "26", content: "hello world 27"},
				{id: "28", content: "hello world 29"},
			},
		},
	}, in)
}
