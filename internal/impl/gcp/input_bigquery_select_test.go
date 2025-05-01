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
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

var testBQInputYAML = `
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
  root = [ 3 ]
`

func TestGCPBigQuerySelectInput(t *testing.T) {
	spec := newBigQuerySelectInputConfig()

	parsed, err := spec.ParseYAML(testBQInputYAML, nil)
	require.NoError(t, err)

	inp, err := newBigQuerySelectInput(parsed, nil)
	require.NoError(t, err)

	mockClient := &mockBQClient{}
	inp.client = mockClient

	iter := &mockBQIterator{
		rows: []string{
			`{"total_count":25568,"word":"the"}`,
			`{"total_count":19649,"word":"and"}`,
			`{"total_count":12527,"word":"you"}`,
			`{"total_count":8561,"word":"that"}`,
			`{"total_count":8395,"word":"not"}`,
			`{"total_count":7780,"word":"And"}`,
			`{"total_count":7224,"word":"with"}`,
			`{"total_count":6811,"word":"his"}`,
			`{"total_count":6244,"word":"your"}`,
			`{"total_count":6154,"word":"for"}`,
		},
	}

	mockClient.On("RunQuery", mock.Anything, mock.Anything).Return(iter, nil)

	err = inp.Connect(t.Context())
	require.NoError(t, err)

	i := 0
	for {
		msg, ack, err := inp.Read(t.Context())
		if i >= len(iter.rows) {
			require.ErrorIs(t, err, service.ErrEndOfInput)
			break
		}

		require.NoError(t, err)
		require.NoError(t, ack(t.Context(), nil))

		bs, err := msg.AsBytes()
		require.NoError(t, err)

		require.Equal(t, iter.rows[i], string(bs))

		i++
	}

	mockClient.AssertExpectations(t)
}

func TestGCPBigQuerySelectInput_NotConnected(t *testing.T) {
	spec := newBigQuerySelectInputConfig()

	parsed, err := spec.ParseYAML(testBQInputYAML, nil)
	require.NoError(t, err)

	inp, err := newBigQuerySelectInput(parsed, nil)
	require.NoError(t, err)

	msg, ack, err := inp.Read(t.Context())
	require.ErrorIs(t, err, service.ErrNotConnected)
	require.Nil(t, msg)
	require.Nil(t, ack)
}

func TestGCPBigQuerySelectInput_IteratorError(t *testing.T) {
	spec := newBigQuerySelectInputConfig()

	parsed, err := spec.ParseYAML(testBQInputYAML, nil)
	require.NoError(t, err)

	inp, err := newBigQuerySelectInput(parsed, nil)
	require.NoError(t, err)

	mockClient := &mockBQClient{}
	inp.client = mockClient

	testErr := errors.New("simulated error")
	iter := &mockBQIterator{
		rows: []string{`{"total_count":25568,"word":"the"}`},
		err:  testErr,
	}

	mockClient.On("RunQuery", mock.Anything, mock.Anything).Return(iter, nil)

	err = inp.Connect(t.Context())
	require.NoError(t, err)

	msg, ack, err := inp.Read(t.Context())
	require.ErrorIs(t, err, testErr)
	require.Nil(t, msg)
	require.Nil(t, ack)
}

func TestGCPBigQuerySelectInput_Connect(t *testing.T) {
	spec := newBigQuerySelectInputConfig()

	parsed, err := spec.ParseYAML(testBQInputYAML, nil)
	require.NoError(t, err)

	inp, err := newBigQuerySelectInput(parsed, nil)
	require.NoError(t, err)

	mockClient := &mockBQClient{}
	mockClient.On("RunQuery", mock.Anything, mock.Anything).Return(&mockBQIterator{}, nil)
	inp.client = mockClient

	err = inp.Connect(t.Context())
	require.NoError(t, err)

	err = inp.Close(t.Context())
	require.NoError(t, err)

	mockClient.AssertExpectations(t)
}

func TestGCPBigQuerySelectInput_ConnectError(t *testing.T) {
	spec := newBigQuerySelectInputConfig()

	parsed, err := spec.ParseYAML(testBQInputYAML, nil)
	require.NoError(t, err)

	inp, err := newBigQuerySelectInput(parsed, nil)
	require.NoError(t, err)

	testErr := errors.New("test error")
	mockClient := &mockBQClient{}
	mockClient.On("RunQuery", mock.Anything, mock.Anything).Return(nil, testErr)
	inp.client = mockClient

	err = inp.Connect(t.Context())
	require.ErrorIs(t, err, testErr)

	mockClient.AssertExpectations(t)
}
