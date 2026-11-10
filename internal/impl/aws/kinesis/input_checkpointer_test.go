// Copyright 2026 Redpanda Data, Inc.
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

package kinesis

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scriptedDynamoTransport answers each DynamoDB operation with a canned JSON
// body keyed by the operation name of the X-Amz-Target header, so the SDK
// deserializes it exactly as it would a real response.
type scriptedDynamoTransport struct {
	responses map[string]string
	calls     map[string]*atomic.Int64
}

func newScriptedDynamoTransport(responses map[string]string) *scriptedDynamoTransport {
	s := &scriptedDynamoTransport{responses: responses, calls: map[string]*atomic.Int64{}}
	for op := range responses {
		s.calls[op] = &atomic.Int64{}
	}
	return s
}

func (s *scriptedDynamoTransport) Do(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")
	op := target[strings.LastIndex(target, ".")+1:]
	body, ok := s.responses[op]
	if !ok {
		return nil, fmt.Errorf("scriptedDynamoTransport: unexpected operation %q", target)
	}
	s.calls[op].Add(1)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    req,
	}, nil
}

func newScriptedCheckpointer(transport aws.HTTPClient) *awsKinesisCheckpointer {
	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: aws.AnonymousCredentials{},
	}
	return &awsKinesisCheckpointer{
		conf:             kiddbConfig{Table: "test-table"},
		clientID:         "thief",
		leaseDuration:    time.Minute,
		commitPeriod:     time.Millisecond,
		stealGracePeriod: time.Millisecond,
		svc: dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
			o.HTTPClient = transport
			o.Retryer = aws.NopRetryer{}
		}),
	}
}

// A claim is written to the table before the first commit, so a client that
// is killed before its first commit period leaves an item with a client ID and
// lease but no SequenceNumber. Stealing that shard must succeed with an empty
// starting sequence (the consumer then starts from the configured position):
// the UpdateItem of the steal has already transferred the lease, so failing
// here leaves the shard owned by this client but never consumed.
func TestCheckpointerClaimStealsShardWithoutCommittedSequence(t *testing.T) {
	// A lease that expired long ago makes the steal grace wait a no-op.
	staleLease := time.Now().Add(-time.Hour).Format(time.RFC3339Nano)
	item := func(seq string) string {
		s := `"StreamID":{"S":"stream-1"},"ShardID":{"S":"shard-0"},"ClientID":{"S":"thief"},"LeaseTimeout":{"S":"` + staleLease + `"}`
		if seq != "" {
			s += `,"SequenceNumber":{"S":"` + seq + `"}`
		}
		return "{" + s + "}"
	}

	for _, tc := range []struct {
		name         string
		updateOld    string // attributes returned by the steal's UpdateItem
		getItem      string // item returned by the post-grace GetItem
		wantSequence string
	}{
		{
			name:      "victim never committed",
			updateOld: `{"Attributes":{"ClientID":{"S":"victim"},"LeaseTimeout":{"S":"` + staleLease + `"}}}`,
			getItem:   `{"Item":` + item("") + `}`,
		},
		{
			name:         "victim committed before the steal",
			updateOld:    `{"Attributes":{"ClientID":{"S":"victim"},"LeaseTimeout":{"S":"` + staleLease + `"},"SequenceNumber":{"S":"10"}}}`,
			getItem:      `{"Item":` + item("10") + `}`,
			wantSequence: "10",
		},
		{
			name:         "victim yielded a later sequence during the grace period",
			updateOld:    `{"Attributes":{"ClientID":{"S":"victim"},"LeaseTimeout":{"S":"` + staleLease + `"},"SequenceNumber":{"S":"10"}}}`,
			getItem:      `{"Item":` + item("12") + `}`,
			wantSequence: "12",
		},
		{
			name:         "victim removed the checkpoint during the grace period",
			updateOld:    `{"Attributes":{"ClientID":{"S":"victim"},"LeaseTimeout":{"S":"` + staleLease + `"},"SequenceNumber":{"S":"10"}}}`,
			getItem:      `{}`,
			wantSequence: "10",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			transport := newScriptedDynamoTransport(map[string]string{
				"UpdateItem": tc.updateOld,
				"GetItem":    tc.getItem,
			})
			cp := newScriptedCheckpointer(transport)

			seq, err := cp.Claim(t.Context(), "stream-1", "shard-0", "victim")
			require.NoError(t, err)
			assert.Equal(t, tc.wantSequence, seq)
			assert.Equal(t, int64(1), transport.calls["UpdateItem"].Load())
			assert.Equal(t, int64(1), transport.calls["GetItem"].Load(), "a steal re-reads the checkpoint after the grace period")
		})
	}
}

// getCheckpoint reports a missing item as nil rather than an error, and an
// item written by a claim that never committed as a checkpoint with no
// position.
func TestCheckpointerGetCheckpointWithoutSequence(t *testing.T) {
	lease := time.Now().Truncate(time.Millisecond).UTC()

	t.Run("missing item", func(t *testing.T) {
		cp := newScriptedCheckpointer(newScriptedDynamoTransport(map[string]string{"GetItem": `{}`}))
		got, err := cp.getCheckpoint(t.Context(), "stream-1", "shard-0")
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("claimed but never committed", func(t *testing.T) {
		cp := newScriptedCheckpointer(newScriptedDynamoTransport(map[string]string{
			"GetItem": `{"Item":{"StreamID":{"S":"stream-1"},"ShardID":{"S":"shard-0"},"ClientID":{"S":"victim"},"LeaseTimeout":{"S":"` + lease.Format(time.RFC3339Nano) + `"}}}`,
		}))
		got, err := cp.getCheckpoint(t.Context(), "stream-1", "shard-0")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Empty(t, got.SequenceNumber)
		require.NotNil(t, got.ClientID)
		assert.Equal(t, "victim", *got.ClientID)
		require.NotNil(t, got.LeaseTimeout)
		assert.True(t, lease.Equal(*got.LeaseTimeout))
	})
}
