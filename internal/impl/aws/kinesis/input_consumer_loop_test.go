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
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/cenkalti/backoff/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// stubDynamoTransport answers every DynamoDB call with an empty success and
// records the time of each PutItem (the checkpoint write).
type stubDynamoTransport struct {
	mu       sync.Mutex
	putTimes []time.Time
}

func (s *stubDynamoTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.HasSuffix(req.Header.Get("X-Amz-Target"), ".PutItem") {
		s.mu.Lock()
		s.putTimes = append(s.putTimes, time.Now())
		s.mu.Unlock()
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(bytes.NewReader([]byte(`{}`))),
	}, nil
}

func (s *stubDynamoTransport) putCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.putTimes)
}

// waitingStubSource waits fetchWait per Fetch and returns no records, like an
// idle enhanced fan-out shard.
type waitingStubSource struct {
	fetchWait time.Duration

	mu      sync.Mutex
	fetches int
}

func (s *waitingStubSource) Fetch(ctx context.Context) ([]types.Record, bool, error) {
	s.mu.Lock()
	s.fetches++
	s.mu.Unlock()
	select {
	case <-time.After(s.fetchWait):
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
	return nil, false, nil
}

func (*waitingStubSource) WaitsForData() bool { return true }
func (*waitingStubSource) Close()             {}

func (s *waitingStubSource) fetchCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fetches
}

func newLoopTestReader(t *testing.T, commitPeriod time.Duration, source shardRecordSource, transport *stubDynamoTransport) *kinesisReader {
	t.Helper()

	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("id", "secret", ""),
		HTTPClient:  &http.Client{Transport: transport},
	}

	k := &kinesisReader{
		conf:         kiConfig{CheckpointLimit: 1024},
		batcher:      service.BatchPolicy{Count: 1},
		log:          service.MockResources().Logger(),
		mgr:          service.MockResources(),
		commitPeriod: commitPeriod,
		msgChan:      make(chan asyncMessage),
		closedChan:   make(chan struct{}),
		checkpointer: &awsKinesisCheckpointer{
			conf:             kiddbConfig{Table: "test-table"},
			clientID:         "loop-test-client",
			leaseDuration:    time.Minute,
			commitPeriod:     commitPeriod,
			stealGracePeriod: commitPeriod,
			svc:              dynamodb.NewFromConfig(cfg),
		},
	}
	k.ctx, k.done = context.WithCancel(context.Background())
	t.Cleanup(k.done)
	k.boffPool = sync.Pool{
		New: func() any {
			boff := backoff.NewExponentialBackOff()
			boff.InitialInterval = time.Millisecond * 300
			boff.MaxInterval = time.Second * 5
			boff.MaxElapsedTime = 0
			return boff
		},
	}
	k.newSource = func(streamInfo, string, string, func() string) (shardRecordSource, error) {
		return source, nil
	}
	return k
}

// Close must not hang when Connect never succeeded: without runners nothing
// closes closedChan, and benthos calls Close with a background context on the
// way out, so the input would stall shutdown until the stream timeout.
func TestReaderCloseWithoutConnectReturns(t *testing.T) {
	transport := &stubDynamoTransport{}
	k := newLoopTestReader(t, time.Second, &waitingStubSource{fetchWait: time.Millisecond}, transport)
	k.msgChan = nil // Connect never succeeded.

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	require.NoError(t, k.Close(ctx), "Close must return promptly when no shard runners were ever started")
}

// An overdue commit must be serviced before the loop re-enters a waited Fetch.
// The pull case's channel is always ready after an empty waited fetch, so
// without a priority check the select picks it over the expired commit timer
// with probability one half per iteration, making commit lateness geometric:
// each loss costs another full fetch wait, and at defaults a quarter of
// commits land later than steal_grace_period, opening the shard to steals of
// stale sequences. With the commit period below the fetch wait, the commit
// timer is expired on every loop pass, so servicing it first means roughly one
// checkpoint per fetch; the racing select manages only about half that.
func TestConsumerLoopServicesOverdueCommitsBeforeFetching(t *testing.T) {
	transport := &stubDynamoTransport{}
	source := &waitingStubSource{fetchWait: 50 * time.Millisecond}
	k := newLoopTestReader(t, 30*time.Millisecond, source, transport)

	var wg sync.WaitGroup
	wg.Add(1)
	require.NoError(t, k.runConsumer(&wg, streamInfo{id: "stream-1"}, "shard-0", ""))

	deadline := time.Now().Add(5 * time.Second)
	for source.fetchCount() < 20 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	k.done()
	wg.Wait()

	fetches, commits := source.fetchCount(), transport.putCount()
	require.GreaterOrEqual(t, fetches, 20, "loop never got going")
	assert.GreaterOrEqual(t, commits, (fetches*8)/10,
		"an expired commit timer must win against the always-ready pull case: got %v commits for %v fetches", commits, fetches)
}
