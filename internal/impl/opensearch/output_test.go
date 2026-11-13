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

package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestOutputCompressRequestBody_DefaultsFalse(t *testing.T) {
	spec := OutputSpec()
	pConf, err := spec.ParseYAML(`
urls: [ "http://localhost:9200" ]
index: foo
id: '${!uuid_v4()}'
action: index
`, nil)
	require.NoError(t, err)

	conf, err := esoConfigFromParsed(pConf)
	require.NoError(t, err)
	assert.False(t, conf.clientOpts.Client.CompressRequestBody,
		"compress_request_body should default to false to preserve existing behaviour")
}

func TestOutputCompressRequestBody_SetTrue(t *testing.T) {
	spec := OutputSpec()
	pConf, err := spec.ParseYAML(`
urls: [ "http://localhost:9200" ]
index: foo
id: '${!uuid_v4()}'
action: index
compress_request_body: true
`, nil)
	require.NoError(t, err)

	conf, err := esoConfigFromParsed(pConf)
	require.NoError(t, err)
	assert.True(t, conf.clientOpts.Client.CompressRequestBody,
		"compress_request_body: true should propagate to the OpenSearch client Config")
}

func TestOutputPerMessageErrorsConfig(t *testing.T) {
	tests := []struct {
		name string
		conf string
		want bool
	}{
		{
			name: "defaults to false",
			want: false,
		},
		{
			name: "set to true",
			conf: "per_message_errors: true",
			want: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pConf, err := OutputSpec().ParseYAML(`
urls: [ "http://localhost:9200" ]
index: foo
id: '${!uuid_v4()}'
action: index
`+test.conf, nil)
			require.NoError(t, err)

			conf, err := esoConfigFromParsed(pConf)
			require.NoError(t, err)
			assert.Equal(t, test.want, conf.perMessageErrors)
		})
	}
}

// Bodies that the fake _bulk endpoint rejects a whole request with.
const (
	plainTooManyRequests = "429 Too Many Requests /_bulk"
	jsonCircuitBreaker   = `{"error":{"root_cause":[{"type":"circuit_breaking_exception","reason":"[parent] Data too large"}],"type":"circuit_breaking_exception","reason":"[parent] Data too large"},"status":429}`
)

// Text that the error of a failed message contains, for each kind of failure.
const (
	wantRequestRejected = "json unmarshal body"
	wantCircuitBreaker  = "circuit_breaking_exception"
	wantItemRejected    = "mapper_parsing_exception: failed to parse"
	wantCanceled        = "context canceled"
	// Requests rejected with a plain-text 429 and then by the circuit
	// breaker, whose error is the last.
	wantDifferentErrors = "bulk requests failed with 2 different errors, last: flush: status: 429, type: " + wantCircuitBreaker
)

// abandonTimeout bounds how long the fake waits for the client to give up on
// a request that it leaves unanswered.
const abandonTimeout = 10 * time.Second

// fakeBulk is an OpenSearch _bulk endpoint. It rejects a whole request that
// carries a document in rejectRequests with a plain-text 429, and one that
// carries a document in breakRequests with a JSON circuit breaker 429. It
// rejects the items of the documents in rejectItems, and cuts the items of a
// response short before the first document in omitResults. The request
// numbered cancelAt, counting from one, calls cancel and gets no answer.
type fakeBulk struct {
	t testing.TB

	rejectRequests map[string]bool
	breakRequests  map[string]bool
	rejectItems    map[string]bool
	omitResults    map[string]bool
	cancelAt       int
	cancel         context.CancelFunc

	mu       sync.Mutex
	requests int
	rejected []string
	written  []string
}

// start serves the endpoint until the test ends and returns its URL.
func (f *fakeBulk) start(t testing.TB) string {
	t.Helper()
	f.t = t
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return srv.URL
}

func (f *fakeBulk) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ids, err := bulkDocumentIDs(r.Body)
	if err != nil {
		f.t.Errorf("parsing bulk request: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if n := f.countRequest(); n == f.cancelAt {
		f.cancel()
		f.awaitAbandoned(r, n)
		return
	}

	for _, id := range ids {
		switch {
		case f.rejectRequests[id]:
			f.recordRejected(ids)
			writeRejection(w, "text/plain", plainTooManyRequests)
			return
		case f.breakRequests[id]:
			f.recordRejected(ids)
			writeRejection(w, "application/json", jsonCircuitBreaker)
			return
		}
	}

	items := make([]string, 0, len(ids))
	written := make([]string, 0, len(ids))
	itemErrors := false
	for _, id := range ids {
		if f.omitResults[id] {
			break
		}
		if f.rejectItems[id] {
			itemErrors = true
			items = append(items, fmt.Sprintf(`{"index":{"_index":"test","_id":%q,"status":400,"error":{"type":"mapper_parsing_exception","reason":"failed to parse"}}}`, id))
			continue
		}
		written = append(written, id)
		items = append(items, fmt.Sprintf(`{"index":{"_index":"test","_id":%q,"_version":1,"result":"created","status":201}}`, id))
	}
	f.recordWritten(written)
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"took":1,"errors":%t,"items":[%s]}`, itemErrors, strings.Join(items, ","))
}

func writeRejection(w http.ResponseWriter, contentType, body string) {
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusTooManyRequests)
	_, _ = io.WriteString(w, body)
}

// awaitAbandoned leaves a request unanswered until the client gives up on it,
// so that nothing confirms the request.
func (f *fakeBulk) awaitAbandoned(r *http.Request, n int) {
	select {
	case <-r.Context().Done():
	case <-time.After(abandonTimeout):
		f.t.Errorf("request %d still waits for an answer after the cancel", n)
	}
}

func (f *fakeBulk) countRequest() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests++
	return f.requests
}

func (f *fakeBulk) recordRejected(ids []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rejected = append(f.rejected, ids...)
}

func (f *fakeBulk) recordWritten(ids []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.written = append(f.written, ids...)
}

func (f *fakeBulk) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

// rejectedDocuments returns the documents of the requests rejected as a whole.
func (f *fakeBulk) rejectedDocuments() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.rejected)
}

// writtenDocuments returns the documents that the fake reported as created.
func (f *fakeBulk) writtenDocuments() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.written)
}

// bulkDocumentIDs returns the document IDs of a bulk request body. Every item
// in these tests is an index action: a metadata line, then the document.
func bulkDocumentIDs(body io.Reader) ([]string, error) {
	var ids []string
	dec := json.NewDecoder(body)
	for {
		var meta struct {
			Index struct {
				ID string `json:"_id"`
			} `json:"index"`
		}
		err := dec.Decode(&meta)
		if errors.Is(err, io.EOF) {
			return ids, nil
		}
		if err != nil {
			return nil, err
		}
		var doc json.RawMessage
		if err := dec.Decode(&doc); err != nil {
			return nil, err
		}
		ids = append(ids, meta.Index.ID)
	}
}

func docID(i int) string {
	return fmt.Sprintf("doc-%02d", i)
}

func docIDs(indexes ...int) map[string]bool {
	ids := make(map[string]bool, len(indexes))
	for _, i := range indexes {
		ids[docID(i)] = true
	}
	return ids
}

func testBatch(size int) service.MessageBatch {
	batch := make(service.MessageBatch, size)
	for i := range batch {
		batch[i] = service.NewMessage(fmt.Appendf(nil, `{"id":%q,"message":"hello world"}`, docID(i)))
	}
	return batch
}

func newTestOutput(t testing.TB, url string, perMessageErrors bool) *Output {
	t.Helper()

	pConf, err := OutputSpec().ParseYAML(fmt.Sprintf(`
urls: [ %q ]
index: test
id: '${! json("id") }'
action: index
per_message_errors: %t
`, url, perMessageErrors), nil)
	require.NoError(t, err)

	out, err := OutputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Connect(t.Context()))
	t.Cleanup(func() {
		require.NoError(t, out.Close(context.Background()))
	})
	return out
}

// oneRequestPerMessage makes the bulk indexer send every message in a request
// of its own, in order, so that a test decides which messages fail together.
func oneRequestPerMessage(out *Output) {
	out.bulkNumWorkers = 1
	out.bulkFlushBytes = 1
}

// flushOnClose spreads a batch over several workers, whatever the number of
// CPUs. As with real batches, which are smaller than the default flush size,
// the workers keep their messages for Close, which sends one request per
// worker, one after another, with the context of WriteBatch.
func flushOnClose(out *Output) {
	out.bulkNumWorkers = 4
}

// failedMessages returns the error text of every message that err reports as
// failed, by the index the message had when idx was created.
func failedMessages(t *testing.T, idx *service.Indexer, err error) map[int]string {
	t.Helper()

	var bErr *service.BatchError
	require.ErrorAs(t, err, &bErr)

	failed := map[int]string{}
	bErr.WalkMessagesIndexedBy(idx, func(i int, _ *service.Message, err error) bool {
		if err != nil {
			failed[i] = err.Error()
		}
		return true
	})
	return failed
}

// assertFailures checks that exactly the messages in want failed, each with an
// error that contains the text that want gives for it.
func assertFailures(t *testing.T, want, got map[int]string) {
	t.Helper()

	require.Len(t, got, len(want), "failed messages: %v", got)
	for i, text := range want {
		require.Contains(t, got, i)
		assert.Contains(t, got[i], text, "message %d", i)
	}
}

func allFailed(size int, text string) map[int]string {
	failed := make(map[int]string, size)
	for i := range size {
		failed[i] = text
	}
	return failed
}

func TestOutputWriteBatchFailures(t *testing.T) {
	const batchSize = 8

	tests := []struct {
		name             string
		perMessageErrors bool
		rejectRequests   []int
		breakRequests    []int
		rejectItems      []int
		omitResults      []int
		wantFailed       map[int]string
		wantWholeBatch   bool
		wantHeadline     string
	}{
		{
			name: "all messages written",
		},
		{
			name:             "all messages written with per_message_errors",
			perMessageErrors: true,
		},
		{
			name:           "rejected requests fail the whole batch",
			rejectRequests: []int{1, 4},
			wantFailed:     allFailed(batchSize, wantRequestRejected),
			wantWholeBatch: true,
			wantHeadline:   wantRequestRejected,
		},
		{
			name:             "rejected requests fail only their messages",
			perMessageErrors: true,
			rejectRequests:   []int{1, 4},
			wantFailed:       map[int]string{1: wantRequestRejected, 4: wantRequestRejected},
			wantHeadline:     wantRequestRejected,
		},
		{
			name:         "rejected items fail only their messages",
			rejectItems:  []int{6},
			wantFailed:   map[int]string{6: wantItemRejected},
			wantHeadline: wantItemRejected,
		},
		{
			name:             "rejected items fail only their messages with per_message_errors",
			perMessageErrors: true,
			rejectItems:      []int{6},
			wantFailed:       map[int]string{6: wantItemRejected},
			wantHeadline:     wantItemRejected,
		},
		{
			name:           "a rejected request fails the whole batch over rejected items",
			rejectRequests: []int{1, 4},
			rejectItems:    []int{6},
			wantFailed:     allFailed(batchSize, wantRequestRejected),
			wantWholeBatch: true,
			wantHeadline:   wantRequestRejected,
		},
		{
			name:             "rejected requests and items fail only their messages",
			perMessageErrors: true,
			rejectRequests:   []int{1, 4},
			rejectItems:      []int{6},
			wantFailed:       map[int]string{1: wantRequestRejected, 4: wantRequestRejected, 6: wantItemRejected},
			wantHeadline:     wantRequestRejected,
		},
		{
			name:             "a circuit breaker rejection fails only its messages",
			perMessageErrors: true,
			breakRequests:    []int{2},
			wantFailed:       map[int]string{2: wantCircuitBreaker},
			wantHeadline:     wantCircuitBreaker,
		},
		{
			name:           "requests rejected with different errors fail the whole batch with the last",
			rejectRequests: []int{1},
			breakRequests:  []int{2},
			wantFailed:     allFailed(batchSize, wantCircuitBreaker),
			wantWholeBatch: true,
			wantHeadline:   wantCircuitBreaker,
		},
		{
			name:             "requests rejected with different errors fail only their messages",
			perMessageErrors: true,
			rejectRequests:   []int{1},
			breakRequests:    []int{2},
			wantFailed:       map[int]string{1: wantDifferentErrors, 2: wantDifferentErrors},
			wantHeadline:     wantDifferentErrors,
		},
		{
			name:        "messages without a result in the response are acknowledged",
			omitResults: []int{5},
		},
		{
			name:             "messages without a result in the response fail",
			perMessageErrors: true,
			omitResults:      []int{5},
			wantFailed:       map[int]string{5: errNoBulkResult.Error()},
			wantHeadline:     errNoBulkResult.Error(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeBulk{
				rejectRequests: docIDs(test.rejectRequests...),
				breakRequests:  docIDs(test.breakRequests...),
				rejectItems:    docIDs(test.rejectItems...),
				omitResults:    docIDs(test.omitResults...),
			}
			out := newTestOutput(t, fake.start(t), test.perMessageErrors)
			oneRequestPerMessage(out)

			batch := testBatch(batchSize)
			idx := batch.Index()
			err := out.WriteBatch(t.Context(), batch)
			if test.wantFailed == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantHeadline)

			var bErr *service.BatchError
			require.ErrorAs(t, err, &bErr)
			if test.wantWholeBatch {
				assert.Zero(t, bErr.IndexedErrors())
			} else {
				assert.Equal(t, len(test.wantFailed), bErr.IndexedErrors())
			}
			assertFailures(t, test.wantFailed, failedMessages(t, idx, err))
		})
	}
}

// TestOutputWriteBatchPerMessageErrorsOnClose checks the requests that Close
// sends. The indexer decides which messages each worker gets, so the messages
// that must fail are those of the request that the fake rejected, and the
// rejected items.
func TestOutputWriteBatchPerMessageErrorsOnClose(t *testing.T) {
	const batchSize = 40
	rejectItems := []int{10, 20, 30}

	fake := &fakeBulk{
		rejectRequests: docIDs(1),
		rejectItems:    docIDs(rejectItems...),
	}
	out := newTestOutput(t, fake.start(t), true)
	flushOnClose(out)

	batch := testBatch(batchSize)
	idx := batch.Index()
	err := out.WriteBatch(t.Context(), batch)
	require.ErrorContains(t, err, wantRequestRejected)

	rejected := fake.rejectedDocuments()
	require.Contains(t, rejected, docID(1))

	want := map[int]string{}
	for _, i := range rejectItems {
		want[i] = wantItemRejected
	}
	for i := range batchSize {
		if slices.Contains(rejected, docID(i)) {
			want[i] = wantRequestRejected
		}
	}
	assertFailures(t, want, failedMessages(t, idx, err))
}

// TestOutputWriteBatchPerMessageErrorsCanceled cancels the context of
// WriteBatch when Close sends the second request, which fails with every
// request after it. Only the messages that the first request wrote are
// acknowledged.
func TestOutputWriteBatchPerMessageErrorsCanceled(t *testing.T) {
	const batchSize = 40

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	fake := &fakeBulk{cancelAt: 2, cancel: cancel}
	out := newTestOutput(t, fake.start(t), true)
	flushOnClose(out)

	batch := testBatch(batchSize)
	idx := batch.Index()
	err := out.WriteBatch(ctx, batch)
	if fake.requestCount() < 2 {
		require.NoError(t, err)
		t.Skip("the indexer sent the batch in one request, so nothing canceled it")
	}
	require.ErrorContains(t, err, wantCanceled)

	written := fake.writtenDocuments()
	require.NotEmpty(t, written)

	want := map[int]string{}
	for i := range batchSize {
		if !slices.Contains(written, docID(i)) {
			want[i] = wantCanceled
		}
	}
	assertFailures(t, want, failedMessages(t, idx, err))
}

// TestOutputWriteBatchConcurrentFlushes has several workers send requests at
// the same time. Each run of numWorkers consecutive messages shares one
// outcome, so that the workers report the same kind of result at once.
func TestOutputWriteBatchConcurrentFlushes(t *testing.T) {
	const (
		batchSize  = 96
		numWorkers = 4
		// A run of messages is in rejected requests, has rejected items, or
		// is written.
		outcomes = 3
	)

	var rejectRequests, rejectItems []int
	want := map[int]string{}
	for i := range batchSize {
		switch (i / numWorkers) % outcomes {
		case 0:
			rejectRequests = append(rejectRequests, i)
			want[i] = wantRequestRejected
		case 1:
			rejectItems = append(rejectItems, i)
			want[i] = wantItemRejected
		}
	}

	for _, perMessageErrors := range []bool{false, true} {
		t.Run(fmt.Sprintf("per_message_errors=%t", perMessageErrors), func(t *testing.T) {
			fake := &fakeBulk{
				rejectRequests: docIDs(rejectRequests...),
				rejectItems:    docIDs(rejectItems...),
			}
			out := newTestOutput(t, fake.start(t), perMessageErrors)
			out.bulkNumWorkers = numWorkers
			out.bulkFlushBytes = 1

			batch := testBatch(batchSize)
			idx := batch.Index()
			err := out.WriteBatch(t.Context(), batch)
			require.ErrorContains(t, err, wantRequestRejected)

			failed := failedMessages(t, idx, err)
			if !perMessageErrors {
				assert.Len(t, failed, batchSize)
				return
			}
			assertFailures(t, want, failed)
		})
	}
}

// writeBulkSuccess answers a bulk request by reporting every item as created.
func writeBulkSuccess(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Every item is an index action: a metadata line, then the document.
	items := bytes.Count(body, []byte("\n")) / 2
	w.Header().Set("Content-Type", "application/json")
	_, _ = io.WriteString(w, `{"took":1,"errors":false,"items":[`+
		strings.TrimSuffix(strings.Repeat(`{"index":{"status":201}},`, items), ",")+`]}`)
}

// BenchmarkOutputWriteBatch also reports the number of bulk requests that the
// opensearch-go defaults split a batch into, which follows runtime.NumCPU().
func BenchmarkOutputWriteBatch(b *testing.B) {
	const batchSize = 500

	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		writeBulkSuccess(w, r)
	}))
	b.Cleanup(srv.Close)

	batch := make(service.MessageBatch, batchSize)
	for i := range batch {
		batch[i] = service.NewMessage(fmt.Appendf(nil,
			`{"id":%q,"user":"user-%03d","message":%q}`, docID(i), i, strings.Repeat("lorem ipsum ", 18)))
	}

	for _, perMessageErrors := range []bool{false, true} {
		b.Run(fmt.Sprintf("per_message_errors=%t", perMessageErrors), func(b *testing.B) {
			out := newTestOutput(b, srv.URL, perMessageErrors)
			requests.Store(0)

			b.ReportAllocs()
			for b.Loop() {
				if err := out.WriteBatch(b.Context(), batch); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(requests.Load())/float64(b.N), "requests/op")
		})
	}
}
