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

package turbopuffer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tpuf "github.com/turbopuffer/turbopuffer-go/v2"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type capturedRequest struct {
	method        string
	path          string
	authorization string
	body          map[string]any
}

type namespaceFailingClient struct {
	failedNamespace string
	err             error
}

type failFirstClient struct {
	writeCalls int
	err        error
}

func (*failFirstClient) CheckConnection(context.Context) error {
	return nil
}

func (c *failFirstClient) Write(context.Context, string, tpuf.NamespaceWriteParams) error {
	c.writeCalls++
	if c.writeCalls == 1 {
		return c.err
	}
	return nil
}

func (*namespaceFailingClient) CheckConnection(context.Context) error {
	return nil
}

func (c *namespaceFailingClient) Write(_ context.Context, namespace string, _ tpuf.NamespaceWriteParams) error {
	if namespace == c.failedNamespace {
		return c.err
	}
	return nil
}

func newTestServer(t *testing.T) (*httptest.Server, <-chan capturedRequest) {
	t.Helper()

	requests := make(chan capturedRequest, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if r.Method == http.MethodPost {
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		}
		requests <- capturedRequest{
			method:        r.Method,
			path:          r.URL.Path,
			authorization: r.Header.Get("Authorization"),
			body:          body,
		}

		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte(`{"namespaces":[],"next_cursor":""}`))
			return
		}
		_, _ = w.Write([]byte(`{"rows_affected":1}`))
	}))
	t.Cleanup(server.Close)
	return server, requests
}

func newTestWriter(t *testing.T, baseURL string) *outputWriter {
	t.Helper()

	conf, err := outputSpec().ParseYAML(fmt.Sprintf(`
api_key: tpuf_test
region: gcp-us-central1
base_url: %s
max_retries: 0
namespace: '${! meta("namespace") }'
operation: '${! meta("operation") }'
id: root = json("id")
document_mapping: root = this.document
schema:
  text:
    type: string
    full_text_search: true
distance_metric: cosine_distance
`, baseURL), service.NewEnvironment())
	require.NoError(t, err)

	w, err := newOutputWriter(conf, service.MockResources())
	require.NoError(t, err)
	return w
}

func newMessage(id any, namespace, operation string, document map[string]any) *service.Message {
	msg := service.NewMessage(nil)
	msg.SetStructuredMut(map[string]any{
		"id":       id,
		"document": document,
	})
	msg.MetaSetMut("namespace", namespace)
	msg.MetaSetMut("operation", operation)
	return msg
}

func TestOutputConnectChecksCredentials(t *testing.T) {
	server, requests := newTestServer(t)
	w := newTestWriter(t, server.URL+"/REGION")

	err := w.Connect(t.Context())
	require.NoErrorf(t, err, "%T: %#v", err, err)

	req := <-requests
	assert.Equal(t, http.MethodGet, req.method)
	assert.Equal(t, "/gcp-us-central1/v1/namespaces", req.path)
	assert.Equal(t, "Bearer tpuf_test", req.authorization)
}

func TestOutputWritesMixedOperationsInOneRequest(t *testing.T) {
	server, requests := newTestServer(t)
	w := newTestWriter(t, server.URL+"/REGION")

	batch := service.MessageBatch{
		newMessage("page-1", "wikipedia", "upsert", map[string]any{"text": "first"}),
		newMessage(int64(2), "wikipedia", "patch", map[string]any{"text": "second"}),
		newMessage("page-3", "wikipedia", "delete", nil),
	}
	err := w.WriteBatch(t.Context(), batch)
	require.NoErrorf(t, err, "%T: %#v", err, err)

	req := <-requests
	assert.Equal(t, http.MethodPost, req.method)
	assert.Equal(t, "/gcp-us-central1/v2/namespaces/wikipedia", req.path)
	assert.Equal(t, "Bearer tpuf_test", req.authorization)
	assert.Equal(t, []any{"page-3"}, req.body["deletes"])
	assert.Equal(t, "cosine_distance", req.body["distance_metric"])
	assert.Empty(t, req.body["schema"].(map[string]any)["text"].(map[string]any)["full_text_search"])

	upserts := req.body["upsert_rows"].([]any)
	require.Len(t, upserts, 1)
	assert.Equal(t, "page-1", upserts[0].(map[string]any)["id"])
	assert.Equal(t, "first", upserts[0].(map[string]any)["text"])

	patches := req.body["patch_rows"].([]any)
	require.Len(t, patches, 1)
	assert.Equal(t, float64(2), patches[0].(map[string]any)["id"])
	assert.Equal(t, "second", patches[0].(map[string]any)["text"])
}

func TestOutputGroupsWritesByNamespace(t *testing.T) {
	server, requests := newTestServer(t)
	w := newTestWriter(t, server.URL+"/REGION")

	batch := service.MessageBatch{
		newMessage("1", "one", "upsert", map[string]any{"text": "first"}),
		newMessage("2", "two", "upsert", map[string]any{"text": "second"}),
	}
	require.NoError(t, w.WriteBatch(t.Context(), batch))

	paths := []string{(<-requests).path, (<-requests).path}
	assert.ElementsMatch(t, []string{"/gcp-us-central1/v2/namespaces/one", "/gcp-us-central1/v2/namespaces/two"}, paths)
}

func TestOutputMappingsUseMessageContentAndMetadata(t *testing.T) {
	server, requests := newTestServer(t)
	conf, err := outputSpec().ParseYAML(fmt.Sprintf(`
api_key: tpuf_test
region: gcp-us-central1
base_url: %s/REGION
max_retries: 0
namespace: '${! meta("namespace") }'
operation: upsert
id: root = meta("id")
document_mapping: |
  root.text = content().string()
  root.source = meta("source")
`, server.URL), service.NewEnvironment())
	require.NoError(t, err)
	w, err := newOutputWriter(conf, service.MockResources())
	require.NoError(t, err)

	msg := service.NewMessage([]byte("hello wikipedia"))
	msg.MetaSetMut("namespace", "wikipedia")
	msg.MetaSetMut("id", "page-1")
	msg.MetaSetMut("source", "kafka")
	require.NoError(t, w.WriteBatch(t.Context(), service.MessageBatch{msg}))

	req := <-requests
	row := req.body["upsert_rows"].([]any)[0].(map[string]any)
	assert.Equal(t, "page-1", row["id"])
	assert.Equal(t, "hello wikipedia", row["text"])
	assert.Equal(t, "kafka", row["source"])
}

func TestOutputSplitsDuplicateIDsIntoOrderedRequests(t *testing.T) {
	server, requests := newTestServer(t)
	w := newTestWriter(t, server.URL+"/REGION")

	batch := service.MessageBatch{
		newMessage("page-1", "wikipedia", "upsert", map[string]any{"revision": 1}),
		newMessage("page-1", "wikipedia", "upsert", map[string]any{"revision": 2}),
	}
	require.NoError(t, w.WriteBatch(t.Context(), batch))

	first := <-requests
	second := <-requests
	assert.Equal(t, float64(1), first.body["upsert_rows"].([]any)[0].(map[string]any)["revision"])
	assert.Equal(t, float64(2), second.body["upsert_rows"].([]any)[0].(map[string]any)["revision"])
}

func TestOutputRejectsInvalidOperationBeforeWriting(t *testing.T) {
	server, requests := newTestServer(t)
	w := newTestWriter(t, server.URL+"/REGION")

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		newMessage("page-1", "wikipedia", "merge", map[string]any{"text": "first"}),
	})
	require.ErrorContains(t, err, "operation")
	assert.Empty(t, requests)
}

func TestOutputRejectsInvalidIDBeforeWriting(t *testing.T) {
	server, requests := newTestServer(t)
	w := newTestWriter(t, server.URL+"/REGION")

	err := w.WriteBatch(t.Context(), service.MessageBatch{
		newMessage(-1, "wikipedia", "upsert", map[string]any{"text": "first"}),
	})
	require.ErrorContains(t, err, "id")
	assert.Empty(t, requests)
}

func TestOutputConfigRequiresRegion(t *testing.T) {
	_, err := outputSpec().ParseYAML(`
api_key: tpuf_test
namespace: wikipedia
id: root = this.id
`, service.NewEnvironment())
	require.ErrorContains(t, err, "region")
}

func TestOutputConfigValidatesCustomBaseURL(t *testing.T) {
	conf, err := outputSpec().ParseYAML(`
api_key: tpuf_test
region: gcp-us-central1
base_url: http://localhost:8080
namespace: wikipedia
id: root = this.id
`, service.NewEnvironment())
	require.NoError(t, err)
	_, err = newOutputWriter(conf, service.MockResources())
	require.ErrorContains(t, err, "REGION")
}

func TestOutputConfigRejectsNegativeMaxRetries(t *testing.T) {
	conf, err := outputSpec().ParseYAML(`
api_key: tpuf_test
region: gcp-us-central1
max_retries: -1
namespace: wikipedia
id: root = json("id")
`, service.NewEnvironment())
	require.NoError(t, err)
	_, err = newOutputWriter(conf, service.MockResources())
	require.ErrorContains(t, err, "max_retries must not be negative")
}

func TestOutputReturnsPerMessageErrors(t *testing.T) {
	w := newTestWriter(t, "http://localhost/REGION")
	writeErr := errors.New("write failed")
	w.client = &namespaceFailingClient{
		failedNamespace: "bad",
		err:             writeErr,
	}

	batch := service.MessageBatch{
		newMessage("1", "good", "upsert", map[string]any{"text": "first"}),
		newMessage("2", "bad", "upsert", map[string]any{"text": "second"}),
		newMessage("3", "good", "upsert", map[string]any{"text": "third"}),
	}
	index := batch.Index()
	err := w.WriteBatch(t.Context(), batch)

	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)
	assert.Equal(t, 1, batchErr.IndexedErrors())
	var failed []int
	batchErr.WalkMessagesIndexedBy(index, func(i int, _ *service.Message, err error) bool {
		if err != nil {
			failed = append(failed, i)
			assert.ErrorIs(t, err, writeErr)
		}
		return true
	})
	assert.Equal(t, []int{1}, failed)
}

func TestOutputDoesNotWriteLaterDuplicateAfterFailure(t *testing.T) {
	w := newTestWriter(t, "http://localhost/REGION")
	writeErr := errors.New("write failed")
	client := &failFirstClient{err: writeErr}
	w.client = client

	batch := service.MessageBatch{
		newMessage("page-1", "wikipedia", "upsert", map[string]any{"revision": 1}),
		newMessage("page-1", "wikipedia", "upsert", map[string]any{"revision": 2}),
	}
	err := w.WriteBatch(t.Context(), batch)

	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)
	assert.Equal(t, 2, batchErr.IndexedErrors())
	assert.Equal(t, 1, client.writeCalls)
}

func TestNormalizeID(t *testing.T) {
	testCases := []struct {
		name    string
		input   any
		want    any
		wantErr string
	}{
		{name: "string", input: "page-1", want: "page-1"},
		{name: "integer", input: int64(42), want: uint64(42)},
		{name: "JSON integer", input: json.Number("42"), want: uint64(42)},
		{name: "maximum unsigned integer", input: json.Number("18446744073709551615"), want: uint64(18446744073709551615)},
		{name: "negative", input: int64(-1), wantErr: "negative"},
		{name: "fraction", input: json.Number("1.5"), wantErr: "unsigned 64-bit"},
		{name: "overflow", input: json.Number("18446744073709551616"), wantErr: "unsigned 64-bit"},
		{name: "long string", input: string(make([]byte, 65)), wantErr: "64 bytes"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, _, err := normalizeID(tc.input)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
