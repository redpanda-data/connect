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

package azure

import (
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// fakeQueueService is a minimal stand-in for the Azure Queue Storage REST API
// that records the delete and update calls made against dequeued messages.
type fakeQueueService struct {
	mu       sync.Mutex
	messages []string // message IDs served by the next dequeue
	deleted  []string
	updated  map[string]string // message ID -> visibilitytimeout query value
}

func newFakeQueueService(messageIDs ...string) *fakeQueueService {
	return &fakeQueueService{messages: messageIDs, updated: map[string]string{}}
}

func (f *fakeQueueService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	path := strings.TrimPrefix(r.URL.Path, "/devstoreaccount1/")
	switch {
	case r.Method == http.MethodGet && r.URL.Query().Get("comp") == "metadata":
		w.Header().Set("x-ms-approximate-messages-count", fmt.Sprint(len(f.messages)))
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodGet && strings.HasSuffix(path, "/messages"):
		now := time.Now().UTC()
		var sb strings.Builder
		sb.WriteString(`<?xml version="1.0" encoding="utf-8"?><QueueMessagesList>`)
		for _, id := range f.messages {
			fmt.Fprintf(&sb, `<QueueMessage><MessageId>%s</MessageId><InsertionTime>%s</InsertionTime><ExpirationTime>%s</ExpirationTime><PopReceipt>receipt-%s</PopReceipt><TimeNextVisible>%s</TimeNextVisible><DequeueCount>1</DequeueCount><MessageText>body-%s</MessageText></QueueMessage>`,
				id, now.Format(time.RFC1123), now.Add(7*24*time.Hour).Format(time.RFC1123), id, now.Add(30*time.Second).Format(time.RFC1123), id)
		}
		sb.WriteString(`</QueueMessagesList>`)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sb.String()))
	case r.Method == http.MethodDelete && strings.Contains(path, "/messages/"):
		f.deleted = append(f.deleted, path[strings.LastIndex(path, "/")+1:])
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPut && strings.Contains(path, "/messages/"):
		f.updated[path[strings.LastIndex(path, "/")+1:]] = r.URL.Query().Get("visibilitytimeout")
		w.Header().Set("x-ms-popreceipt", "receipt-updated")
		w.Header().Set("x-ms-time-next-visible", time.Now().UTC().Format(time.RFC1123))
		w.WriteHeader(http.StatusNoContent)
	default:
		http.Error(w, "unexpected request "+r.Method+" "+r.URL.String(), http.StatusBadRequest)
	}
}

func (f *fakeQueueService) snapshot() (deleted []string, updated map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	deleted = append([]string(nil), f.deleted...)
	updated = make(map[string]string, len(f.updated))
	maps.Copy(updated, f.updated)
	return deleted, updated
}

func newQueueStorageInputForServer(t *testing.T, srv *httptest.Server) *azureQueueStorage {
	t.Helper()

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	connString := fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;QueueEndpoint=http://%s/devstoreaccount1;", u.Host)

	pConf, err := qsiSpec().ParseYAML(fmt.Sprintf(`
queue_name: test-queue
storage_connection_string: %s
`, connString), service.NewEnvironment())
	require.NoError(t, err)

	conf, err := qsiConfigFromParsed(pConf)
	require.NoError(t, err)

	input, err := newAzureQueueStorage(conf, service.MockResources())
	require.NoError(t, err)
	return input
}

func TestQueueStorageInputAckDeletesMessages(t *testing.T) {
	fake := newFakeQueueService("a", "b")
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	input := newQueueStorageInputForServer(t, srv)
	batch, ackFn, err := input.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 2)

	require.NoError(t, ackFn(t.Context(), nil))

	deleted, updated := fake.snapshot()
	assert.ElementsMatch(t, []string{"a", "b"}, deleted)
	assert.Empty(t, updated)
}

// A rejected batch must stay in the queue for redelivery. Before this was
// fixed the ack function ignored its error argument and deleted the messages
// regardless, so a nack silently lost them.
func TestQueueStorageInputNackKeepsMessages(t *testing.T) {
	fake := newFakeQueueService("a", "b")
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	input := newQueueStorageInputForServer(t, srv)
	batch, ackFn, err := input.ReadBatch(t.Context())
	require.NoError(t, err)
	require.Len(t, batch, 2)

	require.NoError(t, ackFn(t.Context(), errors.New("processing failed")))

	deleted, updated := fake.snapshot()
	assert.Empty(t, deleted, "a rejected message must not be deleted from the queue")
	assert.Equal(t, map[string]string{"a": "0", "b": "0"}, updated, "a rejected message must be made visible again immediately")
}
