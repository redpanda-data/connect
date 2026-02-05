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

package bluesky

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func TestJetstreamBasic(t *testing.T) {
	events := []JetstreamEvent{
		{
			Did:    "did:plc:test123",
			TimeUS: 1234567890123456,
			Kind:   "commit",
			Commit: &CommitEvent{
				Rev:        "rev1",
				Operation:  "create",
				Collection: "app.bsky.feed.post",
				Rkey:       "abc123",
				Record:     json.RawMessage(`{"text": "Hello, Bluesky!"}`),
			},
		},
		{
			Did:    "did:plc:test456",
			TimeUS: 1234567890123457,
			Kind:   "commit",
			Commit: &CommitEvent{
				Rev:        "rev2",
				Operation:  "create",
				Collection: "app.bsky.feed.like",
				Rkey:       "def456",
			},
		},
		{
			Did:    "did:plc:test789",
			TimeUS: 1234567890123458,
			Kind:   "identity",
			Identity: &IdentityEvent{
				Did:    "did:plc:test789",
				Handle: "alice.bsky.social",
				Seq:    12345,
				Time:   "2024-01-01T00:00:00Z",
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}

		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()

		for _, event := range events {
			data, _ := json.Marshal(event)
			if err := ws.WriteMessage(websocket.TextMessage, data); err != nil {
				t.Error(err)
				return
			}
		}
	}))
	defer server.Close()

	wsURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	wsURL.Scheme = "ws"

	env := service.NewEnvironment()
	pConf, err := jetstreamInputSpec().ParseYAML(fmt.Sprintf(`
endpoint: %v
`, wsURL.String()), env)
	require.NoError(t, err)

	reader, err := newJetstreamReader(pConf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()

	require.NoError(t, reader.Connect(ctx))

	for i, exp := range events {
		msg, ackFn, err := reader.Read(ctx)
		require.NoError(t, err, "reading event %d", i)

		msgBytes, err := msg.AsBytes()
		require.NoError(t, err)

		var actual JetstreamEvent
		require.NoError(t, json.Unmarshal(msgBytes, &actual))

		require.Equal(t, exp.Did, actual.Did)
		require.Equal(t, exp.TimeUS, actual.TimeUS)
		require.Equal(t, exp.Kind, actual.Kind)

		// Verify metadata
		did, exists := msg.MetaGet("bluesky_did")
		require.True(t, exists)
		require.Equal(t, exp.Did, did)

		kind, exists := msg.MetaGet("bluesky_kind")
		require.True(t, exists)
		require.Equal(t, exp.Kind, kind)

		if exp.Commit != nil {
			collection, exists := msg.MetaGet("bluesky_collection")
			require.True(t, exists)
			require.Equal(t, exp.Commit.Collection, collection)

			operation, exists := msg.MetaGet("bluesky_operation")
			require.True(t, exists)
			require.Equal(t, exp.Commit.Operation, operation)
		}

		require.NoError(t, ackFn(ctx, nil))
	}

	require.NoError(t, reader.Close(ctx))
}

func TestJetstreamCompression(t *testing.T) {
	event := JetstreamEvent{
		Did:    "did:plc:test123",
		TimeUS: 1234567890123456,
		Kind:   "commit",
		Commit: &CommitEvent{
			Rev:        "rev1",
			Operation:  "create",
			Collection: "app.bsky.feed.post",
			Rkey:       "abc123",
		},
	}
	raw, err := json.Marshal(event)
	require.NoError(t, err)

	encoder, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	compressed := encoder.EncodeAll(raw, nil)
	require.NoError(t, encoder.Close())

	var gotEncoding string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotEncoding = r.Header.Get("Socket-Encoding")
		upgrader := websocket.Upgrader{}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()

		_ = ws.WriteMessage(websocket.BinaryMessage, compressed)
	}))
	defer server.Close()

	wsURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	wsURL.Scheme = "ws"

	env := service.NewEnvironment()
	pConf, err := jetstreamInputSpec().ParseYAML(fmt.Sprintf(`
endpoint: %v
compress: true
`, wsURL.String()), env)
	require.NoError(t, err)

	reader, err := newJetstreamReader(pConf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, reader.Connect(ctx))

	msg, ackFn, err := reader.Read(ctx)
	require.NoError(t, err)

	msgBytes, err := msg.AsBytes()
	require.NoError(t, err)
	require.Equal(t, raw, msgBytes)
	require.Equal(t, "zstd", gotEncoding)

	require.NoError(t, ackFn(ctx, nil))
	require.NoError(t, reader.Close(ctx))
}

func TestJetstreamFilters(t *testing.T) {
	tests := []struct {
		name        string
		collections []string
		dids        []string
		expectedURL func(baseURL string) string
	}{
		{
			name:        "single collection filter",
			collections: []string{"app.bsky.feed.post"},
			expectedURL: func(base string) string {
				return base + "?wantedCollections=app.bsky.feed.post"
			},
		},
		{
			name:        "multiple collection filters",
			collections: []string{"app.bsky.feed.post", "app.bsky.feed.like"},
			expectedURL: func(base string) string {
				return base + "?wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.like"
			},
		},
		{
			name: "DID filter",
			dids: []string{"did:plc:test123"},
			expectedURL: func(base string) string {
				return base + "?wantedDids=did%3Aplc%3Atest123"
			},
		},
		{
			name:        "combined filters",
			collections: []string{"app.bsky.feed.post"},
			dids:        []string{"did:plc:test123"},
			expectedURL: func(base string) string {
				return base + "?wantedCollections=app.bsky.feed.post&wantedDids=did%3Aplc%3Atest123"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var receivedURL string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				receivedURL = r.URL.String()
				upgrader := websocket.Upgrader{}
				ws, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				ws.Close()
			}))
			defer server.Close()

			wsURL, err := url.Parse(server.URL)
			require.NoError(t, err)
			wsURL.Scheme = "ws"
			wsURL.Path = "/subscribe" // Add path to match what we expect

			collectionsYAML := ""
			if len(test.collections) > 0 {
				collectionsYAML = "collections:\n"
				for _, c := range test.collections {
					collectionsYAML += fmt.Sprintf("  - %s\n", c)
				}
			}

			didsYAML := ""
			if len(test.dids) > 0 {
				didsYAML = "dids:\n"
				for _, d := range test.dids {
					didsYAML += fmt.Sprintf("  - %s\n", d)
				}
			}

			env := service.NewEnvironment()
			pConf, err := jetstreamInputSpec().ParseYAML(fmt.Sprintf(`
endpoint: %v
%s%s`, wsURL.String(), collectionsYAML, didsYAML), env)
			require.NoError(t, err)

			reader, err := newJetstreamReader(pConf, service.MockResources())
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_ = reader.Connect(ctx)
			_ = reader.Close(ctx)

			// Verify query parameters match expected
			expected := test.expectedURL("/subscribe")
			require.Equal(t, expected, receivedURL)
		})
	}
}

func TestJetstreamDisconnect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()

		event := JetstreamEvent{
			Did:    "did:plc:test",
			TimeUS: 9999999999999999,
			Kind:   "commit",
		}
		data, _ := json.Marshal(event)
		_ = ws.WriteMessage(websocket.TextMessage, data)
	}))
	defer server.Close()

	wsURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	wsURL.Scheme = "ws"

	env := service.NewEnvironment()
	pConf, err := jetstreamInputSpec().ParseYAML(fmt.Sprintf(`
endpoint: %v
`, wsURL.String()), env)
	require.NoError(t, err)

	reader, err := newJetstreamReader(pConf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, reader.Connect(ctx))

	msg, ackFn, err := reader.Read(ctx)
	require.NoError(t, err)
	require.NotNil(t, msg)
	require.NoError(t, ackFn(ctx, nil))

	require.Eventually(t, func() bool {
		readCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		_, _, err := reader.Read(readCtx)
		return errors.Is(err, service.ErrNotConnected)
	}, time.Second, 25*time.Millisecond)

	require.NoError(t, reader.Close(ctx))
}

func TestJetstreamCursor(t *testing.T) {
	var receivedURL string
	var mu sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		receivedURL = r.URL.String()
		mu.Unlock()

		upgrader := websocket.Upgrader{}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()

		// Send one event
		event := JetstreamEvent{
			Did:    "did:plc:test",
			TimeUS: 9999999999999999,
			Kind:   "commit",
		}
		data, _ := json.Marshal(event)
		_ = ws.WriteMessage(websocket.TextMessage, data)

		// Wait a bit before closing
		time.Sleep(50 * time.Millisecond)
	}))
	defer server.Close()

	wsURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	wsURL.Scheme = "ws"

	env := service.NewEnvironment()
	pConf, err := jetstreamInputSpec().ParseYAML(fmt.Sprintf(`
endpoint: %v
`, wsURL.String()), env)
	require.NoError(t, err)

	reader, err := newJetstreamReader(pConf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()

	require.NoError(t, reader.Connect(ctx))

	// First connection should have no cursor
	mu.Lock()
	require.NotContains(t, receivedURL, "cursor=")
	mu.Unlock()

	require.NoError(t, reader.Close(ctx))
}

func TestJetstreamValidation(t *testing.T) {
	tests := []struct {
		name        string
		collections int
		dids        int
		expectErr   string
	}{
		{
			name:        "too many collections",
			collections: 101,
			expectErr:   "maximum of 100 collections allowed",
		},
		{
			name:      "too many dids",
			dids:      10001,
			expectErr: "maximum of 10,000 DIDs allowed",
		},
		{
			name:        "valid limits",
			collections: 100,
			dids:        10000,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			collectionsYAML := ""
			if test.collections > 0 {
				collectionsYAML = "collections:\n"
				for i := 0; i < test.collections; i++ {
					collectionsYAML += fmt.Sprintf("  - collection.%d\n", i)
				}
			}

			didsYAML := ""
			if test.dids > 0 {
				didsYAML = "dids:\n"
				for i := 0; i < test.dids; i++ {
					didsYAML += fmt.Sprintf("  - did:plc:test%d\n", i)
				}
			}

			env := service.NewEnvironment()
			pConf, err := jetstreamInputSpec().ParseYAML(fmt.Sprintf(`
endpoint: wss://example.com/subscribe
%s%s`, collectionsYAML, didsYAML), env)
			require.NoError(t, err)

			_, err = newJetstreamReader(pConf, service.MockResources())
			if test.expectErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.expectErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestJetstreamClose(t *testing.T) {
	closeChan := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()
		<-closeChan
	}))
	defer server.Close()

	wsURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	wsURL.Scheme = "ws"

	env := service.NewEnvironment()
	pConf, err := jetstreamInputSpec().ParseYAML(fmt.Sprintf(`
endpoint: %v
`, wsURL.String()), env)
	require.NoError(t, err)

	reader, err := newJetstreamReader(pConf, service.MockResources())
	require.NoError(t, err)

	ctx := context.Background()

	require.NoError(t, reader.Connect(ctx))

	// Close the reader - this should signal shutdown
	closeCtx, closeCancel := context.WithTimeout(ctx, 2*time.Second)
	defer closeCancel()

	require.NoError(t, reader.Close(closeCtx))
	close(closeChan)
}

func TestJetstreamBuildURL(t *testing.T) {
	tests := []struct {
		name        string
		endpoint    string
		collections []string
		dids        []string
		compress    bool
		cursor      int64
		expected    string
	}{
		{
			name:     "default endpoint with no filters",
			endpoint: "",
			expected: "wss://jetstream1.us-east.bsky.network/subscribe",
		},
		{
			name:     "custom endpoint",
			endpoint: "wss://my-pds.example.com/subscribe",
			expected: "wss://my-pds.example.com/subscribe",
		},
		{
			name:        "with collection filter",
			endpoint:    "wss://example.com/subscribe",
			collections: []string{"app.bsky.feed.post"},
			expected:    "wss://example.com/subscribe?wantedCollections=app.bsky.feed.post",
		},
		{
			name:     "with compression",
			endpoint: "wss://example.com/subscribe",
			compress: true,
			expected: "wss://example.com/subscribe?compress=true",
		},
		{
			name:     "with cursor",
			endpoint: "wss://example.com/subscribe",
			cursor:   1234567890,
			expected: "wss://example.com/subscribe?cursor=1234567890",
		},
		{
			name:        "full combination",
			endpoint:    "wss://example.com/subscribe",
			collections: []string{"app.bsky.feed.post"},
			dids:        []string{"did:plc:abc"},
			compress:    true,
			cursor:      1234567890,
			expected:    "wss://example.com/subscribe?compress=true&cursor=1234567890&wantedCollections=app.bsky.feed.post&wantedDids=did%3Aplc%3Aabc",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := &jetstreamReader{
				endpoint:    test.endpoint,
				collections: test.collections,
				dids:        test.dids,
				compress:    test.compress,
			}

			endpoint := test.endpoint
			if endpoint == "" {
				endpoint = reader.endpointCandidates()[0]
			}
			url, err := reader.buildURL(endpoint, test.cursor)
			require.NoError(t, err)
			require.Equal(t, test.expected, url)
		})
	}
}
