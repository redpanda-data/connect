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

package aws

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/opensearch"
)

func TestOutputSignsRetriedRequestsOverTheirBody(t *testing.T) {
	type attempt struct {
		bodyHash   string
		signedHash string
	}

	var (
		mu       sync.Mutex
		attempts []attempt
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		sum := sha256.Sum256(body)

		mu.Lock()
		attempts = append(attempts, attempt{
			bodyHash:   hex.EncodeToString(sum[:]),
			signedHash: r.Header.Get("X-Amz-Content-Sha256"),
		})
		n := len(attempts)
		mu.Unlock()

		// Fail the first attempt with a retryable status so the client
		// sends the bulk request again.
		if n == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"took":1,"errors":false,"items":[{"index":{"_index":"foo","_id":"1","status":201}}]}`))
	}))
	defer srv.Close()

	pConf, err := opensearch.OutputSpec().ParseYAML(`
urls: [ "`+srv.URL+`" ]
index: foo
id: "1"
action: index
aws:
  enabled: true
  region: us-east-1
  credentials:
    id: foo
    secret: bar
`, nil)
	require.NoError(t, err)

	out, err := opensearch.OutputFromParsed(pConf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, out.Connect(t.Context()))

	require.NoError(t, out.WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(`{"hello":"world"}`)),
	}))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, attempts, 2)
	for i, a := range attempts {
		assert.Equal(t, a.bodyHash, a.signedHash, "attempt %d was signed over a different body", i+1)
	}
}
