// Copyright 2025 Redpanda Data, Inc.
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

package migrator_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/sr"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/migrator"
)

// countingProxy is a reverse proxy in front of the destination Schema
// Registry that records request counts and peak concurrent in-flight
// requests.
type countingProxy struct {
	server *httptest.Server

	total       atomic.Int64 // all requests
	registers   atomic.Int64 // POST /subjects/{s}/versions
	idGets      atomic.Int64 // GET  /schemas/ids/{id}/versions (usage listing)
	versionGets atomic.Int64 // GET  /subjects/{s}/versions/{v} (usage fan-out)
	inFlight    atomic.Int64
	maxInFlight atomic.Int64
}

var (
	reVersionGet = regexp.MustCompile(`^/subjects/[^/]+/versions/[^/]+$`)
	reIDGet      = regexp.MustCompile(`^/schemas/ids/\d+/versions$`)
	reRegister   = regexp.MustCompile(`^/subjects/[^/]+/versions$`)
)

func newCountingProxy(t *testing.T, targetURL string) *countingProxy {
	t.Helper()

	target, err := url.Parse(targetURL)
	require.NoError(t, err)
	rp := httputil.NewSingleHostReverseProxy(target)

	p := &countingProxy{}
	p.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := p.inFlight.Add(1)
		defer p.inFlight.Add(-1)
		for {
			maxSeen := p.maxInFlight.Load()
			if cur <= maxSeen || p.maxInFlight.CompareAndSwap(maxSeen, cur) {
				break
			}
		}

		p.total.Add(1)
		switch {
		case r.Method == "GET" && reVersionGet.MatchString(r.URL.Path):
			p.versionGets.Add(1)
		case r.Method == "GET" && reIDGet.MatchString(r.URL.Path):
			p.idGets.Add(1)
		case r.Method == "POST" && reRegister.MatchString(r.URL.Path):
			p.registers.Add(1)
		}

		rp.ServeHTTP(w, r)
	}))
	t.Cleanup(p.server.Close)

	return p
}

// TestIntegrationSchemaRegistryMigratorSyncSharedSchemaFanout guards against
// O(N^2) destination-registry traffic when syncing subjects that share
// identical schema bodies with translate_ids enabled.
//
// Identical schema bodies deduplicate to a single destination schema ID.
// Registering via franz-go's CreateSchema resolves the returned ID through
// SchemaUsagesByID, which fetches every subject-version sharing that ID using
// one unbounded goroutine per usage: N such subjects cost ~N(N+1)/2 requests,
// none of them bounded by max_parallel_http_requests. The sync must instead
// stay O(N) and respect the configured concurrency limit.
func TestIntegrationSchemaRegistryMigratorSyncSharedSchemaFanout(t *testing.T) {
	integration.CheckSkip(t)

	// Number of source subjects sharing one identical schema body. Large
	// enough to make quadratic growth unambiguous while keeping runtime low.
	const numSubjects = 40

	t.Log("Given: source and destination Redpanda clusters with Schema Registry")
	srcCluster, dstCluster := startRedpandaSourceAndDestination(t)

	srcSR, err := sr.NewClient(sr.URLs(srcCluster.SchemaRegistryURL))
	require.NoError(t, err)

	t.Log("And: a counting reverse proxy in front of the destination Schema Registry")
	proxy := newCountingProxy(t, dstCluster.SchemaRegistryURL)
	dstSR, err := sr.NewClient(sr.URLs(proxy.server.URL))
	require.NoError(t, err)

	t.Logf("And: %d source subjects sharing one identical schema body", numSubjects)
	const sharedSchema = `{"type":"record","name":"Shared","fields":[{"name":"a","type":"int"}]}`
	ctx := t.Context()
	for i := range numSubjects {
		_, err := srcSR.CreateSchema(ctx, fmt.Sprintf("shared-%03d-value", i), sr.Schema{Schema: sharedSchema})
		require.NoError(t, err)
	}

	t.Log("When: the schema migrator syncs with translate_ids enabled")
	conf := migrator.SchemaRegistryMigratorConfig{
		Enabled:      true,
		Versions:     migrator.VersionsAll,
		TranslateIDs: true,
	}
	// NB: the testing constructor sets MaxParallelHTTPRequests to 2.
	m := migrator.NewSchemaRegistryMigratorForTesting(t, conf, srcSR, dstSR)

	syncCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	require.NoError(t, m.Sync(syncCtx))

	registers := proxy.registers.Load()
	idGets := proxy.idGets.Load()
	versionGets := proxy.versionGets.Load()
	maxInFlight := proxy.maxInFlight.Load()

	t.Logf("Destination registry traffic for %d identical-body subjects:", numSubjects)
	t.Logf("  schema registrations (POST): %d", registers)
	t.Logf("  usage listings (GET /schemas/ids/N/versions): %d", idGets)
	t.Logf("  usage fan-out (GET /subjects/S/versions/V): %d (O(N) expectation: <=%d, O(N^2) worst case: %d)",
		versionGets, numSubjects, numSubjects*(numSubjects+1)/2)
	t.Logf("  peak concurrent in-flight requests: %d (max_parallel_http_requests: 2)", maxInFlight)

	// Syncing N subjects must cost O(N) destination requests. 2*N leaves room
	// for one extra lookup per subject.
	assert.LessOrEqual(t, versionGets, int64(2*numSubjects),
		"O(N^2) subject-version GETs against the destination registry - "+
			"schema registration must not fan out to every subject sharing the "+
			"destination schema ID")

	// Concurrency against the destination registry must respect
	// max_parallel_http_requests (2 here).
	assert.LessOrEqual(t, maxInFlight, int64(2),
		"destination request concurrency exceeds max_parallel_http_requests")
}
