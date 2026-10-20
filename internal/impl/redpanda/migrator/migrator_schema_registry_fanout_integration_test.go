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
// identical schema bodies, in both ID-translation modes.
//
// Identical schema bodies deduplicate to a single destination schema ID.
// Registering via franz-go's CreateSchema/CreateSchemaWithIDAndVersion
// resolves the returned ID through SchemaUsagesByID, which fetches every
// subject-version sharing that ID using one unbounded goroutine per usage: N
// such subjects cost ~N(N+1)/2 requests, none of them bounded by
// max_parallel_http_requests. The sync must instead cost one registration per
// subject with no usage fan-out, and respect the configured concurrency
// limit.
func TestIntegrationSchemaRegistryMigratorSyncSharedSchemaFanout(t *testing.T) {
	integration.CheckSkip(t)

	// Number of source subjects sharing one identical schema body. Large
	// enough to make quadratic growth unambiguous while keeping runtime low.
	const numSubjects = 40
	const sharedSchema = `{"type":"record","name":"Shared","fields":[{"name":"a","type":"int"}]}`

	tests := []struct {
		name      string
		translate bool
		mode      sr.Mode
	}{
		{name: "translate_ids=true", translate: true, mode: sr.ModeReadWrite},
		{name: "translate_ids=false", translate: false, mode: sr.ModeImport},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Log("Given: source and destination Redpanda clusters with Schema Registry")
			srcCluster, dstCluster := startRedpandaSourceAndDestination(t)

			srcSR, err := sr.NewClient(sr.URLs(srcCluster.SchemaRegistryURL))
			require.NoError(t, err)

			t.Log("And: a counting reverse proxy in front of the destination Schema Registry")
			proxy := newCountingProxy(t, dstCluster.SchemaRegistryURL)
			dstSR, err := sr.NewClient(sr.URLs(proxy.server.URL))
			require.NoError(t, err)

			t.Logf("And: destination is set to %s mode", tc.mode)
			modeRes := dstSR.SetMode(t.Context(), tc.mode)
			require.NoError(t, modeRes[0].Err)

			t.Logf("And: %d source subjects sharing one identical schema body", numSubjects)
			const autoAssign = -1
			for i := range numSubjects {
				// RegisterSchema: CreateSchema would perform the same usage
				// fan-out this test guards against, against the source.
				_, err := srcSR.RegisterSchema(t.Context(),
					fmt.Sprintf("shared-%03d-value", i),
					sr.Schema{Schema: sharedSchema}, autoAssign, autoAssign)
				require.NoError(t, err)
			}

			t.Logf("When: the schema migrator syncs with translate_ids=%v", tc.translate)
			conf := migrator.SchemaRegistryMigratorConfig{
				Enabled:      true,
				Versions:     migrator.VersionsAll,
				TranslateIDs: tc.translate,
			}
			// NB: the testing constructor sets MaxParallelHTTPRequests to 2.
			m := migrator.NewSchemaRegistryMigratorForTesting(t, conf, srcSR, dstSR)

			ctx, cancel := context.WithTimeout(t.Context(), redpandaTestWaitTimeout)
			defer cancel()
			require.NoError(t, m.Sync(ctx))

			registers := proxy.registers.Load()
			idGets := proxy.idGets.Load()
			versionGets := proxy.versionGets.Load()
			maxInFlight := proxy.maxInFlight.Load()

			t.Logf("Destination registry traffic for %d identical-body subjects:", numSubjects)
			t.Logf("  total requests: %d", proxy.total.Load())
			t.Logf("  schema registrations (POST): %d", registers)
			t.Logf("  usage listings (GET /schemas/ids/N/versions): %d", idGets)
			t.Logf("  usage fan-out (GET /subjects/S/versions/V): %d (O(N^2) worst case: %d)",
				versionGets, numSubjects*(numSubjects+1)/2)
			t.Logf("  peak concurrent in-flight requests: %d (max_parallel_http_requests: 2)", maxInFlight)

			// Guard against a vacuous pass: the sync must actually have
			// registered every subject at the destination.
			assert.GreaterOrEqual(t, registers, int64(numSubjects),
				"expected at least one registration per subject")

			// Registration must not resolve its result through the usage
			// endpoints: any hit is the start of the O(N^2) fan-out.
			assert.Zero(t, versionGets,
				"schema registration must not fan out to the subject-versions sharing the destination schema ID")
			assert.Zero(t, idGets,
				"schema registration must not list usages of the destination schema ID")

			// Concurrency against the destination registry must respect
			// max_parallel_http_requests (2 here).
			assert.LessOrEqual(t, maxInFlight, int64(2),
				"destination request concurrency exceeds max_parallel_http_requests")
		})
	}
}
