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

package arc

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationArc(t *testing.T) {
	integration.CheckSkip(t)

	containerPort := "8000/tcp"

	startupTimeout := 2 * time.Minute
	if deadline, ok := t.Deadline(); ok {
		startupTimeout = time.Until(deadline) - 100*time.Millisecond
	}

	ctr, err := testcontainers.Run(t.Context(), "ghcr.io/basekick-labs/arc:latest",
		testcontainers.WithExposedPorts(containerPort),
		testcontainers.WithEnv(map[string]string{
			"ARC_AUTH_ENABLED":      "false",
			"ARC_TELEMETRY_ENABLED": "false",
			"STORAGE_BACKEND":       "local",
		}),
		testcontainers.WithWaitStrategyAndDeadline(startupTimeout,
			wait.ForHTTP("/health").
				WithPort(nat.Port(containerPort)).
				WithPollInterval(2*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	portM, err := ctr.MappedPort(t.Context(), nat.Port(containerPort))
	require.NoError(t, err)
	arcURL := fmt.Sprintf("http://localhost:%s", portM.Port())

	t.Run("columnar write and query", func(t *testing.T) {
		conf, err := outputSpec().ParseYAML(fmt.Sprintf(`
base_url: %s
database: default
measurement: integration_test
format: columnar
compression: zstd
`, arcURL), nil)
		require.NoError(t, err)

		out, err := newArcOutput(conf, service.MockResources())
		require.NoError(t, err)
		require.NoError(t, out.Connect(t.Context()))
		defer out.Close(t.Context())

		batch := service.MessageBatch{
			service.NewMessage([]byte(`{"vehicle_id":"truck-1","lat":40.7128,"lon":-74.006,"speed_kmh":55}`)),
			service.NewMessage([]byte(`{"vehicle_id":"truck-2","lat":34.0522,"lon":-118.2437,"speed_kmh":72}`)),
			service.NewMessage([]byte(`{"vehicle_id":"truck-3","lat":41.8781,"lon":-87.6298,"speed_kmh":45}`)),
		}

		require.NoError(t, out.WriteBatch(t.Context(), batch))

		// Wait for Arc to flush the buffer to Parquet
		// Arc buffers data in memory before flushing, so we need to wait
		var rowCount float64
		require.Eventually(t, func() bool {
			queryBody := `{"sql": "SELECT vehicle_id, speed_kmh FROM default.integration_test ORDER BY vehicle_id"}`
			resp, err := http.Post(arcURL+"/api/v1/query", "application/json", strings.NewReader(queryBody))
			if err != nil {
				return false
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return false
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return false
			}

			var result map[string]any
			if err := json.Unmarshal(body, &result); err != nil {
				return false
			}

			rc, ok := result["row_count"].(float64)
			if !ok {
				return false
			}
			rowCount = rc
			return rowCount == 3
		}, 30*time.Second, 2*time.Second, "expected 3 rows, got %.0f", rowCount)
	})

	t.Run("row format write", func(t *testing.T) {
		conf, err := outputSpec().ParseYAML(fmt.Sprintf(`
base_url: %s
database: default
measurement: integration_test_row
format: row
compression: zstd
`, arcURL), nil)
		require.NoError(t, err)

		out, err := newArcOutput(conf, service.MockResources())
		require.NoError(t, err)
		require.NoError(t, out.Connect(t.Context()))
		defer out.Close(t.Context())

		batch := service.MessageBatch{
			service.NewMessage([]byte(`{"sensor":"temp-1","value":22.5}`)),
			service.NewMessage([]byte(`{"sensor":"temp-2","value":23.1}`)),
		}

		require.NoError(t, out.WriteBatch(t.Context(), batch))

		// Verify data landed in Arc
		require.Eventually(t, func() bool {
			queryBody := `{"sql": "SELECT sensor, value FROM default.integration_test_row ORDER BY sensor"}`
			resp, err := http.Post(arcURL+"/api/v1/query", "application/json", strings.NewReader(queryBody))
			if err != nil {
				return false
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return false
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return false
			}
			var result map[string]any
			if err := json.Unmarshal(body, &result); err != nil {
				return false
			}
			rc, ok := result["row_count"].(float64)
			return ok && rc == 2
		}, 30*time.Second, 2*time.Second, "expected 2 rows from row format write")
	})
}
