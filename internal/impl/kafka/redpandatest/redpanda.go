// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redpandatest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RedpandaEndpoints contains the endpoints for the Redpanda container.
type RedpandaEndpoints struct {
	BrokerAddr        string
	SchemaRegistryURL string
}

// StartRedpanda starts a Redpanda container.
func StartRedpanda(t *testing.T, pool *dockertest.Pool, exposeBroker bool, autocreateTopics bool) (RedpandaEndpoints, error) {
	t.Helper()

	cmd := []string{
		"redpanda",
		"start",
		"--node-id 0",
		"--mode dev-container",
		"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
		"--schema-registry-addr 0.0.0.0:8081",
	}

	if !autocreateTopics {
		cmd = append(cmd, "--set redpanda.auto_create_topics_enabled=false")
	}

	// Expose Schema Registry and Admin API by default. The Admin API is required for health checks.
	exposedPorts := []string{"8081/tcp", "9644/tcp"}
	var portBindings map[docker.Port][]docker.PortBinding
	var kafkaPort string
	if exposeBroker {
		brokerPort, err := integration.GetFreePort()
		if err != nil {
			return RedpandaEndpoints{}, fmt.Errorf("failed to start container: %s", err)
		}

		// Note: Schema Registry uses `--advertise-kafka-addr` to talk to the broker, so we need to use the same port for `--kafka-addr`.
		// TODO: Ensure we don't stomp over some ports which are already in use inside the container.
		cmd = append(cmd, fmt.Sprintf("--kafka-addr 0.0.0.0:%d", brokerPort), fmt.Sprintf("--advertise-kafka-addr localhost:%d", brokerPort))

		kafkaPort = fmt.Sprintf("%d/tcp", brokerPort)
		exposedPorts = append(exposedPorts, kafkaPort)
		portBindings = map[docker.Port][]docker.PortBinding{docker.Port(kafkaPort): {{HostPort: kafkaPort}}}
	}

	options := &dockertest.RunOptions{
		Repository:   "docker.redpanda.com/redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		Cmd:          cmd,
		ExposedPorts: exposedPorts,
		PortBindings: portBindings,
	}

	resource, err := pool.RunWithOptions(options)
	if err != nil {
		return RedpandaEndpoints{}, fmt.Errorf("failed to start container: %s", err)
	}

	if err := resource.Expire(900); err != nil {
		return RedpandaEndpoints{}, fmt.Errorf("failed to set container expiry period: %s", err)
	}

	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	require.NoError(t, pool.Retry(func() error {
		ctx, done := context.WithTimeout(t.Context(), 3*time.Second)
		defer done()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://localhost:%s/v1/cluster/health_overview", resource.GetPort("9644/tcp")), nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %s", err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to execute request: %s", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return errors.New("invalid status")
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body: %s", err)
		}

		var res struct {
			IsHealthy bool `json:"is_healthy"`
		}

		if err := json.Unmarshal(body, &res); err != nil {
			return fmt.Errorf("failed to unmarshal response body: %s", err)
		}

		if !res.IsHealthy {
			return errors.New("unhealthy")
		}

		return nil
	}))

	return RedpandaEndpoints{
		BrokerAddr:        "localhost:" + resource.GetPort(kafkaPort),
		SchemaRegistryURL: "http://localhost:" + resource.GetPort("8081/tcp"),
	}, nil
}
