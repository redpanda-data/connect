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
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

// Endpoints contains the endpoints for the Redpanda container.
type Endpoints struct {
	BrokerAddr        string
	SchemaRegistryURL string
}

// Config contains configuration for starting a Redpanda broker.
type Config struct {
	// Nightly uses the nightly Redpanda image instead of the latest stable image.
	Nightly bool
	// ExposeBroker exposes the Kafka broker port to the host.
	ExposeBroker bool
	// AutoCreateTopics enables automatic topic creation.
	AutoCreateTopics bool
}

// DefaultConfig returns the default configuration for starting a Redpanda broker.
var DefaultConfig = Config{
	ExposeBroker:     true,
	AutoCreateTopics: true,
}

// StartSingleBroker starts a single Redpanda broker with default configuration.
// It exposes the broker port and enables auto-create topics by default.
func StartSingleBroker(t *testing.T, pool *dockertest.Pool) (Endpoints, *dockertest.Resource, error) {
	t.Helper()
	return StartSingleBrokerWithConfig(t, pool, DefaultConfig)
}

// StartSingleBrokerWithConfig starts a single Redpanda broker with custom configuration.
func StartSingleBrokerWithConfig(t *testing.T, pool *dockertest.Pool, cfg Config) (Endpoints, *dockertest.Resource, error) {
	t.Helper()

	cmd := []string{
		"redpanda",
		"start",
		"--node-id 0",
		"--mode dev-container",
		"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
		"--schema-registry-addr 0.0.0.0:8081",
	}

	if !cfg.AutoCreateTopics {
		cmd = append(cmd, "--set redpanda.auto_create_topics_enabled=false")
	}

	// Expose Schema Registry and Admin API by default. The Admin API is required for health checks.
	exposedPorts := []string{"8081/tcp", "9644/tcp"}
	var portBindings map[docker.Port][]docker.PortBinding
	var kafkaPort string
	if cfg.ExposeBroker {
		brokerPort, err := integration.GetFreePort()
		if err != nil {
			return Endpoints{}, nil, fmt.Errorf("get free port: %w", err)
		}

		// Note: Schema Registry uses `--advertise-kafka-addr` to talk to the broker, so we need to use the same port for `--kafka-addr`.
		// TODO: Ensure we don't stomp over some ports which are already in use inside the container.
		cmd = append(cmd, fmt.Sprintf("--kafka-addr 0.0.0.0:%d", brokerPort), fmt.Sprintf("--advertise-kafka-addr localhost:%d", brokerPort))

		kafkaPort = fmt.Sprintf("%d/tcp", brokerPort)
		exposedPorts = append(exposedPorts, kafkaPort)
		portBindings = map[docker.Port][]docker.PortBinding{
			docker.Port(kafkaPort): {{HostPort: strconv.Itoa(brokerPort)}},
		}
	}

	repo := "docker.redpanda.com/redpandadata/redpanda"
	if cfg.Nightly {
		repo = "docker.redpanda.com/redpandadata/redpanda-nightly"
	}
	options := &dockertest.RunOptions{
		Repository:   repo,
		Tag:          "latest",
		Hostname:     "redpanda",
		Cmd:          cmd,
		ExposedPorts: exposedPorts,
		PortBindings: portBindings,
	}

	resource, err := pool.RunWithOptions(options)
	if err != nil {
		return Endpoints{}, nil, fmt.Errorf("run container: %w", err)
	}

	if err := resource.Expire(900); err != nil {
		return Endpoints{}, nil, fmt.Errorf("set container expiry: %w", err)
	}

	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	if err := pool.Retry(func() error {
		ctx, done := context.WithTimeout(t.Context(), 3*time.Second)
		defer done()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://localhost:%s/v1/cluster/health_overview", resource.GetPort("9644/tcp")), nil)
		if err != nil {
			return fmt.Errorf("create request: %w", err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("execute request: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return errors.New("invalid status")
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("read response body: %w", err)
		}

		var res struct {
			IsHealthy bool `json:"is_healthy"`
		}

		if err := json.Unmarshal(body, &res); err != nil {
			return fmt.Errorf("unmarshal response body: %w", err)
		}

		if !res.IsHealthy {
			return errors.New("unhealthy")
		}

		return nil
	}); err != nil {
		return Endpoints{}, nil, fmt.Errorf("health check: %w", err)
	}

	return Endpoints{
		BrokerAddr:        "localhost:" + resource.GetPort(kafkaPort),
		SchemaRegistryURL: "http://localhost:" + resource.GetPort("8081/tcp"),
	}, resource, nil
}

// StartRedpanda starts a Redpanda container.
//
// Deprecated: Use StartSingleBroker or StartSingleBrokerWithConfig instead.
func StartRedpanda(t *testing.T, pool *dockertest.Pool, exposeBroker, autocreateTopics bool) (Endpoints, error) {
	t.Helper()

	cfg := Config{
		ExposeBroker:     exposeBroker,
		AutoCreateTopics: autocreateTopics,
	}

	endpoints, _, err := StartSingleBrokerWithConfig(t, pool, cfg)
	return endpoints, err
}
