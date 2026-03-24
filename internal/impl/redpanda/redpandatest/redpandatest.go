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
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
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
func StartSingleBroker(t *testing.T) (Endpoints, testcontainers.Container, error) {
	t.Helper()
	return StartSingleBrokerWithConfig(t, DefaultConfig)
}

// StartSingleBrokerWithConfig starts a single Redpanda broker with custom configuration.
func StartSingleBrokerWithConfig(t *testing.T, cfg Config) (Endpoints, testcontainers.Container, error) {
	t.Helper()

	img := "docker.redpanda.com/redpandadata/redpanda:latest"
	if cfg.Nightly {
		img = "docker.redpanda.com/redpandadata/redpanda-nightly:latest"
	}

	var opts []testcontainers.ContainerCustomizer
	if cfg.AutoCreateTopics {
		opts = append(opts, tcredpanda.WithAutoCreateTopics())
	}

	ctr, err := tcredpanda.Run(t.Context(), img, opts...)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		return Endpoints{}, ctr, err
	}

	broker, err := ctr.KafkaSeedBroker(t.Context())
	if err != nil {
		return Endpoints{}, ctr, err
	}

	srAddr, err := ctr.SchemaRegistryAddress(t.Context())
	if err != nil {
		return Endpoints{}, ctr, err
	}

	return Endpoints{
		BrokerAddr:        broker,
		SchemaRegistryURL: srAddr,
	}, ctr, nil
}

// StartRedpanda starts a Redpanda container.
//
// Deprecated: Use StartSingleBroker or StartSingleBrokerWithConfig instead.
func StartRedpanda(t *testing.T, exposeBroker, autocreateTopics bool) (Endpoints, error) {
	t.Helper()

	cfg := Config{
		ExposeBroker:     exposeBroker,
		AutoCreateTopics: autocreateTopics,
	}

	endpoints, _, err := StartSingleBrokerWithConfig(t, cfg)
	return endpoints, err
}
