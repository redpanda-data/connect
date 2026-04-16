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

package kafka_test

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
)

// startRedpanda starts a single-node Redpanda container via the testcontainers
// redpanda module and returns the broker address (host:port) and just the port.
func startRedpanda(t testing.TB) (brokerAddr, port string) {
	t.Helper()

	ctr, err := tcredpanda.Run(t.Context(),
		"docker.redpanda.com/redpandadata/redpanda:latest",
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	brokerAddr, err = ctr.KafkaSeedBroker(t.Context())
	require.NoError(t, err)

	brokerAddr, port = brokerAddrPort(brokerAddr)
	return brokerAddr, port
}

// startRedpandaWithSASL starts a single-node Redpanda container with SASL
// authentication enabled and creates an "admin" superuser with password "foobar".
func startRedpandaWithSASL(t testing.TB) (brokerAddr, port string) {
	t.Helper()

	ctr, err := tcredpanda.Run(t.Context(),
		"docker.redpanda.com/redpandadata/redpanda:latest",
		tcredpanda.WithEnableSASL(),
		tcredpanda.WithSuperusers("admin"),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	// Create admin SASL user via rpk. Retry until the admin API is ready.
	require.Eventually(t, func() bool {
		exitCode, out, err := ctr.Exec(t.Context(), []string{
			"rpk", "acl", "user", "create", "admin",
			"--password", "foobar",
			"--api-urls", "localhost:9644",
		})
		if out != nil {
			_, _ = io.Copy(io.Discard, out)
		}
		return err == nil && exitCode == 0
	}, time.Minute, time.Second)

	brokerAddr, err = ctr.KafkaSeedBroker(t.Context())
	require.NoError(t, err)

	brokerAddr, port = brokerAddrPort(brokerAddr)
	return brokerAddr, port
}

// brokerAddrPort normalises a broker address returned by testcontainers,
// replacing "localhost" with "127.0.0.1" to avoid DNS resolution failures
// in environments where the system resolver cannot resolve "localhost".
func brokerAddrPort(addr string) (brokerAddr, port string) {
	addr = strings.Replace(addr, "localhost", "127.0.0.1", 1)
	parts := strings.Split(addr, ":")
	return addr, parts[len(parts)-1]
}
