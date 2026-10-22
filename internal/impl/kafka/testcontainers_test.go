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

package kafka_test

import (
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcredpanda "github.com/testcontainers/testcontainers-go/modules/redpanda"
)

var (
	sharedRedpandaOnce sync.Once
	sharedRedpandaCtr  *tcredpanda.Container
	sharedRedpandaErr  error
	sharedBrokerAddr   string
	sharedBrokerPort   string
)

// sharedRedpanda returns the broker address (host:port) and port of a single
// package-wide, single-node Redpanda container, starting it on first use.
//
// Sharing one container across the many plain-broker integration tests — rather
// than starting a fresh container per test — keeps the package comfortably
// within its `go test -timeout` budget. The benthos stream-test harness derives
// each test's context timeout from the time remaining until the *package*
// deadline (stream_test_helpers.go: `time.Until(deadline) - 5s`), so a package
// full of per-test container starts can starve tests that happen to run late
// under `-shuffle=on`, producing the uniform ~4s "context deadline exceeded"
// failures (CON-445) and, at the limit, package-level timeouts (CON-453).
//
// The container is terminated in TestMain once all tests finish. Tests that need
// a differently-configured cluster — SASL (startRedpandaWithSASL), multi-broker
// (startRedpandaCluster), or a source/destination pair (redpandatest) — still
// start their own containers.
//
// Callers must use unique per-test topic and consumer-group names (the stream
// harness already does, via its generated $ID); createKafkaTopic tolerates a
// pre-existing topic so shared-cluster readiness probes remain safe.
func sharedRedpanda(t testing.TB) (brokerAddr, port string) {
	t.Helper()

	sharedRedpandaOnce.Do(func() {
		// context.Background(), not t.Context(): the container outlives the
		// first test that happens to trigger startup.
		ctx := context.Background()
		ctr, err := tcredpanda.Run(ctx,
			"docker.redpanda.com/redpandadata/redpanda:latest",
		)
		if err != nil {
			sharedRedpandaErr = err
			return
		}
		sharedRedpandaCtr = ctr

		addr, err := ctr.KafkaSeedBroker(ctx)
		if err != nil {
			sharedRedpandaErr = err
			return
		}
		sharedBrokerAddr, sharedBrokerPort = brokerAddrPort(addr)
	})
	require.NoError(t, sharedRedpandaErr)

	return sharedBrokerAddr, sharedBrokerPort
}

// TestMain terminates the shared Redpanda container (if one was started) after
// the package's tests complete.
func TestMain(m *testing.M) {
	code := m.Run()
	if sharedRedpandaCtr != nil {
		_ = sharedRedpandaCtr.Terminate(context.Background())
	}
	os.Exit(code)
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
