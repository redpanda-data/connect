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

package sql_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	sharedPostgresOnce sync.Once
	sharedPostgresCtr  testcontainers.Container
	sharedPostgresDSN  string
	sharedPostgresErr  error
)

// sharedPostgres returns the DSN of one Postgres container for the package. The first call starts the container.
// TestIntegrationPostgres and TestIntegrationCache use it. Each test creates tables with unique names, so the tests
// can run at the same time. TestMain terminates the container.
func sharedPostgres(t *testing.T) string {
	t.Helper()

	sharedPostgresOnce.Do(func() {
		// Use context.Background(), not t.Context(): the container outlives the test that starts it.
		ctx := context.Background()
		ctr, err := testcontainers.Run(ctx, "postgres:latest",
			testcontainers.WithExposedPorts("5432/tcp"),
			testcontainers.WithEnv(map[string]string{
				"POSTGRES_USER":     "testuser",
				"POSTGRES_PASSWORD": "testpass",
				"POSTGRES_DB":       "testdb",
			}),
			testcontainers.WithWaitStrategy(
				wait.ForListeningPort("5432/tcp").WithStartupTimeout(3*time.Minute),
			),
		)
		sharedPostgresCtr = ctr
		if err != nil {
			sharedPostgresErr = err
			return
		}

		mp, err := ctr.MappedPort(ctx, "5432/tcp")
		if err != nil {
			sharedPostgresErr = err
			return
		}
		sharedPostgresDSN = fmt.Sprintf("postgres://testuser:testpass@localhost:%s/testdb?sslmode=disable", mp.Port())
	})
	require.NoError(t, sharedPostgresErr)

	return sharedPostgresDSN
}

// TestMain terminates the shared Postgres container (if a test started it) after the tests of the package complete.
func TestMain(m *testing.M) {
	code := m.Run()
	if sharedPostgresCtr != nil {
		_ = testcontainers.TerminateContainer(sharedPostgresCtr)
	}
	os.Exit(code)
}
