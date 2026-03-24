// Copyright 2024 Redpanda Data, Inc.
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

package couchbase_test

import (
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tccouchbase "github.com/testcontainers/testcontainers-go/modules/couchbase"
)

var (
	username           = "benthos"
	password           = "password"
	port               = ""
	integrationCleanup func()
	integrationOnce    sync.Once
)

// TestMain cleanup couchbase cluster if required by tests.
func TestMain(m *testing.M) {
	code := m.Run()
	if integrationCleanup != nil {
		integrationCleanup()
	}

	os.Exit(code)
}

func requireCouchbase(tb testing.TB) string {
	integrationOnce.Do(func() {
		ctr, err := tccouchbase.Run(tb.Context(), "couchbase:latest",
			tccouchbase.WithAdminCredentials(username, password),
		)
		require.NoError(tb, err)

		mappedPort, err := ctr.MappedPort(tb.Context(), "11210/tcp")
		require.NoError(tb, err)

		port = mappedPort.Port()
		integrationCleanup = func() {
			_ = testcontainers.TerminateContainer(ctr)
		}
	})

	return port
}
