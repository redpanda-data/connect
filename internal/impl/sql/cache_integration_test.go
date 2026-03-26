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

package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationCache(t *testing.T) {
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "postgres:latest",
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
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "5432/tcp")
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})

	createTable := func(name string) (string, error) {
		_, err := db.Exec(fmt.Sprintf(`create table "%s" (
  "foo" varchar not null,
  "bar" varchar not null,
  primary key ("foo")
)`, name))
		return name, err
	}

	dsn := fmt.Sprintf("postgres://testuser:testpass@localhost:%s/testdb?sslmode=disable", mp.Port())
	require.Eventually(t, func() bool {
		db, err = sql.Open("postgres", dsn)
		if err != nil {
			return false
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return false
		}
		if _, err := createTable("footable"); err != nil {
			return false
		}
		return true
	}, 3*time.Minute, time.Second)

	template := `
cache_resources:
  - label: testcache
    sql:
      driver: postgres
      dsn: $VAR1
      table: $VAR2
      key_column: foo
      value_column: bar
      set_suffix: "ON CONFLICT (foo) DO UPDATE SET bar=excluded.bar"
`
	suite := integration.CacheTests(
		integration.CacheTestOpenClose(),
		integration.CacheTestMissingKey(),
		integration.CacheTestDoubleAdd(),
		integration.CacheTestDelete(),
		integration.CacheTestGetAndSet(50),
	)
	suite.Run(
		t, template,
		integration.CacheTestOptVarSet("VAR1", dsn),
		integration.CacheTestOptPreTest(func(t testing.TB, _ context.Context, vars *integration.CacheTestConfigVars) {
			tableName := strings.ReplaceAll(vars.ID, "-", "_")
			tableName = "table_" + tableName
			vars.General["VAR2"] = tableName
			_, err := createTable(tableName)
			require.NoError(t, err)
		}),
	)
}
