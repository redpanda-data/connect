package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func TestIntegrationCache(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = 3 * time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "postgres",
		ExposedPorts: []string{"5432/tcp"},
		Env: []string{
			"POSTGRES_USER=testuser",
			"POSTGRES_PASSWORD=testpass",
			"POSTGRES_DB=testdb",
		},
	})
	require.NoError(t, err)

	var db *sql.DB
	t.Cleanup(func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %s", err)
		}
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

	dsn := fmt.Sprintf("postgres://testuser:testpass@localhost:%s/testdb?sslmode=disable", resource.GetPort("5432/tcp"))
	require.NoError(t, pool.Retry(func() error {
		db, err = sql.Open("postgres", dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			db.Close()
			db = nil
			return err
		}
		if _, err := createTable("footable"); err != nil {
			return err
		}
		return nil
	}))

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
		integration.CacheTestOptVarOne(dsn),
		integration.CacheTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.CacheTestConfigVars) {
			tableName := strings.ReplaceAll(testID, "-", "_")
			tableName = "table_" + tableName
			vars.Var2 = tableName
			_, err := createTable(tableName)
			require.NoError(t, err)
		}),
	)
}
