package integration

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = registerIntegrationTest("sql", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
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
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var db *sql.DB
	t.Cleanup(func() {
		if db != nil {
			db.Close()
		}
	})

	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		var dberr error
		if db == nil {
			if db, dberr = sql.Open(
				"postgres",
				fmt.Sprintf("postgres://testuser:testpass@localhost:%v/testdb?sslmode=disable", resource.GetPort("5432/tcp")),
			); dberr != nil {
				return dberr
			}
			db.SetMaxIdleConns(0)
		}
		if dberr = db.Ping(); err != nil {
			return dberr
		}
		if _, dberr = db.Exec(`create table testtable (
  id varchar(50) not null,
  content varchar(50) not null,
  primary key (id)
);`); dberr != nil {
			return dberr
		}
		return nil
	}))

	template := `
output:
  sql:
    driver: postgres
    data_source_name: postgres://testuser:testpass@localhost:$PORT/testdb?sslmode=disable
    query: "INSERT INTO testtable (id, content) VALUES ($1, $2);"
    args_mapping: '[ "$ID-"+this.id.string(), this.content ]'
`
	queryGetFn := func(env *testEnvironment, id string) (string, []string, error) {
		key := env.configVars.id + "-" + id

		row := db.QueryRowContext(env.ctx, "SELECT content FROM testtable WHERE id = $1;", key)
		if row.Err() != nil {
			return "", nil, row.Err()
		}

		var content string
		err := row.Scan(&content)
		return fmt.Sprintf(`{"content":"%v","id":%v}`, content, id), nil, err
	}
	suite := integrationTests(
		integrationTestOutputOnlySendSequential(10, queryGetFn),
		integrationTestOutputOnlySendBatch(10, queryGetFn),
	)
	suite.Run(
		t, template,
		testOptPort(resource.GetPort("5432/tcp")),
	)
})
