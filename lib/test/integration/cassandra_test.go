package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = registerIntegrationTest("cassandra", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.Run("cassandra", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var session *gocql.Session
	t.Cleanup(func() {
		if session != nil {
			session.Close()
		}
	})

	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		if session == nil {
			conn := gocql.NewCluster(fmt.Sprintf("localhost:%v", resource.GetPort("9042/tcp")))
			conn.Consistency = gocql.All
			var rerr error
			if session, rerr = conn.CreateSession(); rerr != nil {
				return rerr
			}
		}
		session.Query(
			"CREATE KEYSPACE testspace WITH replication = {'class':'SimpleStrategy','replication_factor':1};",
		).Exec()
		return session.Query(
			"CREATE TABLE testspace.testtable (id int primary key, content text, created_at timestamp);",
		).Exec()
	}))

	t.Run("with JSON", func(t *testing.T) {
		template := `
output:
  cassandra:
    addresses:
      - localhost:$PORT
    query: 'INSERT INTO testspace.table$ID JSON ?'
    args:
      - ${! content() }
`
		queryGetFn := func(env *testEnvironment, id string) (string, []string, error) {
			var resID int
			var resContent string
			if err := session.Query(
				fmt.Sprintf("select id, content from testspace.table%v where id = ?;", env.configVars.id), id,
			).Scan(&resID, &resContent); err != nil {
				return "", nil, err
			}
			return fmt.Sprintf(`{"id":%v,"content":"%v"}`, resID, resContent), nil, err
		}
		suite := integrationTests(
			integrationTestOutputOnlySendSequential(10, queryGetFn),
			integrationTestOutputOnlySendBatch(10, queryGetFn),
		)
		suite.Run(
			t, template,
			testOptPort(resource.GetPort("9042/tcp")),
			testOptPreTest(func(t *testing.T, env *testEnvironment) {
				env.configVars.id = strings.ReplaceAll(env.configVars.id, "-", "")
				require.NoError(t, session.Query(
					fmt.Sprintf(
						"CREATE TABLE testspace.table%v (id int primary key, content text, created_at timestamp);",
						env.configVars.id,
					),
				).Exec())
			}),
		)
	})

	t.Run("with values", func(t *testing.T) {
		template := `
output:
  cassandra:
    addresses:
      - localhost:$PORT
    query: 'INSERT INTO testspace.table$ID (id, content, created_at) VALUES (?, ?, ?)'
    args:
      - ${! json("id") }
      - ${! json("content") }
      - ${! timestamp_unix_nano() }
`
		queryGetFn := func(env *testEnvironment, id string) (string, []string, error) {
			var resID int
			var resContent string
			if err := session.Query(
				fmt.Sprintf("select id, content from testspace.table%v where id = ?;", env.configVars.id), id,
			).Scan(&resID, &resContent); err != nil {
				return "", nil, err
			}
			return fmt.Sprintf(`{"id":%v,"content":"%v"}`, resID, resContent), nil, err
		}
		suite := integrationTests(
			integrationTestOutputOnlySendSequential(10, queryGetFn),
			integrationTestOutputOnlySendBatch(10, queryGetFn),
		)
		suite.Run(
			t, template,
			testOptPort(resource.GetPort("9042/tcp")),
			testOptPreTest(func(t *testing.T, env *testEnvironment) {
				env.configVars.id = strings.ReplaceAll(env.configVars.id, "-", "")
				require.NoError(t, session.Query(
					fmt.Sprintf(
						"CREATE TABLE testspace.table%v (id int primary key, content text, created_at timestamp);",
						env.configVars.id,
					),
				).Exec())
			}),
		)
	})
})
