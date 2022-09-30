package cassandra

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func TestIntegrationCassandra(t *testing.T) {
	integration.CheckSkip(t)
	if runtime.GOOS == "darwin" {
		t.Skip("skipping test on macos")
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 120
	resource, err := pool.Run("cassandra", "latest", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var session *gocql.Session = nil
	var rerr error
	t.Cleanup(func() {
		if session != nil {
			session.Close()
		}
	})

	time.Sleep(time.Second * 20)

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		if session == nil {
			conn := gocql.NewCluster("172.17.0.2")
			timeout, rerr := time.ParseDuration("2000ms")
			if err != nil {
				return rerr
			}
			conn.Timeout = timeout
			if session, rerr = conn.CreateSession(); rerr != nil {
				return rerr
			}
		}

		if session != nil {
			ctx := context.Background()

			_ = session.Query(
				"CREATE KEYSPACE IF NOT EXISTS testspace WITH replication = {'class':'SimpleStrategy','replication_factor':1};",
			).WithContext(ctx).Exec()
			time.Sleep(time.Second)
			rerr = session.Query(
				"CREATE TABLE IF NOT EXISTS testspace.testtable (id int primary key, content text, created_at timestamp);",
			).WithContext(ctx).Exec()
			time.Sleep(time.Second)
		}
		return rerr
	}))

	t.Run("with JSON", func(t *testing.T) {
		template := `
output:
  cassandra:
    addresses:
      - 172.17.0.2
    query: 'INSERT INTO testspace.table$ID JSON ?'
    args_mapping: 'root = [ this ]'
`
		queryGetFn := func(ctx context.Context, testID, messageID string) (string, []string, error) {
			var resID int
			var resContent string
			if err := session.Query(
				fmt.Sprintf("select id, content from testspace.table%v where id = ?;", testID), messageID,
			).Scan(&resID, &resContent); err != nil {
				return "", nil, err
			}
			return fmt.Sprintf(`{"content":"%v","id":%v}`, resContent, resID), nil, err
		}
		suite := integration.StreamTests(
			integration.StreamTestOutputOnlySendSequential(10, queryGetFn),
	//		integration.StreamTestOutputOnlySendBatch(10, queryGetFn),
		)
		suite.Run(
			t, template,
	//		integration.StreamTestOptPort(resource.GetPort("9042/tcp")),
			integration.StreamTestOptSleepAfterInput(time.Second*10),
			integration.StreamTestOptSleepAfterOutput(time.Second*10),
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.StreamTestConfigVars) {
				vars.ID = strings.ReplaceAll(testID, "-", "")
				require.NoError(t, session.Query(
					fmt.Sprintf(
						"CREATE TABLE IF NOT EXISTS testspace.table%v (id int primary key, content text, created_at timestamp);",
						vars.ID,
					),
				).Exec())

				time.Sleep(time.Second * 5)
			}),
		)
	})
}
