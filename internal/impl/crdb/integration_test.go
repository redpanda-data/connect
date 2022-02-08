package crdb

import (
	"context"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/integration"
	"github.com/Jeffail/benthos/v3/public/service"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationCRDB(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "cockroachdb/cockroach",
		Tag:          "latest",
		Cmd:          []string{"start-single-node", "--insecure"},
		ExposedPorts: []string{"8080", "26257"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var pgpool *pgxpool.Pool
	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		pgpool, err = pgxpool.Connect(context.Background(), "postgresql://root@localhost:26257/defaultdb?sslmode=disabled")
		if err != nil {
			return err
		}
		// Enable changefeeds
		_, err = pgpool.Exec(context.Background(), "SET CLUSTER SETTING kv.rangefeed.enabled = true;")
		if err != nil {
			return err
		}
		// Create table
		_, err = pgpool.Exec(context.Background(), "CREATE TABLE foo (a INT PRIMARY KEY);")
		if err != nil {
			return err
		}
		// Insert a row in
		_, err = pgpool.Exec(context.Background(), "INSERT INTO foo VALUES (0);")
		return err
	}))
	t.Cleanup(func() {
		pgpool.Exec(context.Background(), "DROP TABLE foo;")
		pgpool.Close()
	})

	template := `
input:
  crdb_changefeed:
    dsn: postgresql://root@localhost:26257/defaultdb?sslmode=disabled
    tables:
			- foo
    options:

output:
  type: stdout
`
	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
		msgBytes, err := mb[0].AsBytes()
		require.NoError(t, err)
		outBatches = append(outBatches, string(msgBytes))
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	require.NoError(t, streamOut.Run(context.Background()))

	// This is where I get stuck because the output is going to change
	assert.Contains(t, []string{
		"foo,[0],\"{\"\"after\"\": {\"\"a\"\": 0}}",
	}, outBatches)
}
