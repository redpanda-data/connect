package crdb

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
	"github.com/benthosdev/benthos/v4/public/service"
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
		PortBindings: map[docker.Port][]docker.PortBinding{
			"26257/tcp": {{HostIP: "", HostPort: "26257"}},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var pgpool *pgxpool.Pool
	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		pgpool, err = pgxpool.Connect(context.Background(), "postgresql://root@localhost:26257/defaultdb?sslmode=disable")
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
		_, _ = pgpool.Exec(context.Background(), "DROP TABLE foo;")
		pgpool.Close()
	})

	template := `
crdb_changefeed:
  dsn: postgresql://root@localhost:26257/defaultdb?sslmode=disable
  tables:
    - foo
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
