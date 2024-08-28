package questdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	qdb "github.com/questdb/go-questdb-client/v3"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationQuestDB(t *testing.T) {
	ctx := context.Background()

	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 3
	resource, err := pool.Run("questdb/questdb", "8.0.0", []string{
		"JAVA_OPTS=-Xms512m -Xmx512m",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	if err = pool.Retry(func() error {
		clientConfStr := fmt.Sprintf("http::addr=localhost:%v", resource.GetPort("9000/tcp"))
		sender, err := qdb.LineSenderFromConf(ctx, clientConfStr)
		if err != nil {
			return err
		}
		defer sender.Close(ctx)
		err = sender.Table("ping").Int64Column("test", 42).AtNow(ctx)
		if err != nil {
			return err
		}
		return sender.Flush(ctx)
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	_ = resource.Expire(900)

	template := `
output:
  questdb:
    address: "localhost:$PORT"
    table: $ID
`
	queryGetFn := func(ctx context.Context, testID, messageID string) (string, []string, error) {
		pgConn, err := pgconn.Connect(ctx, fmt.Sprintf("postgresql://admin:quest@localhost:%v", resource.GetPort("8812/tcp")))
		require.NoError(t, err)
		defer pgConn.Close(ctx)

		result := pgConn.ExecParams(ctx, fmt.Sprintf("SELECT content, id FROM '%v' WHERE id=%v", testID, messageID), nil, nil, nil, nil)

		result.NextRow()
		id, err := strconv.Atoi(string(result.Values()[1]))
		assert.NoError(t, err)
		data := map[string]any{
			"content": string(result.Values()[0]),
			"id":      id,
		}

		assert.False(t, result.NextRow())

		outputBytes, err := json.Marshal(data)
		require.NoError(t, err)
		return string(outputBytes), nil, nil
	}

	suite := integration.StreamTests(
		integration.StreamTestOutputOnlySendSequential(10, queryGetFn),
		integration.StreamTestOutputOnlySendBatch(10, queryGetFn),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(resource.GetPort("9000/tcp")),
	)
}
