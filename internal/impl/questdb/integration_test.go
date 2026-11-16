// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package questdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	qdb "github.com/questdb/go-questdb-client/v4"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationQuestDB(t *testing.T) {
	ctx := t.Context()

	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "questdb/questdb:8.0.0",
		testcontainers.WithExposedPorts("9000/tcp", "8812/tcp"),
		testcontainers.WithEnv(map[string]string{
			"JAVA_OPTS": "-Xms512m -Xmx512m",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9000/tcp").WithStartupTimeout(3*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	httpPortM, err := ctr.MappedPort(t.Context(), "9000/tcp")
	require.NoError(t, err)
	httpPort := httpPortM.Port()

	pgPortM, err := ctr.MappedPort(t.Context(), "8812/tcp")
	require.NoError(t, err)
	pgPort := pgPortM.Port()

	// Verify QuestDB is ready
	require.Eventually(t, func() bool {
		clientConfStr := fmt.Sprintf("http::addr=localhost:%v", httpPort)
		sender, err := qdb.LineSenderFromConf(ctx, clientConfStr)
		if err != nil {
			return false
		}
		defer sender.Close(ctx)
		if err := sender.Table("ping").Int64Column("test", 42).AtNow(ctx); err != nil {
			return false
		}
		return sender.Flush(ctx) == nil
	}, 3*time.Minute, 2*time.Second, "Could not connect to QuestDB")

	template := `
output:
  questdb:
    address: "localhost:$PORT"
    table: $ID
`
	queryGetFn := func(ctx context.Context, testID, messageID string) (string, []string, error) {
		pgConn, err := pgconn.Connect(ctx, fmt.Sprintf("postgresql://admin:quest@localhost:%v", pgPort))
		if err != nil {
			return "", nil, err
		}
		defer pgConn.Close(ctx)

		// QuestDB creates WAL tables for ILP writes. A WAL table shows new rows
		// only after a background job applies the WAL, so a query right after
		// the write can find no table or no rows. Query again until the row
		// shows or the deadline expires.
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		query := fmt.Sprintf("SELECT content, id FROM '%v' WHERE id=%v", testID, messageID)
		var res *pgconn.Result
		for {
			res = pgConn.ExecParams(ctx, query, nil, nil, nil, nil).Read()
			if res.Err == nil && len(res.Rows) > 0 {
				break
			}
			select {
			case <-ctx.Done():
				return "", nil, fmt.Errorf("row %v not visible in table %v: last error: %v, rows: %v: %w", messageID, testID, res.Err, len(res.Rows), ctx.Err())
			case <-time.After(100 * time.Millisecond):
			}
		}
		if len(res.Rows) != 1 {
			return "", nil, fmt.Errorf("want 1 row with id %v, got %v", messageID, len(res.Rows))
		}

		id, err := strconv.Atoi(string(res.Rows[0][1]))
		if err != nil {
			return "", nil, err
		}
		outputBytes, err := json.Marshal(map[string]any{
			"content": string(res.Rows[0][0]),
			"id":      id,
		})
		if err != nil {
			return "", nil, err
		}
		return string(outputBytes), nil, nil
	}

	suite := integration.StreamTests(
		integration.StreamTestOutputOnlySendSequential(10, queryGetFn),
		integration.StreamTestOutputOnlySendBatch(10, queryGetFn),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPort(httpPort),
	)
}
