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

package cassandra

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationCassandra(t *testing.T) {
	integration.CheckSkip(t)

	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute * 3
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

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		if session == nil {
			conn := gocql.NewCluster(fmt.Sprintf("localhost:%v", resource.GetPort("9042/tcp")))
			conn.Consistency = gocql.All
			var rerr error
			if session, rerr = conn.CreateSession(); rerr != nil {
				return rerr
			}
		}
		_ = session.Query(
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
			integration.StreamTestOutputOnlySendBatch(10, queryGetFn),
		)
		suite.Run(
			t, template,
			integration.StreamTestOptPort(resource.GetPort("9042/tcp")),
			integration.StreamTestOptSleepAfterInput(time.Second*10),
			integration.StreamTestOptSleepAfterOutput(time.Second*10),
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				vars.ID = strings.ReplaceAll(vars.ID, "-", "")
				require.NoError(t, session.Query(
					fmt.Sprintf(
						"CREATE TABLE testspace.table%v (id int primary key, content text, created_at timestamp);",
						vars.ID,
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
    query: 'INSERT INTO testspace.table$ID (id, content, created_at, meows) VALUES (?, ?, ?, ?)'
    args_mapping: |
      root = [ this.id, this.content, now(), [ "first meow", "second meow" ] ]
`
		queryGetFn := func(ctx context.Context, testID, messageID string) (string, []string, error) {
			var resID int
			var resContent string
			var createdAt time.Time
			var meows []string
			if err := session.Query(
				fmt.Sprintf("select id, content, created_at, meows from testspace.table%v where id = ?;", testID), messageID,
			).Scan(&resID, &resContent, &createdAt, &meows); err != nil {
				return "", nil, err
			}
			if time.Since(createdAt) > time.Hour || time.Since(createdAt) < 0 {
				return "", nil, fmt.Errorf("received bad created_at: %v", createdAt)
			}
			assert.Equal(t, []string{"first meow", "second meow"}, meows)
			return fmt.Sprintf(`{"content":"%v","id":%v}`, resContent, resID), nil, err
		}
		suite := integration.StreamTests(
			integration.StreamTestOutputOnlySendSequential(10, queryGetFn),
			integration.StreamTestOutputOnlySendBatch(10, queryGetFn),
		)
		suite.Run(
			t, template,
			integration.StreamTestOptPort(resource.GetPort("9042/tcp")),
			integration.StreamTestOptSleepAfterInput(time.Second*10),
			integration.StreamTestOptSleepAfterOutput(time.Second*10),
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				vars.ID = strings.ReplaceAll(vars.ID, "-", "")
				require.NoError(t, session.Query(
					fmt.Sprintf(
						"CREATE TABLE testspace.table%v (id int primary key, content text, created_at timestamp, meows list<text>);",
						vars.ID,
					),
				).Exec())
			}),
		)
	})
}
