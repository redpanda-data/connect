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

package crdb

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationCRDB(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	tmpDir := t.TempDir()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "cockroachdb/cockroach",
		Tag:          "latest",
		Cmd:          []string{"start-single-node", "--insecure"},
		ExposedPorts: []string{"8080/tcp", "26257/tcp"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	port := resource.GetPort("26257/tcp")

	var pgpool *pgxpool.Pool
	require.NoError(t, resource.Expire(900))

	require.NoError(t, pool.Retry(func() error {
		if pgpool == nil {
			if pgpool, err = pgxpool.Connect(t.Context(), fmt.Sprintf("postgresql://root@localhost:%v/defaultdb?sslmode=disable", port)); err != nil {
				return err
			}
		}
		// Enable changefeeds
		if _, err = pgpool.Exec(t.Context(), "SET CLUSTER SETTING kv.rangefeed.enabled = true;"); err != nil {
			return err
		}
		// Create table
		_, err = pgpool.Exec(t.Context(), "CREATE TABLE foo (a INT PRIMARY KEY);")
		return err
	}))
	t.Cleanup(func() {
		pgpool.Close()
	})

	// Create a backlog of rows
	for i := range 100 {
		// Insert some rows
		if _, err = pgpool.Exec(t.Context(), fmt.Sprintf("INSERT INTO foo VALUES (%v);", i)); err != nil {
			return
		}
	}

	template := fmt.Sprintf(`
cockroachdb_changefeed:
  dsn: postgres://root@localhost:%v/defaultdb?sslmode=disable
  tables:
    - foo
  cursor_cache: foocache
`, port)

	cacheConf := fmt.Sprintf(`
label: foocache
file:
  directory: %v
`, tmpDir)

	streamOutBuilder := service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	var outBatches []string
	var outBatchMut sync.Mutex
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		msgBytes, err := mb[0].AsBytes()
		require.NoError(t, err)
		outBatchMut.Lock()
		outBatches = append(outBatches, string(msgBytes))
		outBatchMut.Unlock()
		return nil
	}))

	streamOut, err := streamOutBuilder.Build()
	require.NoError(t, err)

	go func() {
		_ = streamOut.Run(t.Context())
	}()

	for i := range 900 {
		// Insert some more rows in
		if _, err = pgpool.Exec(t.Context(), fmt.Sprintf("INSERT INTO foo VALUES (%v);", 100+i)); err != nil {
			t.Error(err)
		}
	}

	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		return len(outBatches) == 1000
	}, time.Second*5, time.Millisecond*100)

	require.NoError(t, streamOut.StopWithin(time.Second*10))

	//--------------------------------------------------------------------------

	// Execute once more and ensure we don't backfil
	streamOutBuilder = service.NewStreamBuilder()
	require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))
	require.NoError(t, streamOutBuilder.AddCacheYAML(cacheConf))
	require.NoError(t, streamOutBuilder.AddInputYAML(template))

	outBatches = nil
	require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(_ context.Context, mb service.MessageBatch) error {
		msgBytes, err := mb[0].AsBytes()
		require.NoError(t, err)
		outBatchMut.Lock()
		outBatches = append(outBatches, string(msgBytes))
		outBatchMut.Unlock()
		return nil
	}))

	streamOut, err = streamOutBuilder.Build()
	require.NoError(t, err)

	go func() {
		assert.NoError(t, streamOut.Run(t.Context()))
	}()

	time.Sleep(time.Second)
	for i := range 50 {
		// Insert some more rows
		if _, err = pgpool.Exec(t.Context(), fmt.Sprintf("INSERT INTO foo VALUES (%v);", 1000+i)); err != nil {
			t.Error(err)
		}
	}

	var tmpSize int
	assert.Eventually(t, func() bool {
		outBatchMut.Lock()
		defer outBatchMut.Unlock()
		tmpSize = len(outBatches)
		return tmpSize == 50
	}, time.Second*10, time.Millisecond*100, "length: %v", tmpSize)

	require.NoError(t, streamOut.StopWithin(time.Second*10))
}
