// Copyright 2026 Redpanda Data, Inc.
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

package doris

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

const dorisIntegrationVersion = "4.1.0"

func TestIntegrationDorisStreamLoadOutput(t *testing.T) {
	integration.CheckSkip(t)
	if runtime.GOOS != "linux" {
		t.Skip("Doris Stream Load redirects to the BE container IP, which requires Linux Docker bridge networking")
	}

	ctx := t.Context()
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	if err := pool.Client.Ping(); err != nil {
		t.Skipf("Skipping Doris Docker integration test because Docker is unavailable: %v", err)
	}
	pool.MaxWait = 5 * time.Minute

	network, feIP, beIP := createDorisIntegrationNetwork(t, pool)
	t.Cleanup(func() {
		assert.NoError(t, pool.RemoveNetwork(network))
	})

	feName := fmt.Sprintf("doris-it-fe-%d", time.Now().UnixNano())
	fe, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:         feName,
		Hostname:     feName,
		Repository:   "apache/doris",
		Tag:          "fe-" + dorisIntegrationVersion,
		Networks:     []*dockertest.Network{network},
		ExposedPorts: []string{"8030/tcp", "9010/tcp", "9030/tcp"},
		Env: []string{
			"FE_SERVERS=fe1:" + feIP + ":9010",
			"FE_ID=1",
		},
	}, noRestartAutoRemove)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(fe))
	})
	require.Equal(t, feIP, fe.GetIPInNetwork(network))

	beName := fmt.Sprintf("doris-it-be-%d", time.Now().UnixNano())
	be, err := pool.RunWithOptions(&dockertest.RunOptions{
		Name:         beName,
		Hostname:     beName,
		Repository:   "apache/doris",
		Tag:          "be-" + dorisIntegrationVersion,
		Networks:     []*dockertest.Network{network},
		ExposedPorts: []string{"8040/tcp", "9050/tcp"},
		Env: []string{
			"FE_SERVERS=fe1:" + feIP + ":9010",
			"BE_ADDR=" + beIP + ":9050",
		},
	}, noRestartAutoRemove)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(be))
	})
	require.Equal(t, beIP, be.GetIPInNetwork(network))

	queryPort := fe.GetPort("9030/tcp")
	db := openDorisIntegrationDB(t, pool, queryPort)
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	t.Log("Given a Doris FE and BE started from the official Docker images")
	waitForDorisBackend(t, pool, db, beIP)
	createDorisStreamLoadTable(t, db)

	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.AddOutputYAML(fmt.Sprintf(`
doris_stream_load:
  url: http://127.0.0.1:%s
  database: connect_it
  table: stream_load_events
  username: root
  password: ""
  query_port: %s
  format: json
  read_json_by_line: true
  columns: [id, name, created_at]
  batching:
    count: 2
`, fe.GetPort("8030/tcp"), queryPort)))

	sendBatch, err := streamBuilder.AddBatchProducerFunc()
	require.NoError(t, err)

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- stream.Run(ctx)
	}()
	t.Cleanup(func() {
		assert.NoError(t, stream.StopWithin(10*time.Second))
		assert.NoError(t, ignoreContextCanceled(<-runErrCh))
	})

	t.Log("When Redpanda Connect writes a JSON batch through doris_stream_load")
	require.NoError(t, sendBatch(ctx, service.MessageBatch{
		service.NewMessage([]byte(`{"id":1,"name":"alice","created_at":"2026-05-19 09:00:00"}`)),
		service.NewMessage([]byte(`{"id":2,"name":"bob","created_at":"2026-05-19 09:00:01"}`)),
	}))

	t.Log("Then Doris contains the loaded rows")
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		rows, err := fetchDorisStreamLoadRows(db)
		if !assert.NoError(c, err) {
			return
		}
		assert.Equal(c, []string{
			"1 alice 2026-05-19 09:00:00",
			"2 bob 2026-05-19 09:00:01",
		}, rows)
	}, time.Minute, time.Second)
}

func noRestartAutoRemove(config *docker.HostConfig) {
	config.AutoRemove = true
	config.RestartPolicy = docker.RestartPolicy{Name: "no"}
}

func ignoreContextCanceled(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func createDorisIntegrationNetwork(t *testing.T, pool *dockertest.Pool) (*dockertest.Network, string, string) {
	t.Helper()

	var lastErr error
	for i := range 128 {
		thirdOctet := 50 + int((time.Now().UnixNano()+int64(i))%150)
		subnet := fmt.Sprintf("168.%d.0.0/24", thirdOctet)
		network, err := pool.CreateNetwork(fmt.Sprintf("doris-it-%d-%d", time.Now().UnixNano(), i), func(config *docker.CreateNetworkOptions) {
			config.Driver = "bridge"
			config.IPAM = &docker.IPAMOptions{
				Driver: "default",
				Config: []docker.IPAMConfig{{Subnet: subnet}},
			}
		})
		if err == nil {
			return network, fmt.Sprintf("168.%d.0.2", thirdOctet), fmt.Sprintf("168.%d.0.3", thirdOctet)
		}
		lastErr = err
	}
	require.NoError(t, lastErr)
	return nil, "", ""
}

func openDorisIntegrationDB(t *testing.T, pool *dockertest.Pool, queryPort string) *sql.DB {
	t.Helper()

	cfg := mysqlDriver.NewConfig()
	cfg.Net = "tcp"
	cfg.Addr = net.JoinHostPort("127.0.0.1", queryPort)
	cfg.User = "root"
	cfg.Timeout = 30 * time.Second
	cfg.ReadTimeout = 30 * time.Second
	cfg.WriteTimeout = 30 * time.Second
	cfg.ParseTime = true

	db, err := sql.Open("mysql", cfg.FormatDSN())
	require.NoError(t, err)
	require.NoError(t, pool.Retry(db.Ping))
	return db
}

func waitForDorisBackend(t *testing.T, pool *dockertest.Pool, db *sql.DB, beIP string) {
	t.Helper()

	require.NoError(t, pool.Retry(func() error {
		if _, err := db.Exec(fmt.Sprintf(`ALTER SYSTEM ADD BACKEND "%s:9050"`, beIP)); err != nil && !strings.Contains(strings.ToLower(err.Error()), "already") {
			return fmt.Errorf("adding doris backend: %w", err)
		}
		return waitForAliveDorisBackend(db)
	}))
}

func waitForAliveDorisBackend(db *sql.DB) error {
	rows, err := db.Query("SHOW BACKENDS")
	if err != nil {
		return err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	aliveIndex := -1
	for i, column := range columns {
		if strings.EqualFold(column, "Alive") {
			aliveIndex = i
			break
		}
	}
	if aliveIndex == -1 {
		return errors.New("SHOW BACKENDS did not return an Alive column")
	}

	for rows.Next() {
		values := make([]sql.RawBytes, len(columns))
		scanArgs := make([]any, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}
		if strings.EqualFold(string(values[aliveIndex]), "true") {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return errors.New("no alive doris backend")
}

func createDorisStreamLoadTable(t *testing.T, db *sql.DB) {
	t.Helper()

	_, err := db.Exec("CREATE DATABASE IF NOT EXISTS connect_it")
	require.NoError(t, err)
	_, err = db.Exec("DROP TABLE IF EXISTS connect_it.stream_load_events")
	require.NoError(t, err)
	_, err = db.Exec(`
CREATE TABLE connect_it.stream_load_events (
  id INT,
  name VARCHAR(64),
  created_at DATETIME
)
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1")
`)
	require.NoError(t, err)
}

func fetchDorisStreamLoadRows(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`
SELECT id, name, DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') AS created_at
FROM connect_it.stream_load_events
ORDER BY id
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []string
	for rows.Next() {
		var (
			id        int
			name      string
			createdAt string
		)
		if err := rows.Scan(&id, &name, &createdAt); err != nil {
			return nil, err
		}
		results = append(results, fmt.Sprintf("%d %s %s", id, name, createdAt))
	}
	return results, rows.Err()
}
