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

	"github.com/moby/moby/api/types/container"
	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/moby/moby/api/types/container"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"

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

	dockerNet, err := network.New(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dockerNet.Remove(context.Background()) })

	// Set a stable hostname so the FE can reference itself in FE_SERVERS using
	// Docker's internal DNS rather than a pre-allocated IP.
	fe, err := testcontainers.Run(ctx, "apache/doris:fe-"+dorisIntegrationVersion,
		testcontainers.WithConfigModifier(func(c *container.Config) {
			c.Hostname = "doris-fe"
		}),
		network.WithNetwork([]string{"doris-fe"}, dockerNet),
		testcontainers.WithExposedPorts("8030/tcp", "9010/tcp", "9030/tcp"),
		testcontainers.WithEnv(map[string]string{
			"FE_SERVERS": "fe1:doris-fe:9010",
			"FE_ID":      "1",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9030/tcp").WithStartupTimeout(5*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, fe)
	require.NoError(t, err)

	// Get the FE's internal Docker network IP so the BE can connect to it.
	feIP := dorisContainerNetworkIP(t, ctx, fe, dockerNet.Name)

	// BE_ADDR is intentionally omitted: the BE image auto-detects its own
	// Docker network IP, which is exactly what stream-load redirects will use.
	be, err := testcontainers.Run(ctx, "apache/doris:be-"+dorisIntegrationVersion,
		network.WithNetwork([]string{"doris-be"}, dockerNet),
		testcontainers.WithExposedPorts("8040/tcp", "9050/tcp"),
		testcontainers.WithEnv(map[string]string{
			"FE_SERVERS": "fe1:" + feIP + ":9010",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("9050/tcp").WithStartupTimeout(5*time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, be)
	require.NoError(t, err)

	// Inspect the BE's Docker-internal IP: used for ALTER SYSTEM ADD BACKEND
	// and as the redirect target for stream-load requests (routable from the
	// host on Linux via Docker bridge networking).
	beIP := dorisContainerNetworkIP(t, ctx, be, dockerNet.Name)

	queryPort, err := fe.MappedPort(ctx, "9030/tcp")
	require.NoError(t, err)

	db := openDorisIntegrationDB(t, queryPort.Port())
	t.Cleanup(func() {
		assert.NoError(t, db.Close())
	})

	t.Log("Given a Doris FE and BE started from the official Docker images")
	waitForDorisBackend(t, db, beIP)
	createDorisStreamLoadTable(t, db)

	feHTTPPort, err := fe.MappedPort(ctx, "8030/tcp")
	require.NoError(t, err)

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
`, feHTTPPort.Port(), queryPort.Port())))

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

// dorisContainerNetworkIP returns the container's IP address on the named
// Docker network by inspecting Docker's container state directly. ContainerIP
// only covers the default bridge; user-defined network IPs are in Networks map.
func dorisContainerNetworkIP(t *testing.T, ctx context.Context, ctr *testcontainers.DockerContainer, networkName string) string {
	t.Helper()
	inspect, err := ctr.Inspect(ctx)
	require.NoError(t, err)
	netInfo, ok := inspect.NetworkSettings.Networks[networkName]
	require.True(t, ok, "container not found on network %q", networkName)
	ip := netInfo.IPAddress.String()
	require.NotEmpty(t, ip, "container has no IP on network %q", networkName)
	return ip
}

func ignoreContextCanceled(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func openDorisIntegrationDB(t *testing.T, queryPort string) *sql.DB {
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
	require.Eventually(t, func() bool {
		return db.Ping() == nil
	}, 5*time.Minute, time.Second, "Doris FE MySQL port never became ready")
	return db
}

func waitForDorisBackend(t *testing.T, db *sql.DB, beIP string) {
	t.Helper()

	require.Eventually(t, func() bool {
		if _, err := db.Exec(fmt.Sprintf(`ALTER SYSTEM ADD BACKEND "%s:9050"`, beIP)); err != nil {
			if !strings.Contains(strings.ToLower(err.Error()), "already") {
				return false
			}
		}
		return waitForAliveDorisBackend(db) == nil
	}, 5*time.Minute, time.Second, "Doris BE never became alive")
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
