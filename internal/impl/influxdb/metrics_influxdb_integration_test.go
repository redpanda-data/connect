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

package influxdb

import (
	"encoding/json"
	"fmt"
	"runtime"
	"testing"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestInfluxIntegration(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("skipping test on macos")
	}

	integration.CheckSkip(t)

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctr, err := testcontainers.Run(t.Context(), "influxdb:1.8.3-alpine",
		testcontainers.WithExposedPorts("8086/tcp"),
		testcontainers.WithEnv(map[string]string{
			"INFLUXDB_DB":             "db0",
			"INFLUXDB_ADMIN_USER":     "admin",
			"INFLUXDB_ADMIN_PASSWORD": "admin",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/ping").WithPort("8086/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mappedPort, err := ctr.MappedPort(t.Context(), "8086/tcp")
	require.NoError(t, err)

	url := fmt.Sprintf("http://127.0.0.1:%s", mappedPort.Port())

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: url,
	})
	require.NoError(t, err)

	pConf, err := configSpec().ParseYAML(fmt.Sprintf(`
url: %v
db: db0
interval: 1s
tags:
  hostname: localhost
`, url), nil)
	require.NoError(t, err)

	i, err := fromParsed(pConf, nil)
	if err != nil {
		t.Fatalf("problem creating to InfluxDB: %s", err)
	}
	i.client = c

	t.Run("testInfluxConnect", func(t *testing.T) {
		testInfluxConnect(t, i, c)
	})
}

func testInfluxConnect(t *testing.T, i *influxDBMetrics, c client.Client) {
	i.NewGaugeCtor("testing")().Set(31337)
	i.Close(t.Context())

	resp, err := c.Query(client.Query{Command: `SELECT "hostname"::tag, "value"::field FROM "testing"`, Database: "db0"})
	if err != nil {
		t.Errorf("problem with influx query: %s", err)
	}
	if resp.Error() != nil {
		t.Errorf("problem with influx result: %s", resp.Error())
	}

	if len(resp.Results) != 1 {
		t.Fatal("expected 1 result.")
	}
	if len(resp.Results[0].Series) != 1 {
		t.Fatal("expected 1 series.")
	}
	if len(resp.Results[0].Series[0].Values) != 1 {
		t.Fatal("expected 1 values.")
	}
	if len(resp.Results[0].Series[0].Values[0]) != 3 {
		t.Fatal("expected 3 values.")
	}

	// these show up as json.Number
	hostname := resp.Results[0].Series[0].Values[0][1].(string)
	if hostname != "localhost" {
		t.Errorf("expected localhost received %s", hostname)
	}
	val, err := resp.Results[0].Series[0].Values[0][2].(json.Number).Int64()
	if err != nil {
		t.Errorf("problem converting json.Number: %s", err)
	}
	if val != 31337 {
		t.Errorf("unexpected value")
	}
}
