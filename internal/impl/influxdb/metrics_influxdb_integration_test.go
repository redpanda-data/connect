package influxdb

import (
	"encoding/json"
	"fmt"
	"runtime"
	"testing"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
	"github.com/ory/dockertest/v3"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

func TestInfluxIntegration(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("skipping test on macos")
	}

	integration.CheckSkip(t)

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "influxdb",
		Tag:        "1.8.3-alpine",
		Env: []string{
			"INFLUXDB_DB=db0",
			"INFLUXDB_ADMIN_USER=admin",
			"INFLUXDB_ADMIN_PASSWORD=admin",
		},
	})
	if err != nil {
		t.Fatalf("Could not start resource: %v", err)
	}

	url := fmt.Sprintf("http://127.0.0.1:%v", resource.GetPort("8086/tcp"))

	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	var c client.Client
	if err = pool.Retry(func() error {
		c, err = client.NewHTTPClient(client.HTTPConfig{
			Addr: url,
		})
		if err != nil {
			return fmt.Errorf("problem creating influx client: %s", err)
		}
		defer c.Close()

		_, _, err = c.Ping(5 * time.Second)
		if err != nil {
			return fmt.Errorf("problem connecting to influx: %s", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("Could not connect to influxdb docker container: %s", err)
	}

	globalConfig := metrics.NewConfig()
	config := metrics.NewInfluxDBConfig()
	config.URL = url
	config.DB = "db0"
	config.Interval = "1s"
	config.Tags = map[string]string{"hostname": "localhost"}
	globalConfig.InfluxDB = config

	flux, err := newInfluxDB(globalConfig, mock.NewManager())
	if err != nil {
		t.Fatalf("problem creating to InfluxDB: %s", err)
	}
	i := flux.(*influxDBMetrics)
	i.client = c

	t.Run("testInfluxConnect", func(t *testing.T) {
		testInfluxConnect(t, flux, c)
	})
}

func testInfluxConnect(t *testing.T, i metrics.Type, c client.Client) {
	i.GetGauge("testing").Set(31337)
	i.Close()

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
