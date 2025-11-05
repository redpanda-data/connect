// Copyright 2025 Redpanda Data, Inc.
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

package migrator_test

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"text/template"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
	"github.com/redpanda-data/connect/v4/internal/impl/redpanda/redpandatest"
	_ "github.com/redpanda-data/connect/v4/public/components/prometheus"
)

var (
	soakHTTPAddr               = flag.String("soak-http-addr", "127.0.0.1:4195", "HTTP address used by connect when running soak test")
	soakMinWaitSeconds         = flag.Int("soak-min-wait-seconds", 10, "Min wait time for data generation prior to starting migrator")
	soakDatagenWaitSeconds     = flag.Int("soak-datagen-wait-seconds", 60, "Max wait time for data generation prior to starting migrator")
	soakMigrationWaitSeconds   = flag.Int("soak-migration-wait-seconds", 30, "Max wait time after migrator starts")
	soakPostConsumeWaitSeconds = flag.Int("soak-post-consume-wait-seconds", 30, "Max wait time after consuming data")
)

// TestIntegrationMigratorSoak runs a long-running test of the migrator. It must
// be run with test flag -timeout to prevent it from timing out early. In
// case you want to change the wait times, you can use the flags above, just
// make sure to adjust the timeout accordingly. The standard way for running
// this test is:
//
// go test -count 100 -race -timeout 0 -run TestIntegrationMigratorSoak -v . \
// -soak-min-wait-seconds=20 -soak-datagen-wait-seconds=600 -soak-migration-wait-seconds=120 \
// -soak-post-consume-wait-seconds=60
//
// You can run resources/docker/profiling containers to get Metrics.
func TestIntegrationMigratorSoak(t *testing.T) {
	integration.CheckSkip(t)
	if os.Getenv("CI") != "" {
		t.Skip("Skipping soak test in CI")
	}

	ctx := t.Context()

	waitSecondsRand := func(seconds int) {
		d := time.Duration(*soakMinWaitSeconds+rand.Intn(seconds-*soakMinWaitSeconds)) * time.Second

		t.Logf(">> Waiting for %s", d)
		select {
		case <-ctx.Done():
		case <-time.After(d):
		}
		t.Log("<< Done waiting")
	}

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	t.Log("Given: Confluent CP cluster")
	src := startConfluentInPool(t, pool, true)

	t.Log("And: datagen connectors producing data")
	{
		pageviewsConf := map[string]any{
			"connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
			"key.converter":   "org.apache.kafka.connect.storage.StringConverter",
			"kafka.topic":     "pageviews",
			"quickstart":      "pageviews",
			"max.interval":    1000,
			"iterations":      10000000,
			"tasks.max":       "1",
		}
		require.NoError(t, createConnector(ctx, src.ConnectURL, "datagen_pageviews", pageviewsConf))

		usersConf := map[string]any{
			"connector.class": "io.confluent.kafka.connect.datagen.DatagenConnector",
			"key.converter":   "org.apache.kafka.connect.storage.StringConverter",
			"kafka.topic":     "users",
			"quickstart":      "users",
			"max.interval":    1000,
			"iterations":      10000000,
			"tasks.max":       "1",
		}
		require.NoError(t, createConnector(ctx, src.ConnectURL, "datagen_users", usersConf))
	}

	t.Log("And: Redpanda destination cluster")
	var dst EmbeddedRedpandaCluster
	{
		ep, _, err := redpandatest.StartSingleBrokerWithConfig(t, pool, redpandatest.Config{
			ExposeBroker:     true,
			AutoCreateTopics: false,
		})
		require.NoError(t, err)
		dst = EmbeddedRedpandaCluster{t: t, Endpoints: ep}
		dst.Client, err = kgo.NewClient(kgo.SeedBrokers(src.BrokerAddr))
		require.NoError(t, err)
		t.Cleanup(func() { src.Client.Close() })
		dst.Admin = src.Admin
	}

	t.Log("And: data generation period elapsed")
	waitSecondsRand(*soakDatagenWaitSeconds)

	t.Log("When: migrator is started")
	const configYAML = `
http:
  enabled: true
  address: {{.HTTPAddr}}

input:
  redpanda_migrator:
    seed_brokers: [ "{{.Src.BrokerAddr}}" ]
    topics:
      - "pageviews"
      - "users"
      - "docker-connect.*"
    regexp_topics: true
    consumer_group: migrator_bundle
    schema_registry:
      url: {{.Src.SchemaRegistryURL}}

output:
  redpanda_migrator:
    seed_brokers: [ "{{.Dst.BrokerAddr}}" ]
    schema_registry:
      url: {{.Dst.SchemaRegistryURL}}
    consumer_groups:
      interval: 10s

metrics:
  prometheus:
    add_process_metrics: true
    add_go_metrics: true

logger:
  level: INFO
`

	tmpl, err := template.New("soak").Parse(configYAML)
	require.NoError(t, err)

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, struct {
		HTTPAddr string
		Src      EmbeddedConfluentCluster
		Dst      EmbeddedRedpandaCluster
	}{
		HTTPAddr: *soakHTTPAddr,
		Src:      src,
		Dst:      dst,
	})
	require.NoError(t, err)

	sb := service.NewStreamBuilder()
	require.NoError(t, sb.SetYAML(buf.String()))
	stream, err := sb.Build()
	require.NoError(t, err)

	go func() {
		err := stream.Run(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	t.Cleanup(func() {
		t.Log("Stopping Migrator")
		require.NoError(t, stream.StopWithin(3*time.Second))
	})
	t.Logf("Migrator HTTP address: %s", *soakHTTPAddr)
	t.Log("And: migration period elapsed")
	waitSecondsRand(*soakMigrationWaitSeconds)

	t.Log("Then: topics match between source and destination")
	{
		assert.ElementsMatch(t, src.ListTopics(), dst.ListTopics())
	}

	t.Log("And: partitions match between source and destination")
	{
		srcPageviews := src.DescribeTopic("pageviews")
		dstPageviews := dst.DescribeTopic("pageviews")
		assert.Equal(t, srcPageviews.Partitions, dstPageviews.Partitions)
	}

	t.Log("When: consumer group offset is established on source")
	parseKey := func(s []byte) int {
		assert.NotEmpty(t, s)
		v, err := strconv.ParseInt(string(s), 10, 64)
		assert.NoError(t, err)
		return int(v)
	}

	consume(src.EmbeddedRedpandaCluster, "pageviews", "mygroup", 2, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	kafkaRecords := consume(src.EmbeddedRedpandaCluster, "pageviews", "mygroup", 1)
	kafkaKey := parseKey(kafkaRecords[0].Key)
	t.Logf("Kafka key: %d", kafkaKey)

	t.Log("And: post-consume period elapsed")
	waitSecondsRand(*soakPostConsumeWaitSeconds)

	t.Log("Then: consumer group offset is migrated correctly")
	redpandaRecords := consume(dst, "pageviews", "mygroup", 1)
	redpandaKey := parseKey(redpandaRecords[0].Key)
	t.Logf("Redpanda key: %d", redpandaKey)

	require.Equal(t, 10, redpandaKey-kafkaKey)
}
