package integration

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Shopify/sarama"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
)

func createKafkaTopic(address, id string, partitions int32) error {
	topicName := fmt.Sprintf("topic-%v", id)

	b := sarama.NewBroker(address)
	defer b.Close()

	if err := b.Open(sarama.NewConfig()); err != nil {
		return err
	}

	req := &sarama.CreateTopicsRequest{
		TopicDetails: map[string]*sarama.TopicDetail{
			topicName: {
				NumPartitions:     partitions,
				ReplicationFactor: 1,
			},
		},
	}

	res, err := b.CreateTopics(req)
	if err != nil {
		return err
	}
	if len(res.TopicErrors) > 0 {
		if errStr := res.TopicErrors[topicName].ErrMsg; errStr != nil {
			return errors.New(*errStr)
		}
	}

	var meta *sarama.MetadataResponse
	for i := 0; i < 20; i++ {
		meta, err = b.GetMetadata(&sarama.MetadataRequest{
			Topics: []string{topicName},
		})
		if err == nil && len(meta.Topics) == 1 && len(meta.Topics[0].Partitions) == 4 {
			break
		}
		<-time.After(time.Millisecond * 100)
	}
	if err != nil {
		return err
	}
	if len(meta.Topics) == 0 || len(meta.Topics[0].Partitions) != 4 {
		return fmt.Errorf("failed to create topic: %v", topicName)
	}

	return nil
}

var _ = registerIntegrationTest("kafka", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute

	networks, _ := pool.Client.ListNetworks()
	hostIP := ""
	for _, network := range networks {
		if network.Name == "bridge" {
			hostIP = network.IPAM.Config[0].Gateway
		}
	}

	zkResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "wurstmeister/zookeeper",
		Tag:        "latest",
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pool.Purge(zkResource))
	})
	zkResource.Expire(900)
	zkAddr := fmt.Sprintf("%v:2181", zkResource.Container.NetworkSettings.IPAddress)

	kafkaPort, err := getFreePort()
	require.NoError(t, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)
	env := []string{
		"KAFKA_ADVERTISED_HOST_NAME=" + hostIP,
		"KAFKA_BROKER_ID=1",
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=OUTSIDE:PLAINTEXT,INSIDE:PLAINTEXT",
		"KAFKA_LISTENERS=OUTSIDE://:" + kafkaPortStr + ",INSIDE://:9092",
		"KAFKA_ADVERTISED_LISTENERS=OUTSIDE://" + hostIP + ":" + kafkaPortStr + ",INSIDE://:9092",
		"KAFKA_INTER_BROKER_LISTENER_NAME=INSIDE",
		"KAFKA_ZOOKEEPER_CONNECT=" + zkAddr,
	}

	kafkaResource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "wurstmeister/kafka",
		Tag:          "latest",
		ExposedPorts: []string{kafkaPortStr + "/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			docker.Port(kafkaPortStr + "/tcp"): {{HostIP: "", HostPort: kafkaPortStr}},
		},
		Env: env,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pool.Purge(kafkaResource))
	})
	kafkaResource.Expire(900)

	address := fmt.Sprintf("%v:%v", hostIP, kafkaPortStr)

	require.NoError(t, pool.Retry(func() error {
		outConf := writer.NewKafkaConfig()
		outConf.TargetVersion = "2.1.0"
		outConf.Addresses = []string{address}
		outConf.Topic = "pls_ignore_just_testing_connection"
		tmpOutput, serr := writer.NewKafka(outConf, types.NoopMgr(), log.Noop(), metrics.Noop())
		if serr != nil {
			return serr
		}
		defer tmpOutput.CloseAsync()
		if serr = tmpOutput.Connect(); serr != nil {
			return serr
		}
		return tmpOutput.Write(message.New([][]byte{
			[]byte("foo message"),
		}))
	}))

	template := fmt.Sprintf(`
output:
  kafka:
    addresses: [ %v ]
    topic: topic-$ID
    max_in_flight: $MAX_IN_FLIGHT
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  kafka:
    addresses: [ %v ]
    topics: [ topic-$ID$VAR1 ]
    consumer_group: consumer-$ID
    checkpoint_limit: $VAR2
    batching:
      count: $INPUT_BATCH_COUNT
`, address, address)

	suite := integrationTests(
		integrationTestOpenClose(),
		integrationTestMetadata(),
		integrationTestSendBatch(10),
		integrationTestStreamSequential(1000),
		integrationTestStreamParallel(1000),
		integrationTestStreamParallelLossy(1000),
		integrationTestSendBatchCount(10),
	)
	// In some tests include testing input level batching
	suiteExt := append(suite, integrationTestReceiveBatchCount(10))

	t.Run("balanced", func(t *testing.T) {
		t.Parallel()
		suiteExt.Run(
			t, template,
			testOptVarOne(""),
			testOptVarTwo("1"),
		)

		t.Run("checkpointed", func(t *testing.T) {
			t.Parallel()
			suiteExt.Run(
				t, template,
				testOptVarOne(""),
				testOptVarTwo("1000"),
			)
		})

		t.Run("with four partitions", func(t *testing.T) {
			t.Parallel()
			suite.Run(
				t, template,
				testOptPreTest(func(t *testing.T, env *testEnvironment) {
					require.NoError(t, createKafkaTopic(address, env.configVars.id, 4))
				}),
				testOptVarOne(""),
				testOptVarTwo("1"),
			)

			t.Run("checkpointed", func(t *testing.T) {
				t.Parallel()
				suite.Run(
					t, template,
					testOptPreTest(func(t *testing.T, env *testEnvironment) {
						require.NoError(t, createKafkaTopic(address, env.configVars.id, 4))
					}),
					testOptVarOne(""),
					testOptVarTwo("1000"),
				)
			})
		})
	})

	t.Run("partitions", func(t *testing.T) {
		t.Parallel()
		suiteExt.Run(
			t, template,
			testOptVarOne(":0"),
			testOptVarTwo("1"),
		)

		t.Run("checkpointed", func(t *testing.T) {
			t.Parallel()
			suiteExt.Run(
				t, template,
				testOptVarOne(":0"),
				testOptVarTwo("1000"),
			)
		})

		t.Run("with four partitions", func(t *testing.T) {
			t.Parallel()
			suite.Run(
				t, template,
				testOptPreTest(func(t *testing.T, env *testEnvironment) {
					topicName := "topic-" + env.configVars.id
					env.configVars.var1 = fmt.Sprintf(":0,%v:1,%v:2,%v:3", topicName, topicName, topicName)
					require.NoError(t, createKafkaTopic(address, env.configVars.id, 4))
				}),
				testOptSleepAfterInput(time.Second*3),
				testOptVarTwo("1"),
			)

			t.Run("checkpointed", func(t *testing.T) {
				t.Parallel()
				suite.Run(
					t, template,
					testOptPreTest(func(t *testing.T, env *testEnvironment) {
						topicName := "topic-" + env.configVars.id
						env.configVars.var1 = fmt.Sprintf(":0,%v:1,%v:2,%v:3", topicName, topicName, topicName)
						require.NoError(t, createKafkaTopic(address, env.configVars.id, 4))
					}),
					testOptSleepAfterInput(time.Second*3),
					testOptVarTwo("1000"),
				)
			})
		})
	})
})
