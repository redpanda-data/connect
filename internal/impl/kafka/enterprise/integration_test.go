// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/protobuf/encoding/protojson"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka/enterprise"
	"github.com/redpanda-data/connect/v4/internal/impl/kafka/redpandatest"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/protoconnect"
	_ "github.com/redpanda-data/connect/v4/public/components/confluent"
)

func createKafkaTopic(ctx context.Context, address, topicName string, partitions int32) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(address))
	if err != nil {
		return err
	}
	defer cl.Close()

	createTopicsReq := kmsg.NewPtrCreateTopicsRequest()
	topicReq := kmsg.NewCreateTopicsRequestTopic()
	topicReq.NumPartitions = partitions
	topicReq.Topic = topicName
	topicReq.ReplicationFactor = 1
	createTopicsReq.Topics = append(createTopicsReq.Topics, topicReq)

	res, err := createTopicsReq.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	if len(res.Topics) != 1 {
		return fmt.Errorf("expected one topic in response, saw %d", len(res.Topics))
	}
	return kerr.ErrorForCode(res.Topics[0].ErrorCode)
}

func readNKafkaMessages(ctx context.Context, t testing.TB, address, topic string, nMessages int) (res []*kgo.Record) {
	t.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(address),
		kgo.ClientID("meow"),
		kgo.ConsumeTopics(topic),
	)
	require.NoError(t, err)

	defer cl.Close()

	for len(res) < nMessages {
		fetches := cl.PollRecords(ctx, nMessages-len(res))
		require.NoError(t, ctx.Err(), len(res))
		fetches.EachError(func(_ string, _ int32, err error) {
			t.Error(err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			res = append(res, r)
		})
	}
	return
}

func TestKafkaEnterpriseIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	container, err := redpandatest.StartRedpanda(t, pool, true, false)
	require.NoError(t, err)

	ctx, done := context.WithTimeout(t.Context(), time.Minute*3)
	defer done()

	t.Run("test_logs_happy", func(t *testing.T) {
		testLogsHappy(ctx, t, container.BrokerAddr)
	})

	t.Run("test_status_happy", func(t *testing.T) {
		testStatusHappy(ctx, t, container.BrokerAddr)
	})

	t.Run("test_logs_overrides", func(t *testing.T) {
		testLogsOverrides(ctx, t, container.BrokerAddr)
	})

	t.Run("test_logs_close_flush", func(t *testing.T) {
		testLogsCloseFlush(ctx, t, container.BrokerAddr)
	})
}

func testLogsHappy(ctx context.Context, t testing.TB, brokerAddr string) {
	logsTopic, statusTopic := "__testlogshappy.logs", "_testlogshappy.status"

	require.NoError(t, createKafkaTopic(ctx, brokerAddr, logsTopic, 1))
	require.NoError(t, createKafkaTopic(ctx, brokerAddr, statusTopic, 1))

	conf, err := service.NewConfigSpec().Fields(enterprise.GlobalRedpandaFields()...).ParseYAML(fmt.Sprintf(`
seed_brokers: [ %v ]
pipeline_id: bar
logs_topic: %v
logs_level: info
status_topic: %v
max_message_bytes: 1MB
`, brokerAddr, logsTopic, statusTopic), nil)
	require.NoError(t, err)

	license.InjectTestService(conf.Resources())

	gmgr := enterprise.NewGlobalRedpandaManager("foo")
	require.NoError(t, gmgr.InitFromParsedConfig(conf))

	inputLogs := 10

	tmpLogger := slog.New(gmgr.SlogHandler())
	for i := 0; i < inputLogs; i++ {
		tmpLogger.With("v", i).Info("This is a log message")
	}

	outRecords := readNKafkaMessages(ctx, t, brokerAddr, logsTopic, inputLogs)
	assert.Len(t, outRecords, inputLogs)

	for i, v := range outRecords {
		j := struct {
			PipelineID string `json:"pipeline_id"`
			InstanceID string `json:"instance_id"`
			Message    string `json:"message"`
			Level      string `json:"level"`
			V          string `json:"v"`
		}{}
		require.NoError(t, json.Unmarshal(v.Value, &j))
		assert.Equal(t, "foo", j.InstanceID)
		assert.Equal(t, "bar", j.PipelineID)
		assert.Equal(t, strconv.Itoa(i), j.V)
		assert.Equal(t, "INFO", j.Level)
		assert.Equal(t, "This is a log message", j.Message)
		assert.Equal(t, "bar", string(v.Key))
	}
}

func testLogsOverrides(ctx context.Context, t testing.TB, brokerAddr string) {
	logsTopicConf, statusTopicConf := "__testlogsnope.logs", "_testlogsnope.status"
	logsTopicOverride, statusTopicOverride := "__testlogsoverride.logs", "_testlogsoverride.status"
	topicCustom := "__testlogsoverrides.custom"

	require.NoError(t, createKafkaTopic(ctx, brokerAddr, logsTopicConf, 1))
	require.NoError(t, createKafkaTopic(ctx, brokerAddr, statusTopicConf, 1))
	require.NoError(t, createKafkaTopic(ctx, brokerAddr, logsTopicOverride, 1))
	require.NoError(t, createKafkaTopic(ctx, brokerAddr, statusTopicOverride, 1))
	require.NoError(t, createKafkaTopic(ctx, brokerAddr, topicCustom, 1))

	conf, err := service.NewConfigSpec().Fields(enterprise.GlobalRedpandaFields()...).ParseYAML(fmt.Sprintf(`
seed_brokers: [ %v ]
pipeline_id: bar
logs_topic: %v
logs_level: info
status_topic: %v
max_message_bytes: 1MB
`, brokerAddr, logsTopicConf, statusTopicConf), nil)
	require.NoError(t, err)

	license.InjectTestService(conf.Resources())

	gmgr := enterprise.NewGlobalRedpandaManager("foo")

	pConf, err := service.NewConfigSpec().
		Fields(kafka.FranzConnectionFields()...).
		ParseYAML(
			fmt.Sprintf(`seed_brokers: [ %v ]`, brokerAddr),
			nil,
		)
	require.NoError(t, err)

	cd, err := kafka.FranzConnectionDetailsFromConfig(pConf, conf.Resources().Logger())
	require.NoError(t, err)

	require.NoError(t, gmgr.InitWithCustomDetails("meowcustom", logsTopicOverride, statusTopicOverride, cd))
	require.NoError(t, gmgr.InitFromParsedConfig(conf))

	inputLogs := 10

	tmpLogger := slog.New(gmgr.SlogHandler())
	for i := 0; i < inputLogs; i++ {
		tmpLogger.With("v", i).Info("This is a log message")
	}

	outRecords := readNKafkaMessages(ctx, t, brokerAddr, logsTopicOverride, inputLogs)
	assert.Len(t, outRecords, inputLogs)

	for i, v := range outRecords {
		j := struct {
			PipelineID string `json:"pipeline_id"`
			InstanceID string `json:"instance_id"`
			Message    string `json:"message"`
			Level      string `json:"level"`
			V          string `json:"v"`
		}{}
		require.NoError(t, json.Unmarshal(v.Value, &j))
		assert.Equal(t, "foo", j.InstanceID)
		assert.Equal(t, "meowcustom", j.PipelineID)
		assert.Equal(t, strconv.Itoa(i), j.V)
		assert.Equal(t, "INFO", j.Level)
		assert.Equal(t, "This is a log message", j.Message)
		assert.Equal(t, "meowcustom", string(v.Key))
	}

	strmBuilder := service.NewStreamBuilder()

	require.NoError(t, strmBuilder.AddOutputYAML(fmt.Sprintf(`
redpanda_common:
  topic: %v
`, topicCustom)))

	require.NoError(t, strmBuilder.AddProcessorYAML(`
mapping: 'root = content().uppercase()'
`))

	prodFn, err := strmBuilder.AddProducerFunc()
	require.NoError(t, err)

	strm, err := strmBuilder.Build()
	require.NoError(t, err)

	// Ooooo, this is rather yucky.
	sharedRef, err := kafka.FranzSharedClientPop(enterprise.SharedGlobalRedpandaClientKey, conf.Resources())
	require.NoError(t, err)
	require.NoError(t, kafka.FranzSharedClientSet(enterprise.SharedGlobalRedpandaClientKey, sharedRef, strm.Resources()))

	license.InjectTestService(strm.Resources())

	go func() {
		assert.NoError(t, strm.Run(ctx))
	}()

	for i := 0; i < 10; i++ {
		require.NoError(t, prodFn(ctx, service.NewMessage(fmt.Appendf(nil, "Meow%v", i))))
	}

	outRecords = readNKafkaMessages(ctx, t, brokerAddr, topicCustom, 10)
	assert.Len(t, outRecords, inputLogs)

	for i := 0; i < 10; i++ {
		assert.Equal(t, fmt.Sprintf("MEOW%v", i), string(outRecords[i].Value))
	}

	require.NoError(t, strm.Stop(ctx))
}

func testLogsCloseFlush(ctx context.Context, t testing.TB, brokerAddr string) {
	logsTopic, statusTopic := "__testlogscloseflush.logs", "_testlogscloseflush.status"

	require.NoError(t, createKafkaTopic(ctx, brokerAddr, logsTopic, 1))
	require.NoError(t, createKafkaTopic(ctx, brokerAddr, statusTopic, 1))

	conf, err := service.NewConfigSpec().Fields(enterprise.GlobalRedpandaFields()...).ParseYAML(fmt.Sprintf(`
seed_brokers: [ %v ]
pipeline_id: bar
logs_topic: %v
logs_level: info
status_topic: %v
max_message_bytes: 1MB
`, brokerAddr, logsTopic, statusTopic), nil)
	require.NoError(t, err)

	license.InjectTestService(conf.Resources())

	gmgr := enterprise.NewGlobalRedpandaManager("foo")
	require.NoError(t, gmgr.InitFromParsedConfig(conf))

	inputLogs := 10

	tmpLogger := slog.New(gmgr.SlogHandler())
	for i := 0; i < inputLogs; i++ {
		tmpLogger.With("v", i).Info("This is a log message")
	}

	require.NoError(t, gmgr.Close(ctx))

	outRecords := readNKafkaMessages(ctx, t, brokerAddr, logsTopic, inputLogs)
	assert.Len(t, outRecords, inputLogs)

	for i, v := range outRecords {
		j := struct {
			PipelineID string `json:"pipeline_id"`
			InstanceID string `json:"instance_id"`
			Message    string `json:"message"`
			Level      string `json:"level"`
			V          string `json:"v"`
		}{}
		require.NoError(t, json.Unmarshal(v.Value, &j))
		assert.Equal(t, "foo", j.InstanceID)
		assert.Equal(t, "bar", j.PipelineID)
		assert.Equal(t, strconv.Itoa(i), j.V)
		assert.Equal(t, "INFO", j.Level)
		assert.Equal(t, "This is a log message", j.Message)
		assert.Equal(t, "bar", string(v.Key))
	}
}

func testStatusHappy(ctx context.Context, t testing.TB, brokerAddr string) {
	logsTopic, statusTopic := "__teststatushappy.logs", "_teststatushappy.status"

	require.NoError(t, createKafkaTopic(ctx, brokerAddr, logsTopic, 1))
	require.NoError(t, createKafkaTopic(ctx, brokerAddr, statusTopic, 1))

	conf, err := service.NewConfigSpec().Fields(enterprise.GlobalRedpandaFields()...).ParseYAML(fmt.Sprintf(`
seed_brokers: [ %v ]
pipeline_id: buz
logs_topic: %v
logs_level: info
status_topic: %v
max_message_bytes: 1MB
`, brokerAddr, logsTopic, statusTopic), nil)
	require.NoError(t, err)

	license.InjectTestService(conf.Resources())

	gmgr := enterprise.NewGlobalRedpandaManager("baz")
	require.NoError(t, gmgr.InitFromParsedConfig(conf))

	gmgr.TriggerEventStopped(errors.New("uh oh"))

	outRecords := readNKafkaMessages(ctx, t, brokerAddr, statusTopic, 2)
	assert.Len(t, outRecords, 2)

	var m protoconnect.StatusEvent

	require.NoError(t, protojson.Unmarshal(outRecords[0].Value, &m))
	assert.Equal(t, protoconnect.StatusEvent_TYPE_INITIALIZING, m.Type)
	assert.Equal(t, "baz", m.InstanceId)
	assert.Equal(t, "buz", m.PipelineId)
	assert.Equal(t, "buz", string(outRecords[0].Key))

	require.NoError(t, protojson.Unmarshal(outRecords[1].Value, &m))
	assert.Equal(t, protoconnect.StatusEvent_TYPE_EXITING, m.Type)
	assert.Equal(t, "uh oh", m.ExitError.Message)
	assert.Equal(t, "baz", m.InstanceId)
	assert.Equal(t, "buz", m.PipelineId)
	assert.Equal(t, "buz", string(outRecords[1].Key))
}
