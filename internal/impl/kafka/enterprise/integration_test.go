// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package enterprise_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/protobuf/encoding/protojson"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka/enterprise"
	"github.com/redpanda-data/connect/v4/internal/protoconnect"
)

func createKafkaTopic(ctx context.Context, address, id string, partitions int32) error {
	topicName := fmt.Sprintf("topic-%v", id)

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
		fetches.EachError(func(s string, i int32, err error) {
			t.Error(err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			res = append(res, r)
		})
	}
	return
}

func TestIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	kafkaPort, err := integration.GetFreePort()
	require.NoError(t, err)

	kafkaPortStr := strconv.Itoa(kafkaPort)
	options := &dockertest.RunOptions{
		Repository:   "redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{"9092"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: kafkaPortStr}},
		},
		Cmd: []string{
			"redpanda",
			"start",
			"--node-id 0",
			"--mode dev-container",
			"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
			"--kafka-addr 0.0.0.0:9092",
			fmt.Sprintf("--advertise-kafka-addr localhost:%v", kafkaPort),
		},
	}

	brokerAddr := "localhost:" + kafkaPortStr

	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	ctx, done := context.WithTimeout(context.Background(), time.Minute*3)
	defer done()

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return createKafkaTopic(ctx, brokerAddr, "testingconnection", 1)
	}))

	t.Run("test_logs_happy", func(t *testing.T) {
		testLogsHappy(ctx, t, brokerAddr)
	})

	t.Run("test_status_happy", func(t *testing.T) {
		testStatusHappy(ctx, t, brokerAddr)
	})
}

func testLogsHappy(ctx context.Context, t testing.TB, brokerAddr string) {
	logsTopic, statusTopic := "__testlogshappy.logs", "_testlogshappy.status"

	require.NoError(t, createKafkaTopic(ctx, brokerAddr, logsTopic, 1))
	require.NoError(t, createKafkaTopic(ctx, brokerAddr, statusTopic, 1))

	conf, err := service.NewConfigSpec().Fields(enterprise.TopicLoggerFields()...).ParseYAML(fmt.Sprintf(`
seed_brokers: [ %v ]
pipeline_id: bar
logs_topic: %v
logs_level: info
status_topic: %v
max_message_bytes: 1MB
`, brokerAddr, logsTopic, statusTopic), nil)
	require.NoError(t, err)

	logger := enterprise.NewTopicLogger("foo")
	require.NoError(t, logger.InitOutputFromParsed(conf))

	inputLogs := 10

	tmpLogger := slog.New(logger)
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

func testStatusHappy(ctx context.Context, t testing.TB, brokerAddr string) {
	logsTopic, statusTopic := "__teststatushappy.logs", "_teststatushappy.status"

	require.NoError(t, createKafkaTopic(ctx, brokerAddr, logsTopic, 1))
	require.NoError(t, createKafkaTopic(ctx, brokerAddr, statusTopic, 1))

	conf, err := service.NewConfigSpec().Fields(enterprise.TopicLoggerFields()...).ParseYAML(fmt.Sprintf(`
seed_brokers: [ %v ]
pipeline_id: buz
logs_topic: %v
logs_level: info
status_topic: %v
max_message_bytes: 1MB
`, brokerAddr, logsTopic, statusTopic), nil)
	require.NoError(t, err)

	logger := enterprise.NewTopicLogger("baz")
	require.NoError(t, logger.InitOutputFromParsed(conf))

	logger.TriggerEventStopped(errors.New("uh oh"))

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

func TestSchemaRegistryIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	tests := []struct {
		name                       string
		schema                     string
		includeSoftDeletedSubjects bool
		subjectFilter              string
	}{
		{
			name:   "roundtrip",
			schema: `{"name":"foo", "type": "string"}`,
		},
		{
			name:                       "roundtrip with deleted subject",
			schema:                     `{"name":"foo", "type": "string"}`,
			includeSoftDeletedSubjects: true,
		},
		{
			name:          "roundtrip with subject filter",
			schema:        `{"name":"foo", "type": "string"}`,
			subjectFilter: `\w+-\w+-\w+-\w+-\w+`,
		},
	}

	sourcePort := startSchemaRegistry(t, pool)
	sinkPort := startSchemaRegistry(t, pool)

	cleanupSubject := func(port, subject string, hardDelete bool) {
		u, err := url.Parse(fmt.Sprintf("http://localhost:%s/subjects/%s", port, subject))
		require.NoError(t, err)
		if hardDelete {
			q := u.Query()
			q.Add("permanent", "true")
			u.RawQuery = q.Encode()
		}
		req, err := http.NewRequest(http.MethodDelete, u.String(), nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			u4, err := uuid.NewV4()
			require.NoError(t, err)
			subject := u4.String()
			extraSubject := "foobar"

			t.Cleanup(func() {
				cleanupSubject(sourcePort, subject, false)
				cleanupSubject(sourcePort, subject, true)
				cleanupSubject(sinkPort, subject, false)
				cleanupSubject(sinkPort, subject, true)
			})

			postContentType := "application/vnd.schemaregistry.v1+json"
			type payload struct {
				Subject string `json:"subject,omitempty"`
				Version int    `json:"version,omitempty"`
				Schema  string `json:"schema"`
			}
			body, err := json.Marshal(payload{Schema: test.schema})
			require.NoError(t, err)
			req, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost:%s/subjects/%s/versions", sourcePort, subject), bytes.NewReader(body))
			require.NoError(t, err)
			req.Header.Set("Content-Type", postContentType)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())

			if test.subjectFilter != "" {
				resp, err = http.DefaultClient.Post(fmt.Sprintf("http://localhost:%s/subjects/%s/versions", sourcePort, extraSubject), postContentType, bytes.NewReader(body))
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
			}

			if test.includeSoftDeletedSubjects {
				req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("http://localhost:%s/subjects/%s", sourcePort, subject), nil)
				require.NoError(t, err)
				resp, err = http.DefaultClient.Do(req)
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
			}

			streamBuilder := service.NewStreamBuilder()
			require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  schema_registry:
    url: http://localhost:%s
    include_deleted: %t
    subject_filter: %s
output:
  schema_registry:
    url: http://localhost:%s
    subject: ${! @schema_registry_subject }
`, sourcePort, test.includeSoftDeletedSubjects, test.subjectFilter, sinkPort)))
			require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))

			stream, err := streamBuilder.Build()
			require.NoError(t, err)

			ctx, done := context.WithTimeout(context.Background(), 3*time.Second)
			defer done()

			require.NoError(t, stream.Run(ctx))

			resp, err = http.DefaultClient.Get(fmt.Sprintf("http://localhost:%s/subjects", sinkPort))
			require.NoError(t, err)
			body, err = io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)
			if test.subjectFilter != "" {
				assert.Contains(t, string(body), subject)
				assert.NotContains(t, string(body), extraSubject)
			}

			resp, err = http.DefaultClient.Get(fmt.Sprintf("http://localhost:%s/subjects/%s/versions/1", sinkPort, subject))
			require.NoError(t, err)
			body, err = io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var p payload
			require.NoError(t, json.Unmarshal(body, &p))
			assert.Equal(t, subject, p.Subject)
			assert.Equal(t, 1, p.Version)
			assert.JSONEq(t, test.schema, p.Schema)
		})
	}
}

func startSchemaRegistry(t *testing.T, pool *dockertest.Pool) string {
	// TODO: Generalise this helper for the other Kafka tests here which use Redpanda...
	t.Helper()

	options := &dockertest.RunOptions{
		Repository:   "redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		ExposedPorts: []string{"8081"},
		Cmd: []string{
			"redpanda",
			"start",
			"--node-id 0",
			"--mode dev-container",
			"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
			"--schema-registry-addr 0.0.0.0:8081",
		},
	}

	pool.MaxWait = time.Minute
	resource, err := pool.RunWithOptions(options)
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	port := resource.GetPort("8081/tcp")

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		ctx, done := context.WithTimeout(context.Background(), 3*time.Second)
		defer done()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://localhost:%s/subjects", port), nil)
		if err != nil {
			return err
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return errors.New("invalid status")
		}

		return nil
	}))

	return port
}
