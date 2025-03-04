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
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	franz_sr "github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/encoding/protojson"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/kafka/enterprise"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/protoconnect"
	_ "github.com/redpanda-data/connect/v4/public/components/confluent"
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

func TestKafkaEnterpriseIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	container, err := startRedpanda(t, pool, true, true)
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Minute*3)
	defer done()

	t.Run("test_logs_happy", func(t *testing.T) {
		testLogsHappy(ctx, t, container.brokerAddr)
	})

	t.Run("test_status_happy", func(t *testing.T) {
		testStatusHappy(ctx, t, container.brokerAddr)
	})

	t.Run("test_logs_close_flush", func(t *testing.T) {
		testLogsCloseFlush(ctx, t, container.brokerAddr)
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

	license.InjectTestService(conf.Resources())

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

func testLogsCloseFlush(ctx context.Context, t testing.TB, brokerAddr string) {
	logsTopic, statusTopic := "__testlogscloseflush.logs", "_testlogscloseflush.status"

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

	license.InjectTestService(conf.Resources())

	logger := enterprise.NewTopicLogger("foo")
	require.NoError(t, logger.InitOutputFromParsed(conf))

	inputLogs := 10

	tmpLogger := slog.New(logger)
	for i := 0; i < inputLogs; i++ {
		tmpLogger.With("v", i).Info("This is a log message")
	}

	require.NoError(t, logger.Close(ctx))

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

	license.InjectTestService(conf.Resources())

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

func createSchema(t *testing.T, url string, subject string, schema string, references []franz_sr.SchemaReference) {
	t.Helper()

	client, err := franz_sr.NewClient(franz_sr.URLs(url))
	require.NoError(t, err)

	_, err = client.CreateSchema(context.Background(), subject, franz_sr.Schema{Schema: schema, References: references})
	require.NoError(t, err)
}

func deleteSubject(t *testing.T, url string, subject string, hardDelete bool) {
	t.Helper()

	client, err := franz_sr.NewClient(franz_sr.URLs(url))
	require.NoError(t, err)

	deleteMode := franz_sr.SoftDelete
	if hardDelete {
		deleteMode = franz_sr.HardDelete
	}

	_, err = client.DeleteSubject(context.Background(), subject, deleteMode)
	require.NoError(t, err)
}

func TestSchemaRegistryIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	tests := []struct {
		name                       string
		schema                     string
		includeSoftDeletedSubjects bool
		extraSubject               string
		subjectFilter              string
		schemaWithReference        string
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
			extraSubject:  "foobar",
			subjectFilter: `^\w+-\w+-\w+-\w+-\w+$`,
		},
		{
			name:   "roundtrip with schema references",
			schema: `{"name":"foo", "type": "string"}`,
			// A UUID which always gets picked first when querying the `/subjects` endpoint.
			extraSubject:        "ffffffff-ffff-ffff-ffff-ffffffffffff",
			schemaWithReference: `{"name":"bar", "type": "record", "fields":[{"name":"data", "type": "foo"}]}`,
		},
	}

	source, err := startRedpanda(t, pool, false, true)
	require.NoError(t, err)
	destination, err := startRedpanda(t, pool, false, true)
	require.NoError(t, err)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			u4, err := uuid.NewV4()
			require.NoError(t, err)
			subject := u4.String()

			t.Cleanup(func() {
				// Clean up the extraSubject first since it may contain schemas with references.
				if test.extraSubject != "" {
					deleteSubject(t, source.schemaRegistryURL, test.extraSubject, false)
					deleteSubject(t, source.schemaRegistryURL, test.extraSubject, true)
					if test.subjectFilter == "" {
						deleteSubject(t, destination.schemaRegistryURL, test.extraSubject, false)
						deleteSubject(t, destination.schemaRegistryURL, test.extraSubject, true)
					}
				}

				if !test.includeSoftDeletedSubjects {
					deleteSubject(t, source.schemaRegistryURL, subject, false)
				}
				deleteSubject(t, source.schemaRegistryURL, subject, true)

				deleteSubject(t, destination.schemaRegistryURL, subject, false)
				deleteSubject(t, destination.schemaRegistryURL, subject, true)
			})

			createSchema(t, source.schemaRegistryURL, subject, test.schema, nil)

			if test.subjectFilter != "" {
				createSchema(t, source.schemaRegistryURL, test.extraSubject, test.schema, nil)
			}

			if test.includeSoftDeletedSubjects {
				deleteSubject(t, source.schemaRegistryURL, subject, false)
			}

			if test.schemaWithReference != "" {
				createSchema(t, source.schemaRegistryURL, test.extraSubject, test.schemaWithReference, []franz_sr.SchemaReference{{Name: "foo", Subject: subject, Version: 1}})
			}

			streamBuilder := service.NewStreamBuilder()
			require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  schema_registry:
    url: %s
    include_deleted: %t
    subject_filter: %s
    fetch_in_order: %t
output:
  fallback:
    - schema_registry:
        url: %s
        subject: ${! @schema_registry_subject }
        # Preserve schema order.
        max_in_flight: 1
    # Don't retry the same message multiple times so we do fail if schemas with references are sent in the wrong order
    - drop: {}
`, source.schemaRegistryURL, test.includeSoftDeletedSubjects, test.subjectFilter, test.schemaWithReference != "", destination.schemaRegistryURL)))
			require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))

			stream, err := streamBuilder.Build()
			require.NoError(t, err)

			license.InjectTestService(stream.Resources())

			ctx, done := context.WithTimeout(context.Background(), 3*time.Second)
			defer done()

			err = stream.Run(ctx)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Get(fmt.Sprintf("%s/subjects", destination.schemaRegistryURL))
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)
			if test.subjectFilter != "" {
				assert.Contains(t, string(body), subject)
				assert.NotContains(t, string(body), test.extraSubject)
			}

			resp, err = http.DefaultClient.Get(fmt.Sprintf("%s/subjects/%s/versions/1", destination.schemaRegistryURL, subject))
			require.NoError(t, err)
			body, err = io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NoError(t, resp.Body.Close())
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var sd franz_sr.SubjectSchema
			require.NoError(t, json.Unmarshal(body, &sd))
			assert.Equal(t, subject, sd.Subject)
			assert.Equal(t, 1, sd.Version)
			assert.JSONEq(t, test.schema, sd.Schema.Schema)

			if test.schemaWithReference != "" {
				resp, err = http.DefaultClient.Get(fmt.Sprintf("%s/subjects/%s/versions/1", destination.schemaRegistryURL, test.extraSubject))
				require.NoError(t, err)
				body, err = io.ReadAll(resp.Body)
				require.NoError(t, err)
				require.NoError(t, resp.Body.Close())
				require.Equal(t, http.StatusOK, resp.StatusCode)

				var sd franz_sr.SubjectSchema
				require.NoError(t, json.Unmarshal(body, &sd))
				assert.Equal(t, test.extraSubject, sd.Subject)
				assert.Equal(t, 1, sd.Version)
				assert.JSONEq(t, test.schemaWithReference, sd.Schema.Schema)
			}
		})
	}
}

func TestSchemaRegistryIDTranslationIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	source, err := startRedpanda(t, pool, false, true)
	require.NoError(t, err)
	destination, err := startRedpanda(t, pool, false, true)
	require.NoError(t, err)

	// Create two schemas under subject `foo`.
	createSchema(t, source.schemaRegistryURL, "foo", `{"name":"foo", "type": "record", "fields":[{"name":"str", "type": "string"}]}`, nil)
	createSchema(t, source.schemaRegistryURL, "foo", `{"name":"foo", "type": "record", "fields":[{"name":"str", "type": "string"}, {"name":"num", "type": "int", "default": 42}]}`, nil)

	// Create a schema under subject `bar` which references the second schema under `foo`.
	createSchema(t, source.schemaRegistryURL, "bar", `{"name":"bar", "type": "record", "fields":[{"name":"data", "type": "foo"}]}`,
		[]franz_sr.SchemaReference{{Name: "foo", Subject: "foo", Version: 2}},
	)

	// Create a schema at the destination which will have ID 1 so we can check that the ID translation works
	// correctly.
	createSchema(t, destination.schemaRegistryURL, "baz", `{"name":"baz", "type": "record", "fields":[{"name":"num", "type": "int"}]}`, nil)

	// Use a Stream with a mapping filter to send only the schema with the reference to the destination in order
	// to force the output to backfill the rest of the schemas.
	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  schema_registry:
    url: %s
  processors:
    - mapping: |
        if this.id != 3 { root = deleted() }
output:
  fallback:
    - schema_registry:
        url: %s
        subject: ${! @schema_registry_subject }
        # Preserve schema order.
        max_in_flight: 1
    # Don't retry the same message multiple times so we do fail if schemas with references are sent in the wrong order
    - drop: {}
`, source.schemaRegistryURL, destination.schemaRegistryURL)))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	license.InjectTestService(stream.Resources())

	ctx, done := context.WithTimeout(context.Background(), 3*time.Second)
	defer done()

	err = stream.Run(ctx)
	require.NoError(t, err)

	// Check that the schemas were backfilled correctly.
	tests := []struct {
		subject            string
		version            int
		expectedID         int
		expectedReferences []franz_sr.SchemaReference
	}{
		{
			subject:    "foo",
			version:    1,
			expectedID: 2,
		},
		{
			subject:    "foo",
			version:    2,
			expectedID: 3,
		},
		{
			subject:            "bar",
			version:            1,
			expectedID:         4,
			expectedReferences: []franz_sr.SchemaReference{{Name: "foo", Subject: "foo", Version: 2}},
		},
	}

	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			resp, err := http.DefaultClient.Get(fmt.Sprintf("%s/subjects/%s/versions/%d", destination.schemaRegistryURL, test.subject, test.version))
			require.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			var sd franz_sr.SubjectSchema
			require.NoError(t, json.Unmarshal(body, &sd))
			require.NoError(t, resp.Body.Close())

			assert.Equal(t, test.expectedID, sd.ID)
			assert.Equal(t, test.expectedReferences, sd.References)
		})
	}
}

type redpandaEndpoints struct {
	brokerAddr        string
	schemaRegistryURL string
}

// TODO: Generalise this helper for the other Kafka tests here which use Redpanda.
func startRedpanda(t *testing.T, pool *dockertest.Pool, exposeBroker bool, autocreateTopics bool) (redpandaEndpoints, error) {
	t.Helper()

	cmd := []string{
		"redpanda",
		"start",
		"--node-id 0",
		"--mode dev-container",
		"--set rpk.additional_start_flags=[--reactor-backend=epoll]",
		"--schema-registry-addr 0.0.0.0:8081",
	}

	if !autocreateTopics {
		cmd = append(cmd, "--set redpanda.auto_create_topics_enabled=false")
	}

	// Expose Schema Registry and Admin API by default. The Admin API is required for health checks.
	exposedPorts := []string{"8081/tcp", "9644/tcp"}
	var portBindings map[docker.Port][]docker.PortBinding
	var kafkaPort string
	if exposeBroker {
		brokerPort, err := integration.GetFreePort()
		if err != nil {
			return redpandaEndpoints{}, fmt.Errorf("failed to start container: %s", err)
		}

		// Note: Schema Registry uses `--advertise-kafka-addr` to talk to the broker, so we need to use the same port for `--kafka-addr`.
		// TODO: Ensure we don't stomp over some ports which are already in use inside the container.
		cmd = append(cmd, fmt.Sprintf("--kafka-addr 0.0.0.0:%d", brokerPort), fmt.Sprintf("--advertise-kafka-addr localhost:%d", brokerPort))

		kafkaPort = fmt.Sprintf("%d/tcp", brokerPort)
		exposedPorts = append(exposedPorts, kafkaPort)
		portBindings = map[docker.Port][]docker.PortBinding{docker.Port(kafkaPort): {{HostPort: kafkaPort}}}
	}

	options := &dockertest.RunOptions{
		Repository:   "docker.redpanda.com/redpandadata/redpanda",
		Tag:          "latest",
		Hostname:     "redpanda",
		Cmd:          cmd,
		ExposedPorts: exposedPorts,
		PortBindings: portBindings,
	}

	resource, err := pool.RunWithOptions(options)
	if err != nil {
		return redpandaEndpoints{}, fmt.Errorf("failed to start container: %s", err)
	}

	if err := resource.Expire(900); err != nil {
		return redpandaEndpoints{}, fmt.Errorf("failed to set container expiry period: %s", err)
	}

	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	require.NoError(t, pool.Retry(func() error {
		ctx, done := context.WithTimeout(context.Background(), 3*time.Second)
		defer done()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://localhost:%s/v1/cluster/health_overview", resource.GetPort("9644/tcp")), nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %s", err)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return fmt.Errorf("failed to execute request: %s", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return errors.New("invalid status")
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response body: %s", err)
		}

		var res struct {
			IsHealthy bool `json:"is_healthy"`
		}

		if err := json.Unmarshal(body, &res); err != nil {
			return fmt.Errorf("failed to unmarshal response body: %s", err)
		}

		if !res.IsHealthy {
			return errors.New("unhealthy")
		}

		return nil
	}))

	return redpandaEndpoints{
		brokerAddr:        fmt.Sprintf("localhost:%s", resource.GetPort(kafkaPort)),
		schemaRegistryURL: fmt.Sprintf("http://localhost:%s", resource.GetPort("8081/tcp")),
	}, nil
}

func createTopicWithACLs(t *testing.T, brokerAddr, topic, retentionTime, principal string, operation kadm.ACLOperation) {
	client, err := kgo.NewClient(kgo.SeedBrokers([]string{brokerAddr}...))
	require.NoError(t, err)
	defer client.Close()

	adm := kadm.NewClient(client)

	configs := map[string]*string{"retention.ms": &retentionTime}
	_, err = adm.CreateTopic(context.Background(), 1, -1, configs, topic)
	require.NoError(t, err)

	updateTopicACL(t, adm, topic, principal, operation)
}

func updateTopicACL(t *testing.T, client *kadm.Client, topic, principal string, operation kadm.ACLOperation) {
	builder := kadm.NewACLs().Allow(principal).AllowHosts("*").Topics(topic).ResourcePatternType(kadm.ACLPatternLiteral).Operations(operation)
	res, err := client.CreateACLs(context.Background(), builder)
	require.NoError(t, err)
	require.Len(t, res, 1)
	assert.NoError(t, res[0].Err)
}

func checkTopic(t *testing.T, brokerAddr, topic, retentionTime, principal string, operation kadm.ACLOperation) {
	client, err := kgo.NewClient(kgo.SeedBrokers([]string{brokerAddr}...))
	require.NoError(t, err)
	defer client.Close()

	adm := kadm.NewClient(client)

	topicConfigs, err := adm.DescribeTopicConfigs(context.Background(), topic)
	require.NoError(t, err)

	rc, err := topicConfigs.On(topic, nil)
	require.NoError(t, err)
	assert.Condition(t, func() bool {
		for _, c := range rc.Configs {
			if c.Key == "retention.ms" && *c.Value == retentionTime {
				return true
			}
		}
		return false
	})

	builder := kadm.NewACLs().Topics(topic).
		ResourcePatternType(kadm.ACLPatternLiteral).Operations(operation).Allow().Deny().AllowHosts().DenyHosts()

	aclResults, err := adm.DescribeACLs(context.Background(), builder)
	require.NoError(t, err)
	require.Len(t, aclResults[0].Described, 1)
	require.NoError(t, aclResults[0].Err)
	require.Len(t, aclResults[0].Described, 1)

	for _, acl := range aclResults[0].Described {
		assert.Equal(t, principal, acl.Principal)
		assert.Equal(t, "*", acl.Host)
		assert.Equal(t, topic, acl.Name)
		assert.Equal(t, operation.String(), acl.Operation.String())
	}
}

// produceMessages produces `count` messages to the given `topic` with the given `message` content. The
// `timestampOffset` indicates an offset which gets added to the `counter()` Bloblang function which is used to generate
// the message timestamps sequentially, the first one being `1 + timestampOffset`.
func produceMessages(t *testing.T, rpe redpandaEndpoints, topic, message string, timestampOffset, count int, encode bool) {
	streamBuilder := service.NewStreamBuilder()
	config := ""
	if encode {
		config = fmt.Sprintf(`
pipeline:
  processors:
    - schema_registry_encode:
        url: %s
        subject: %s
        avro_raw_json: true
`, rpe.schemaRegistryURL, topic)
	}
	config += fmt.Sprintf(`
output:
  kafka_franz:
    seed_brokers: [ %s ]
    topic: %s
    timestamp_ms: ${! counter() + %d}
`, rpe.brokerAddr, topic, timestampOffset)
	require.NoError(t, streamBuilder.SetYAML(config))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))

	inFunc, err := streamBuilder.AddProducerFunc()
	require.NoError(t, err)

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	license.InjectTestService(stream.Resources())

	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(done)

	go func() {
		for range count {
			require.NoError(t, inFunc(ctx, service.NewMessage([]byte(message))))
		}

		require.NoError(t, stream.StopWithin(3*time.Second))
	}()

	err = stream.Run(ctx)
	require.NoError(t, err)
}

// readMessagesWithCG reads `count` messages from the given `topic` with the given `consumerGroup`.
// TODO: Since `read_until` can't guarantee that we will read `count` messages, `topic` needs to have exactly `count`
// messages in it. We should add some mechanism to the Kafka inputs to allow us to read a range of offsets if possible
// (or up to a certain offset).
func readMessagesWithCG(t *testing.T, rpe redpandaEndpoints, topic, consumerGroup, message string, count int, decode bool) {
	streamBuilder := service.NewStreamBuilder()
	config := fmt.Sprintf(`
input:
  kafka_franz:
    seed_brokers: [ %s ]
    topics: [ %s ]
    consumer_group: %s
    start_from_oldest: true
`, rpe.brokerAddr, topic, consumerGroup)
	if decode {
		config += fmt.Sprintf(`
  processors:
    - schema_registry_decode:
        url: %s
        avro_raw_json: true
`, rpe.schemaRegistryURL)
	}
	config += `
output:
  # Need to use drop explicitly with SetYAML(). Otherwise, the output will be inproc
  # (or stdout if we import github.com/redpanda-data/benthos/v4/public/components/io)
  drop: {}
`
	require.NoError(t, streamBuilder.SetYAML(config))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: OFF`))

	recvMsgWG := sync.WaitGroup{}
	recvMsgWG.Add(count)
	err := streamBuilder.AddConsumerFunc(func(ctx context.Context, m *service.Message) error {
		defer recvMsgWG.Done()
		b, err := m.AsBytes()
		require.NoError(t, err)

		assert.Equal(t, message, string(b))

		return nil
	})
	require.NoError(t, err)

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	license.InjectTestService(stream.Resources())

	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(done)

	go func() {
		require.NoError(t, stream.Run(ctx))
	}()

	recvMsgWG.Wait()

	require.NoError(t, stream.StopWithin(3*time.Second))
}

func runMigratorBundle(t *testing.T, source, destination redpandaEndpoints, topic string, callback func(*service.Message)) {
	streamBuilder := service.NewStreamBuilder()
	require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  redpanda_migrator_bundle:
    redpanda_migrator:
      seed_brokers: [ %s ]
      topics: [ %s ]
      consumer_group: migrator_cg
      start_from_oldest: true
    schema_registry:
      url: %s
  processors:
    - switch:
        - check: '@input_label == "redpanda_migrator_offsets_input"'
          processors:
            - log:
                message: Migrating Kafka offset
                fields:
                  kafka_offset_topic:            ${! @kafka_offset_topic }
                  kafka_offset_group:            ${! @kafka_offset_group }
                  kafka_offset_partition:        ${! @kafka_offset_partition }
                  kafka_offset_commit_timestamp: ${! @kafka_offset_commit_timestamp }
                  kafka_offset_metadata:         ${! @kafka_offset_metadata }
                  kafka_offset:                  ${! @kafka_offset } # This is just the offset of the __consumer_offsets topic
        - check: '@input_label == "redpanda_migrator_input"'
          processors:
            - branch:
                processors:
                  - schema_registry_decode:
                      url: %s
                      avro_raw_json: true
                  - log:
                      message: 'Migrating Kafka message: ${! content() }'
        - check: '@input_label == "schema_registry_input"'
          processors:
            - branch:
                processors:
                  - log:
                      message: 'Migrating Schema Registry schema: ${! content() }'

output:
  redpanda_migrator_bundle:
    redpanda_migrator:
      seed_brokers: [ %s ]
      replication_factor_override: true
      replication_factor: -1
    schema_registry:
      url: %s
`, source.brokerAddr, topic, source.schemaRegistryURL, source.schemaRegistryURL, destination.brokerAddr, destination.schemaRegistryURL)))
	require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))

	require.NoError(t, streamBuilder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
		callback(m)
		return nil
	}))

	// Ensure the callback function is called after the output wrote the message
	streamBuilder.SetOutputBrokerPattern(service.OutputBrokerPatternFanOutSequential)

	stream, err := streamBuilder.Build()
	require.NoError(t, err)

	license.InjectTestService(stream.Resources())

	// Run stream in the background and shut it down when the test is finished
	closeChan := make(chan struct{})
	go func() {
		err = stream.Run(context.Background())
		require.NoError(t, err)

		t.Log("Migrator pipeline shut down")

		close(closeChan)
	}()
	t.Cleanup(func() {
		require.NoError(t, stream.StopWithin(3*time.Second))

		<-closeChan
	})
}

func TestRedpandaMigratorIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	source, err := startRedpanda(t, pool, true, true)
	require.NoError(t, err)
	destination, err := startRedpanda(t, pool, true, false)
	require.NoError(t, err)

	t.Logf("Source broker: %s", source.brokerAddr)
	t.Logf("Destination broker: %s", destination.brokerAddr)

	dummyTopic := "test"

	// Create a schema associated with the test topic
	createSchema(t, source.schemaRegistryURL, dummyTopic, fmt.Sprintf(`{"name":"%s", "type": "record", "fields":[{"name":"test", "type": "string"}]}`, dummyTopic), nil)

	// Produce one message
	dummyMessage := `{"test":"foo"}`
	produceMessages(t, source, dummyTopic, dummyMessage, 0, 1, true)
	t.Log("Finished producing first message in source")

	// Run the Redpanda Migrator bundle
	msgChan := make(chan *service.Message)
	checkMigrated := func(label string, validate func(string, map[string]string)) {
	loop:
		for {
			select {
			case m := <-msgChan:
				l, ok := m.MetaGet("input_label")
				require.True(t, ok)
				if l != label {
					goto loop
				}

				b, err := m.AsBytes()
				require.NoError(t, err)

				meta := map[string]string{}
				require.NoError(t, m.MetaWalk(func(k, v string) error {
					meta[k] = v
					return nil
				}))

				validate(string(b), meta)
			case <-time.After(5 * time.Second):
				t.Error("timed out waiting for migrator transfer")
			}

			break loop
		}

	}
	runMigratorBundle(t, source, destination, dummyTopic, func(m *service.Message) {
		msgChan <- m
	})

	checkMigrated("redpanda_migrator_input", func(msg string, _ map[string]string) {
		assert.Equal(t, "\x00\x00\x00\x00\x01\x06foo", msg)
	})
	t.Log("Migrator started")

	dummyCG := "foobar_cg"
	// Read the message from source using a consumer group
	readMessagesWithCG(t, source, dummyTopic, dummyCG, dummyMessage, 1, true)
	checkMigrated("redpanda_migrator_offsets_input", func(_ string, meta map[string]string) {
		assert.Equal(t, dummyTopic, meta["kafka_offset_topic"])
	})
	t.Logf("Finished reading first message from source with consumer group %q", dummyCG)

	// Produce one more message in the source
	secondDummyMessage := `{"test":"bar"}`
	produceMessages(t, source, dummyTopic, secondDummyMessage, 0, 1, true)
	checkMigrated("redpanda_migrator_input", func(msg string, _ map[string]string) {
		assert.Equal(t, "\x00\x00\x00\x00\x01\x06bar", msg)
	})
	t.Log("Finished producing second message in source")

	// Read the new message from the destination using a consumer group
	readMessagesWithCG(t, destination, dummyTopic, dummyCG, secondDummyMessage, 1, true)
	checkMigrated("redpanda_migrator_offsets_input", func(_ string, meta map[string]string) {
		assert.Equal(t, dummyTopic, meta["kafka_offset_topic"])
	})
	t.Logf("Finished reading second message from destination with consumer group %q", dummyCG)
}

func TestRedpandaMigratorOffsetsIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	tests := []struct {
		name          string
		cgAtEndOffset bool
		extraCGUpdate bool
	}{
		{
			name:          "source consumer group points to the topic end offset",
			cgAtEndOffset: true,
		},
		{
			name:          "source consumer group points to an older offset inside the topic",
			cgAtEndOffset: false,
		},
		{
			name:          "subsequent consumer group updates are processed correctly",
			cgAtEndOffset: true,
			extraCGUpdate: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			pool, err := dockertest.NewPool("")
			require.NoError(t, err)
			pool.MaxWait = time.Minute

			source, err := startRedpanda(t, pool, true, true)
			require.NoError(t, err)
			destination, err := startRedpanda(t, pool, true, true)
			require.NoError(t, err)

			t.Logf("Source broker: %s", source.brokerAddr)
			t.Logf("Destination broker: %s", destination.brokerAddr)

			dummyTopic := "test"
			dummyMessage := `{"test":"foo"}`
			dummyConsumerGroup := "test_cg"
			messageCount := 5

			// Produce messages in the source cluster.
			// The message timestamps are produced in ascending order, starting from 1 all the way to messageCount.
			produceMessages(t, source, dummyTopic, dummyMessage, 0, messageCount, false)

			// Produce the exact same messages in the destination cluster.
			produceMessages(t, destination, dummyTopic, dummyMessage, 0, messageCount, false)

			// Read the messages from the source cluster using a consumer group.
			readMessagesWithCG(t, source, dummyTopic, dummyConsumerGroup, dummyMessage, 5, false)

			if test.extraCGUpdate || !test.cgAtEndOffset {
				// Make sure both source and destination have extra messages after the current consumer group offset.
				// The next messages need to have more recent timestamps than the existing messages, so we use
				// `messageCount` as an offset for their timestamps.
				produceMessages(t, source, dummyTopic, dummyMessage, messageCount, messageCount, false)
				produceMessages(t, destination, dummyTopic, dummyMessage, messageCount, messageCount, false)
			}

			t.Log("Finished setting up messages in the source and destination clusters")

			// Migrate the consumer group offsets.
			streamBuilder := service.NewStreamBuilder()
			require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  redpanda_migrator_offsets:
    seed_brokers: [ %s ]
    topics: [ %s ]

output:
  redpanda_migrator_offsets:
    seed_brokers: [ %s ]
`, source.brokerAddr, dummyTopic, destination.brokerAddr)))
			require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))

			migratorUpdateWG := sync.WaitGroup{}
			migratorUpdateWG.Add(1)
			require.NoError(t, streamBuilder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
				defer migratorUpdateWG.Done()
				return nil
			}))

			// Ensure the callback function is called after the output wrote the message.
			streamBuilder.SetOutputBrokerPattern(service.OutputBrokerPatternFanOutSequential)

			stream, err := streamBuilder.Build()
			require.NoError(t, err)

			license.InjectTestService(stream.Resources())

			// Run stream in the background.
			migratorCloseChan := make(chan struct{})
			go func() {
				err = stream.Run(context.Background())
				require.NoError(t, err)

				t.Log("redpanda_migrator_offsets pipeline shut down")

				close(migratorCloseChan)
			}()

			t.Cleanup(func() {
				require.NoError(t, stream.StopWithin(3*time.Second))

				<-migratorCloseChan
			})

			migratorUpdateWG.Wait()

			if test.extraCGUpdate {
				// Trigger another consumer group update to get it to point to the end of the topic.
				migratorUpdateWG.Add(1)
				readMessagesWithCG(t, source, dummyTopic, dummyConsumerGroup, dummyMessage, messageCount, false)
				migratorUpdateWG.Wait()
			}

			client, err := kgo.NewClient(kgo.SeedBrokers([]string{destination.brokerAddr}...))
			require.NoError(t, err)
			t.Cleanup(func() {
				client.Close()
			})

			adm := kadm.NewClient(client)
			offsets, err := adm.FetchOffsets(context.Background(), dummyConsumerGroup)
			currentCGOffset, ok := offsets.Lookup(dummyTopic, 0)
			require.True(t, ok)

			endOffset := int64(messageCount)
			if test.cgAtEndOffset {
				offsets, err := adm.ListEndOffsets(context.Background(), dummyTopic)
				require.NoError(t, err)
				o, ok := offsets.Lookup(dummyTopic, 0)
				require.True(t, ok)
				endOffset = o.Offset
			}
			assert.Equal(t, endOffset, currentCGOffset.At)
			assert.Equal(t, dummyTopic, currentCGOffset.Topic)
		})
	}
}

func TestRedpandaMigratorTopicConfigAndACLsIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Minute

	source, err := startRedpanda(t, pool, true, true)
	require.NoError(t, err)
	destination, err := startRedpanda(t, pool, true, true)
	require.NoError(t, err)

	dummyTopic := "test"

	runMigrator := func() {
		streamBuilder := service.NewStreamBuilder()
		require.NoError(t, streamBuilder.SetYAML(fmt.Sprintf(`
input:
  redpanda_migrator:
    seed_brokers: [ %s ]
    topics: [ %s ]
    consumer_group: migrator_cg

output:
  redpanda_migrator:
    seed_brokers: [ %s ]
    topic: ${! @kafka_topic }
    key: ${! @kafka_key }
    partition: ${! @kafka_partition }
    partitioner: manual
    timestamp_ms: ${! @kafka_timestamp_ms }
    translate_schema_ids: false
    replication_factor_override: true
    replication_factor: -1
`, source.brokerAddr, dummyTopic, destination.brokerAddr)))
		require.NoError(t, streamBuilder.SetLoggerYAML(`level: INFO`))

		migratorUpdateWG := sync.WaitGroup{}
		migratorUpdateWG.Add(1)
		require.NoError(t, streamBuilder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
			defer migratorUpdateWG.Done()
			return nil
		}))

		// Ensure the callback function is called after the output wrote the message.
		streamBuilder.SetOutputBrokerPattern(service.OutputBrokerPatternFanOutSequential)

		stream, err := streamBuilder.Build()
		require.NoError(t, err)

		license.InjectTestService(stream.Resources())

		// Run stream in the background.
		go func() {
			err = stream.Run(context.Background())
			require.NoError(t, err)

			t.Log("redpanda_migrator_offsets pipeline shut down")
		}()

		migratorUpdateWG.Wait()

		require.NoError(t, stream.StopWithin(3*time.Second))
	}

	// Create a topic with a custom retention time
	dummyRetentionTime := strconv.Itoa(int((48 * time.Hour).Milliseconds()))
	dummyPrincipal := "User:redpanda"
	dummyACLOperation := kmsg.ACLOperationRead
	createTopicWithACLs(t, source.brokerAddr, dummyTopic, dummyRetentionTime, dummyPrincipal, dummyACLOperation)

	// Produce one message
	dummyMessage := `{"test":"foo"}`
	produceMessages(t, source, dummyTopic, dummyMessage, 0, 1, true)

	// Run the Redpanda Migrator
	runMigrator()

	// Ensure that the topic and ACL were migrated correctly
	checkTopic(t, destination.brokerAddr, dummyTopic, dummyRetentionTime, dummyPrincipal, dummyACLOperation)

	client, err := kgo.NewClient(kgo.SeedBrokers([]string{source.brokerAddr}...))
	require.NoError(t, err)
	defer client.Close()

	adm := kadm.NewClient(client)

	// Update ACL in the source topic and ensure that it's reflected in the destination
	dummyACLOperation = kmsg.ACLOperationDescribe
	updateTopicACL(t, adm, dummyTopic, dummyPrincipal, dummyACLOperation)

	// Produce one more message so the consumerFunc will get triggered to indicate that Migrator ran successfully
	produceMessages(t, source, dummyTopic, dummyMessage, 0, 1, true)

	// Run the Redpanda Migrator again
	runMigrator()

	// Ensure that the ACL was updated correctly
	checkTopic(t, destination.brokerAddr, dummyTopic, dummyRetentionTime, dummyPrincipal, dummyACLOperation)
}
