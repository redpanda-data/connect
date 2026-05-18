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

package amqp1

import (
	"fmt"
	"testing"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func setupActiveMQ(t *testing.T) string {
	t.Helper()
	integration.CheckSkip(t)

	ctr, err := testcontainers.Run(t.Context(), "apache/activemq-classic:latest",
		testcontainers.WithExposedPorts("5672/tcp"),
		testcontainers.WithEnv(map[string]string{
			"ACTIVEMQ_CONNECTION_USER":     "guest",
			"ACTIVEMQ_CONNECTION_PASSWORD": "guest",
		}),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("5672/tcp").WithStartupTimeout(time.Minute),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "5672/tcp")
	require.NoError(t, err)

	port := mp.Port()

	require.Eventually(t, func() bool {
		client, err := amqp.Dial(t.Context(), fmt.Sprintf("amqp://guest:guest@localhost:%v/", port), nil)
		if err == nil {
			client.Close()
			return true
		}
		return false
	}, time.Minute, time.Second)

	return port
}

func TestIntegrationAMQP1MessageProperties(t *testing.T) {
	port := setupActiveMQ(t)
	url := fmt.Sprintf("amqp://guest:guest@localhost:%s/", port)
	queue := "queue:/test-msg-props"

	t.Run("input extracts all properties", func(t *testing.T) {
		ctx := t.Context()

		// Send a message with properties directly via go-amqp.
		conn, err := amqp.Dial(ctx, url, nil)
		require.NoError(t, err)
		defer conn.Close()

		sess, err := conn.NewSession(ctx, nil)
		require.NoError(t, err)

		sender, err := sess.NewSender(ctx, queue, nil)
		require.NoError(t, err)

		subject := "test-subject"
		replyTo := "reply-queue"
		groupID := "group-1"
		groupSeq := uint32(42)
		replyToGroupID := "reply-group"
		contentType := "application/json"
		contentEncoding := "utf-8"
		testUUID := amqp.UUID{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}

		err = sender.Send(ctx, &amqp.Message{
			Data: [][]byte{[]byte("hello")},
			Properties: &amqp.MessageProperties{
				MessageID:       testUUID,
				CorrelationID:   "corr-123",
				Subject:         &subject,
				ReplyTo:         &replyTo,
				GroupID:         &groupID,
				GroupSequence:   &groupSeq,
				ReplyToGroupID:  &replyToGroupID,
				UserID:          []byte("testuser"),
				ContentType:     &contentType,
				ContentEncoding: &contentEncoding,
			},
		}, nil)
		require.NoError(t, err)
		require.NoError(t, sender.Close(ctx))
		require.NoError(t, sess.Close(ctx))
		conn.Close()

		// Read via amqp_1 input.
		reader := &amqp1Reader{
			urls:       []string{url},
			sourceAddr: queue,
			credit:     64,
			connOpts:   &amqp.ConnOptions{},
			log:        service.MockResources().Logger(),
		}

		require.NoError(t, reader.Connect(ctx))
		defer reader.Close(ctx)

		batch, ack, err := reader.ReadBatch(ctx)
		require.NoError(t, err)
		require.Len(t, batch, 1)

		msg := batch[0]
		body, err := msg.AsBytes()
		require.NoError(t, err)
		require.Equal(t, "hello", string(body))

		assertMeta := func(key, expected string) {
			t.Helper()
			v, ok := msg.MetaGet(key)
			require.True(t, ok, "missing metadata key %q", key)
			require.Equal(t, expected, v, "metadata key %q", key)
		}

		assertMeta("amqp_message_id", testUUID.String())
		assertMeta("amqp_correlation_id", "corr-123")
		assertMeta("amqp_subject", "test-subject")
		assertMeta("amqp_reply_to", "reply-queue")
		assertMeta("amqp_group_id", "group-1")
		assertMeta("amqp_group_sequence", "42")
		assertMeta("amqp_reply_to_group_id", "reply-group")
		assertMeta("amqp_user_id", "testuser")
		assertMeta("amqp_content_type", "application/json")
		assertMeta("amqp_content_encoding", "utf-8")

		require.NoError(t, ack(ctx, nil))
	})

	t.Run("output sets all properties", func(t *testing.T) {
		ctx := t.Context()
		outputQueue := "queue:/test-msg-props-out"

		msgID := "550e8400-e29b-41d4-a716-446655440000"
		corrID := "12345"

		conf, err := amqp1OutputSpec().ParseYAML(fmt.Sprintf(`
urls: ["%s"]
target_address: "%s"
message_properties_message_id: "%s"
message_properties_correlation_id: "%s"
message_properties_subject: "my-subject"
message_properties_reply_to: "my-reply"
message_properties_group_id: "my-group"
message_properties_group_sequence: "7"
message_properties_reply_to_group_id: "my-reply-group"
message_properties_user_id: "my-user"
message_properties_content_type: "text/plain"
message_properties_content_encoding: "gzip"
`, url, outputQueue, msgID, corrID), nil)
		require.NoError(t, err)

		writer, err := amqp1WriterFromParsed(conf, service.MockResources())
		require.NoError(t, err)

		require.NoError(t, writer.Connect(ctx))
		defer writer.Close(ctx)

		outMsg := service.NewMessage([]byte("world"))
		require.NoError(t, writer.Write(ctx, outMsg))

		// Read back via go-amqp directly.
		conn, err := amqp.Dial(ctx, url, nil)
		require.NoError(t, err)
		defer conn.Close()

		sess, err := conn.NewSession(ctx, nil)
		require.NoError(t, err)

		receiver, err := sess.NewReceiver(ctx, outputQueue, nil)
		require.NoError(t, err)

		received, err := receiver.Receive(ctx, nil)
		require.NoError(t, err)
		require.NoError(t, receiver.AcceptMessage(ctx, received))

		require.NotNil(t, received.Properties)
		props := received.Properties

		// MessageID: UUID auto-detected
		msgUUID, ok := props.MessageID.(amqp.UUID)
		require.True(t, ok, "expected UUID type for message-id, got %T", props.MessageID)
		require.Equal(t, msgID, msgUUID.String())

		// CorrelationID: uint64 auto-detected
		corrUint, ok := props.CorrelationID.(uint64)
		require.True(t, ok, "expected uint64 type for correlation-id, got %T", props.CorrelationID)
		require.Equal(t, uint64(12345), corrUint)

		require.NotNil(t, props.Subject)
		require.Equal(t, "my-subject", *props.Subject)
		require.NotNil(t, props.ReplyTo)
		require.Equal(t, "my-reply", *props.ReplyTo)
		require.NotNil(t, props.GroupID)
		require.Equal(t, "my-group", *props.GroupID)
		require.NotNil(t, props.GroupSequence)
		require.Equal(t, uint32(7), *props.GroupSequence)
		require.NotNil(t, props.ReplyToGroupID)
		require.Equal(t, "my-reply-group", *props.ReplyToGroupID)
		require.Equal(t, []byte("my-user"), props.UserID)
		require.NotNil(t, props.ContentType)
		require.Equal(t, "text/plain", *props.ContentType)
		require.NotNil(t, props.ContentEncoding)
		require.Equal(t, "gzip", *props.ContentEncoding)
	})

	t.Run("round-trip preserves properties", func(t *testing.T) {
		ctx := t.Context()
		rtQueue := "queue:/test-msg-props-rt"

		conf, err := amqp1OutputSpec().ParseYAML(fmt.Sprintf(`
urls: ["%s"]
target_address: "%s"
message_properties_message_id: '${! meta("amqp_message_id") }'
message_properties_correlation_id: '${! meta("amqp_correlation_id") }'
message_properties_subject: '${! meta("amqp_subject") }'
message_properties_reply_to: '${! meta("amqp_reply_to") }'
message_properties_group_id: '${! meta("amqp_group_id") }'
message_properties_group_sequence: '${! meta("amqp_group_sequence") }'
message_properties_reply_to_group_id: '${! meta("amqp_reply_to_group_id") }'
message_properties_user_id: '${! meta("amqp_user_id") }'
message_properties_content_type: '${! meta("amqp_content_type") }'
message_properties_content_encoding: '${! meta("amqp_content_encoding") }'
metadata:
  exclude_prefixes: ["amqp_"]
`, url, rtQueue), nil)
		require.NoError(t, err)

		writer, err := amqp1WriterFromParsed(conf, service.MockResources())
		require.NoError(t, err)
		require.NoError(t, writer.Connect(ctx))
		defer writer.Close(ctx)

		// Build a message with metadata as if it came from an amqp_1 input.
		outMsg := service.NewMessage([]byte("roundtrip"))
		outMsg.MetaSetMut("amqp_message_id", "550e8400-e29b-41d4-a716-446655440000")
		outMsg.MetaSetMut("amqp_correlation_id", "corr-string-value")
		outMsg.MetaSetMut("amqp_subject", "rt-subject")
		outMsg.MetaSetMut("amqp_reply_to", "rt-reply")
		outMsg.MetaSetMut("amqp_group_id", "rt-group")
		outMsg.MetaSetMut("amqp_group_sequence", "99")
		outMsg.MetaSetMut("amqp_reply_to_group_id", "rt-reply-group")
		outMsg.MetaSetMut("amqp_user_id", "rt-user")
		outMsg.MetaSetMut("amqp_content_type", "application/xml")
		outMsg.MetaSetMut("amqp_content_encoding", "identity")
		require.NoError(t, writer.Write(ctx, outMsg))

		// Read back via input.
		reader := &amqp1Reader{
			urls:       []string{url},
			sourceAddr: rtQueue,
			credit:     64,
			connOpts:   &amqp.ConnOptions{},
			log:        service.MockResources().Logger(),
		}
		require.NoError(t, reader.Connect(ctx))
		defer reader.Close(ctx)

		batch, ack, err := reader.ReadBatch(ctx)
		require.NoError(t, err)
		require.Len(t, batch, 1)

		msg := batch[0]
		body, err := msg.AsBytes()
		require.NoError(t, err)
		require.Equal(t, "roundtrip", string(body))

		assertMeta := func(key, expected string) {
			t.Helper()
			v, ok := msg.MetaGet(key)
			require.True(t, ok, "missing metadata key %q", key)
			require.Equal(t, expected, v, "metadata key %q", key)
		}

		// UUID round-trips through string representation.
		assertMeta("amqp_message_id", "550e8400-e29b-41d4-a716-446655440000")
		// String correlation-id stays string.
		assertMeta("amqp_correlation_id", "corr-string-value")
		assertMeta("amqp_subject", "rt-subject")
		assertMeta("amqp_reply_to", "rt-reply")
		assertMeta("amqp_group_id", "rt-group")
		assertMeta("amqp_group_sequence", "99")
		assertMeta("amqp_reply_to_group_id", "rt-reply-group")
		assertMeta("amqp_user_id", "rt-user")
		assertMeta("amqp_content_type", "application/xml")
		assertMeta("amqp_content_encoding", "identity")

		require.NoError(t, ack(ctx, nil))
	})
}

func TestIntegrationAMQP1(t *testing.T) {
	port := setupActiveMQ(t)

	templateWithFieldURL := `
output:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    target_address: "queue:/$ID"
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]

input:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    source_address: "queue:/$ID"
`

	templateWithFieldURLS := `
output:
  amqp_1:
    urls:
      - amqp://guest:guest@localhost:1234/
      - amqp://guest:guest@localhost:$PORT/ # fallback URL
      - amqp://guest:guest@localhost:4567/
    target_address: "queue:/$ID"
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]

input:
  amqp_1:
    urls:
      - amqp://guest:guest@localhost:1234/
      - amqp://guest:guest@localhost:$PORT/ # fallback URL
      - amqp://guest:guest@localhost:4567/
    source_address: "queue:/$ID"
`

	templateWithContentTypeString := `
output:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    target_address: "queue:/$ID"
    max_in_flight: $MAX_IN_FLIGHT
    content_type: "string"
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]
input:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    source_address: "queue:/$ID"
`

	templateWithAnonymousTerminus := `
output:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    target_address: ""
    message_properties_to: "queue:/$ID"
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]
input:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    source_address: "queue:/$ID"
`

	templateWithAnonymousTerminusBloblang := `
output:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    target_address: ""
    message_properties_to: '${! meta("target_queue").or("queue:/$ID") }'
    max_in_flight: $MAX_IN_FLIGHT
    metadata:
      exclude_prefixes: [ $OUTPUT_META_EXCLUDE_PREFIX ]
input:
  amqp_1:
    url: amqp://guest:guest@localhost:$PORT/
    source_address: "queue:/$ID"
`

	testcases := []struct {
		label    string
		template string
	}{
		{
			label:    "should handle old field url",
			template: templateWithFieldURL,
		},
		{
			label:    "should handle new field urls",
			template: templateWithFieldURLS,
		},
		{
			label:    "should handle content type string",
			template: templateWithContentTypeString,
		},
		{
			label:    "should handle Anonymous Terminus pattern",
			template: templateWithAnonymousTerminus,
		},
		{
			label:    "should handle Anonymous Terminus with Bloblang interpolation",
			template: templateWithAnonymousTerminusBloblang,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.label, func(t *testing.T) {
			suite := integration.StreamTests(
				integration.StreamTestOpenClose(),
				integration.StreamTestSendBatch(10),
				integration.StreamTestStreamSequential(1000),
				integration.StreamTestStreamParallel(1000),
				integration.StreamTestMetadata(),
				integration.StreamTestMetadataFilter(),
			)
			suite.Run(
				t, tc.template,
				integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
				integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
				integration.StreamTestOptPort(port),
			)

			t.Run("with max in flight", func(t *testing.T) {
				t.Parallel()
				suite.Run(
					t, tc.template,
					integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
					integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
					integration.StreamTestOptPort(port),
					integration.StreamTestOptMaxInFlight(10),
				)
			})
		})
	}
}
