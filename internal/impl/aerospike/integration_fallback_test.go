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

package aerospike

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
	"github.com/redpanda-data/benthos/v4/public/service"
)

// dlqSink collects messages the fallback output forwards after Aerospike
// rejects them. It is registered on a private environment, not the global one.
type dlqSink struct {
	sync.Mutex
	msgs []*service.Message
}

func (*dlqSink) Connect(context.Context) error { return nil }
func (*dlqSink) Close(context.Context) error   { return nil }

func (s *dlqSink) WriteBatch(_ context.Context, batch service.MessageBatch) error {
	s.Lock()
	defer s.Unlock()
	for _, m := range batch {
		body, err := m.AsBytes()
		if err != nil {
			return err
		}
		copied := service.NewMessage(body)
		_ = m.MetaWalk(func(k, v string) error {
			copied.MetaSet(k, v)
			return nil
		})
		s.msgs = append(s.msgs, copied)
	}
	return nil
}

func (s *dlqSink) reset() {
	s.Lock()
	s.msgs = nil
	s.Unlock()
}

func (s *dlqSink) messages() []*service.Message {
	s.Lock()
	defer s.Unlock()
	out := make([]*service.Message, len(s.msgs))
	copy(out, s.msgs)
	return out
}

func newDLQEnv(t *testing.T, sink *dlqSink) *service.Environment {
	t.Helper()
	env := service.NewEnvironment()
	err := env.RegisterBatchOutput(
		"aerospike_test_dlq",
		service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			return sink, service.BatchPolicy{}, 1, nil
		},
	)
	require.NoError(t, err)
	return env
}

const noNSUPNamespace = "nosup"

// TestIntegrationFallbackDLQ is the config/examples/aerospike_kafka_dlq.yaml path.
// Without ignore_error_codes the rejected record is published to the next
// output. With code 21 listed, that record is acknowledged and the next
// output receives nothing. Listing only 22 drops a positive TTL on a namespace
// with nsup-period 0, and an unlisted reject is still published.
func TestIntegrationFallbackDLQ(t *testing.T) {
	host := integrationHost(t)
	client := fallbackClient(t, host)
	sink := new(dlqSink)
	env := newDLQEnv(t, sink)

	t.Run("rejected record is published", func(t *testing.T) {
		sink.reset()
		produce := fallbackProduce(t, env, host, integrationNamespace, "")
		require.NoError(t, produce(t.Context(), msg(t, `{"id":"fb-ok","v":1}`)))
		require.NoError(t, produce(t.Context(), msg(t, `{"id":"fb-long","this_bin_name_is_much_too_long":1}`)))
		require.NoError(t, produce(t.Context(), msg(t, `{"id":"fb-ok2","v":1}`)))

		got := sink.messages()
		require.Len(t, got, 1)
		body, err := got[0].AsBytes()
		require.NoError(t, err)
		assert.Contains(t, string(body), "fb-long")
		errText, ok := got[0].MetaGet("fallback_error")
		require.True(t, ok)
		assert.Contains(t, errText, "exceeds the Aerospike limit")

		assert.NotNil(t, readID(t, client, integrationNamespace, "fb-ok"))
		assert.Nil(t, readID(t, client, integrationNamespace, "fb-long"))
		assert.NotNil(t, readID(t, client, integrationNamespace, "fb-ok2"))
	})

	t.Run("listed code is not published", func(t *testing.T) {
		sink.reset()
		produce := fallbackProduce(t, env, host, integrationNamespace, "ignore_error_codes: [21]")
		require.NoError(t, produce(t.Context(), msg(t, `{"id":"ig-ok","v":1}`)))
		require.NoError(t, produce(t.Context(), msg(t, `{"id":"ig-long","this_bin_name_is_much_too_long":1}`)))
		require.NoError(t, produce(t.Context(), msg(t, `{"id":"ig-ok2","v":1}`)))

		assert.Empty(t, sink.messages())
		assert.NotNil(t, readID(t, client, integrationNamespace, "ig-ok"))
		assert.Nil(t, readID(t, client, integrationNamespace, "ig-long"))
		assert.NotNil(t, readID(t, client, integrationNamespace, "ig-ok2"))
	})

	// ignore_error_codes lists 22 only. A positive TTL on nosup is acknowledged
	// and dropped. A long bin name is code 21, so fallback publishes it.
	t.Run("ignored code is dropped and other rejects are published", func(t *testing.T) {
		sink.reset()
		produce := fallbackProduce(t, env, host, noNSUPNamespace, "ignore_error_codes: [22]\nttl: '${! json(\"ttl\").or(\"0\") }'")
		require.NoError(t, produce(t.Context(), msg(t, `{"id":"mix-ok","v":1}`)))
		require.NoError(t, produce(t.Context(), msg(t, `{"id":"mix-ttl","v":1,"ttl":"30s"}`)))
		require.NoError(t, produce(t.Context(), msg(t, `{"id":"mix-long","this_bin_name_is_much_too_long":1}`)))

		got := sink.messages()
		require.Len(t, got, 1)
		body, err := got[0].AsBytes()
		require.NoError(t, err)
		assert.Contains(t, string(body), "mix-long")
		assert.NotContains(t, string(body), "mix-ttl")
		errText, ok := got[0].MetaGet("fallback_error")
		require.True(t, ok)
		assert.Contains(t, errText, "exceeds the Aerospike limit")

		assert.NotNil(t, readID(t, client, noNSUPNamespace, "mix-ok"))
		assert.Nil(t, readID(t, client, noNSUPNamespace, "mix-ttl"))
		assert.Nil(t, readID(t, client, noNSUPNamespace, "mix-long"))
	})
}

func fallbackProduce(t *testing.T, env *service.Environment, host, namespace, extra string) service.MessageHandlerFunc {
	t.Helper()

	var extraYAML strings.Builder
	for line := range strings.SplitSeq(extra, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		extraYAML.WriteString("        ")
		extraYAML.WriteString(line)
		extraYAML.WriteByte('\n')
	}

	builder := env.NewStreamBuilder()
	require.NoError(t, builder.SetYAML(`
http:
  enabled: false
logger:
  level: ERROR
output:
  fallback:
    - aerospike:
        hosts: [ "`+host+`" ]
        namespace: `+namespace+`
        set: rpa_dlq
        key: '${! json("id") }'
        bins: 'root = this.without("id", "ttl")'
        batching:
          count: 1
`+extraYAML.String()+`    - aerospike_test_dlq: {}
`))
	produce, err := builder.AddProducerFunc()
	require.NoError(t, err)

	stream, err := builder.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		err := stream.Run(ctx)
		if errors.Is(err, context.Canceled) {
			err = nil
		}
		done <- err
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(20 * time.Second):
			t.Error("stream did not stop")
		}
	})

	return produce
}

func fallbackClient(t *testing.T, hostport string) *as.Client {
	t.Helper()
	host, portStr, err := net.SplitHostPort(hostport)
	require.NoError(t, err)
	port, err := strconv.Atoi(portStr)
	require.NoError(t, err)
	client, err := as.NewClient(host, port)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

func readID(t *testing.T, client *as.Client, namespace, id string) *as.Record {
	t.Helper()
	key, err := as.NewKey(namespace, "rpa_dlq", id)
	require.NoError(t, err)
	rec, asErr := client.Get(nil, key)
	if asErr != nil && asErr.Matches(2 /* KEY_NOT_FOUND_ERROR */) {
		return nil
	}
	require.NoError(t, asErr)
	return rec
}
