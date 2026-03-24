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

package nats

import (
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationNatsReq(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	ctr, err := testcontainers.Run(t.Context(), "nats:latest",
		testcontainers.WithCmd("--trace"),
		testcontainers.WithExposedPorts("4222/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("4222/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	mp, err := ctr.MappedPort(t.Context(), "4222/tcp")
	require.NoError(t, err)

	var natsConn *nats.Conn
	require.Eventually(t, func() bool {
		natsConn, err = nats.Connect(fmt.Sprintf("tcp://localhost:%v", mp.Port()))
		return err == nil
	}, 30*time.Second, time.Second)

	var sub *nats.Subscription
	require.Eventually(t, func() bool {
		sub, err = natsConn.Subscribe("test.>", func(m *nats.Msg) {
			if m.Subject == "test.timeout" {
				time.Sleep(2 * time.Second)
			}
			resp := fmt.Sprintf("%s yourself", string(m.Data))
			_ = m.Respond([]byte(resp))
		})
		return err == nil
	}, 30*time.Second, time.Second)
	t.Cleanup(func() {
		_ = sub.Unsubscribe()
		natsConn.Close()
	})

	t.Run("processor", func(t *testing.T) {
		process := func(yaml string) (service.MessageBatch, error) {
			spec := natsRequestReplyConfig()
			parsed, err := spec.ParseYAML(yaml, nil)
			require.NoError(t, err)

			p, err := newRequestReplyProcessor(parsed, service.MockResources())
			require.NoError(t, err)

			m := service.NewMessage([]byte("hello"))
			return p.Process(t.Context(), m)
		}

		t.Run("normal request", func(t *testing.T) {
			url := fmt.Sprintf("tcp://localhost:%v", mp.Port())
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
urls: [%s]
subject: "test.testing"
timeout: 1s`, url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("hello yourself"), bytes)
		})

		t.Run("timeout", func(t *testing.T) {
			url := fmt.Sprintf("tcp://localhost:%v", mp.Port())
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
urls: [%s]
subject: "test.timeout"
timeout: 1s`, url)

			_, err = process(yaml)
			require.Error(t, err)
			assert.EqualError(t, err, "context deadline exceeded")
		})

		t.Run("no listeners", func(t *testing.T) {
			url := fmt.Sprintf("tcp://localhost:%v", mp.Port())

			yaml := fmt.Sprintf(`
urls: [%s]
subject: "noonelistening"
timeout: 1s`, url)

			_, err := process(yaml)
			require.ErrorIs(t, err, nats.ErrNoResponders)
		})
	})
}
