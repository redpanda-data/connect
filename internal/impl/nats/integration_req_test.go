package nats

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationNatsReq(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "nats",
		Tag:        "latest",
		Cmd:        []string{"--trace"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	var natsConn *nats.Conn
	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		natsConn, err = nats.Connect(fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp")))
		return err
	}))

	var sub *nats.Subscription
	require.NoError(t, pool.Retry(func() error {
		sub, err = natsConn.Subscribe("test.>", func(m *nats.Msg) {
			if m.Subject == "test.timeout" {
				time.Sleep(2 * time.Second)
			}
			resp := fmt.Sprintf("%s yourself", string(m.Data))
			_ = m.Respond([]byte(resp))
		})
		if err != nil {
			return err
		}

		return nil
	}))
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
			return p.Process(context.Background(), m)
		}

		t.Run("normal request", func(t *testing.T) {
			url := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp"))
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
			url := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp"))
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
			url := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp"))

			yaml := fmt.Sprintf(`
urls: [%s]
subject: "noonelistening"
timeout: 1s`, url)

			_, err := process(yaml)
			require.ErrorIs(t, err, nats.ErrNoResponders)
		})
	})
}
