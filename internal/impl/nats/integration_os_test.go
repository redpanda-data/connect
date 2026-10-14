package nats

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationNatsObjectStore(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "nats",
		Tag:        "latest",
		Cmd:        []string{"--js", "--trace"},
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
	t.Cleanup(func() {
		natsConn.Close()
	})

	template := `
output:
  label: object_store_output
  nats_object_store:
    urls:
      - tcp://localhost:$PORT
    bucket: bucket-$ID
    object_name: ${! ksuid() }

input:
  label: object_store_input
  nats_object_store:
    urls:
      - tcp://localhost:$PORT
    bucket: bucket-$ID
`

	suite := integration.StreamTests(
		integration.StreamTestOpenClose(),
		integration.StreamTestSendBatch(10),
		integration.StreamTestStreamParallel(1000),
		integration.StreamTestStreamSequential(1000),
		integration.StreamTestStreamParallelLossy(1000),
		integration.StreamTestStreamParallelLossyThroughReconnect(1000),
	)
	suite.Run(
		t, template,
		integration.StreamTestOptPreTest(func(t testing.TB, _ context.Context, vars *integration.StreamTestConfigVars) {
			js, err := jetstream.New(natsConn)
			require.NoError(t, err)

			bucketName := "bucket-" + vars.ID

			_, err = js.CreateObjectStore(context.Background(), jetstream.ObjectStoreConfig{
				Bucket: bucketName,
			})
			require.NoError(t, err)
		}),
		integration.StreamTestOptSleepAfterInput(100*time.Millisecond),
		integration.StreamTestOptSleepAfterOutput(100*time.Millisecond),
		integration.StreamTestOptPort(resource.GetPort("4222/tcp")),
	)

	t.Run("processor", func(t *testing.T) {
		createBucket := func(t *testing.T) (jetstream.ObjectStore, string) {

			js, err := jetstream.New(natsConn)
			require.NoError(t, err)

			bucketName := "object-store-bucket"

			bucket, err := js.CreateObjectStore(context.Background(), jetstream.ObjectStoreConfig{
				Bucket: bucketName,
			})
			require.NoError(t, err)

			url := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp"))

			return bucket, url
		}

		process := func(yaml string) (service.MessageBatch, error) {
			spec := natsOSProcessorConfig()
			parsed, err := spec.ParseYAML(yaml, nil)
			require.NoError(t, err)

			p, err := newOSProcessor(parsed, service.MockResources())
			require.NoError(t, err)

			m := service.NewMessage([]byte("hello"))
			return p.Process(context.Background(), m)
		}

		t.Run("get operation", func(t *testing.T) {
			bucket, url := createBucket(t)
			_, err := bucket.PutString(context.Background(), "blob", "lawblog")
			require.NoError(t, err)

			yaml := fmt.Sprintf(`
            bucket: object-store-bucket
            operation: get
            object_name: blob
            urls: [%s]`, url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("lawblog"), bytes)
		})

		t.Run("put operation", func(t *testing.T) {
			_, url := createBucket(t)

			yaml := fmt.Sprintf(`
            bucket: object-store-bucket
            operation: put
            object_name: ting
            urls: [%s]`, url)

			result, err := process(yaml)
			require.NoError(t, err)

			m := result[0]
			bytes, err := m.AsBytes()
			require.NoError(t, err)
			assert.Equal(t, []byte("hello"), bytes)

		})

	})
}
