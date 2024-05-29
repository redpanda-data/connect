package aws

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"

	_ "github.com/redpanda-data/connect/v4/public/components/pure"
)

func getLocalStack(t testing.TB) (port string) {
	portInt, err := integration.GetFreePort()
	require.NoError(t, err)

	port = strconv.Itoa(portInt)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Minute

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "localstack/localstack",
		ExposedPorts: []string{port + "/tcp"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			docker.Port(port + "/tcp"): {
				docker.PortBinding{HostIP: "", HostPort: port},
			},
		},
		Env: []string{
			fmt.Sprintf("GATEWAY_LISTEN=0.0.0.0:%v", port),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	require.NoError(t, pool.Retry(func() (err error) {
		defer func() {
			if err != nil {
				t.Logf("localstack probe error: %v", err)
			}
		}()
		return createBucket(context.Background(), port, "test-bucket")
	}))
	return
}

func TestIntegration(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	servicePort := getLocalStack(t)

	t.Run("kinesis", func(t *testing.T) {
		kinesisIntegrationSuite(t, servicePort)
	})

	t.Run("s3", func(t *testing.T) {
		s3IntegrationSuite(t, servicePort)
	})

	t.Run("sqs", func(t *testing.T) {
		sqsIntegrationSuite(t, servicePort)
	})
}
