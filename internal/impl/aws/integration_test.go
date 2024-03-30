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

	"github.com/benthosdev/benthos/v4/internal/integration"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func getLocalStack(tb testing.TB) (port string) {
	tb.Helper()

	portInt, err := integration.GetFreePort()
	require.NoError(tb, err)

	port = strconv.Itoa(portInt)

	pool, err := dockertest.NewPool("")
	require.NoError(tb, err)

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
	require.NoError(tb, err)
	tb.Cleanup(func() {
		assert.NoError(tb, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	require.NoError(tb, pool.Retry(func() (err error) {
		defer func() {
			if err != nil {
				tb.Logf("localstack probe error: %v", err)
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
