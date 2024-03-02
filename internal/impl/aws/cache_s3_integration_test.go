package aws

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func createBucket(ctx context.Context, s3Port, bucket string) error {
	endpoint := fmt.Sprintf("http://localhost:%v", s3Port)

	conf, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
	)
	if err != nil {
		return err
	}

	conf.BaseEndpoint = &endpoint

	client := s3.NewFromConfig(conf, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		return err
	}

	waiter := s3.NewBucketExistsWaiter(client)
	return waiter.Wait(ctx, &s3.HeadBucketInput{
		Bucket: &bucket,
	}, time.Minute)
}

func TestIntegrationS3Cache(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "localstack/localstack",
		ExposedPorts: []string{"4566/tcp"},
		Env:          []string{"SERVICES=s3"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	servicePort := resource.GetPort("4566/tcp")

	_ = resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return createBucket(context.Background(), servicePort, "probe-bucket")
	}))

	template := `
cache_resources:
  - label: testcache
    aws_s3:
      endpoint: http://localhost:$PORT
      region: eu-west-1
      force_path_style_urls: true
      bucket: $ID
      credentials:
        id: xxxxx
        secret: xxxxx
        token: xxxxx
`
	suite := integration.CacheTests(
		integration.CacheTestOpenClose(),
		integration.CacheTestMissingKey(),
		integration.CacheTestDoubleAdd(),
		integration.CacheTestDelete(),
		integration.CacheTestGetAndSet(1),
	)
	suite.Run(
		t, template,
		integration.CacheTestOptPort(servicePort),
		integration.CacheTestOptPreTest(func(t testing.TB, ctx context.Context, testID string, vars *integration.CacheTestConfigVars) {
			require.NoError(t, createBucket(ctx, servicePort, testID))
		}),
	)
}
