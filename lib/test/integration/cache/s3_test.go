package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createBucket(ctx context.Context, s3Port, bucket string) error {
	endpoint := fmt.Sprintf("http://localhost:%v", s3Port)

	client := s3.New(session.Must(session.NewSession(&aws.Config{
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials("xxxxx", "xxxxx", "xxxxx"),
		Endpoint:         aws.String(endpoint),
		Region:           aws.String("eu-west-1"),
	})))
	_, err := client.CreateBucket(&s3.CreateBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		return err
	}

	return client.WaitUntilBucketExistsWithContext(ctx, &s3.HeadBucketInput{
		Bucket: &bucket,
	})
}

var _ = registerIntegrationTest("s3", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository:   "localstack/localstack",
		ExposedPorts: []string{"4572/tcp"},
		Env:          []string{"SERVICES=s3"},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return createBucket(context.Background(), resource.GetPort("4572/tcp"), "probe-bucket")
	}))

	template := `
resources:
  caches:
    testcache:
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
	suite := integrationTests(
		integrationTestOpenClose(),
		integrationTestMissingKey(),
		integrationTestDoubleAdd(),
		integrationTestDelete(),
		integrationTestGetAndSet(1),
	)
	suite.Run(
		t, template,
		testOptPort(resource.GetPort("4572/tcp")),
		testOptPreTest(func(t *testing.T, env *testEnvironment) {
			require.NoError(t, createBucket(env.ctx, resource.GetPort("4572/tcp"), env.configVars.id))
		}),
	)
})
