package couchbase_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/couchbase/gocb/v2"
	"github.com/stretchr/testify/require"
)

func TestCacheIntegration(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := requireCouchbase(t)

	template := `
cache_resources:
  - label: testcache
    couchbase:
      url: couchbase://localhost:$PORT
      username: $VAR1
      password: $VAR2
      bucket: $ID
`

	suite := integration.CacheTests(
		integration.CacheTestOpenClose(),
		integration.CacheTestMissingKey(),
		integration.CacheTestDoubleAdd(),
		integration.CacheTestDelete(),
		integration.CacheTestGetAndSet(50),
	)
	suite.Run(
		t, template,
		integration.CacheTestOptPort(servicePort),
		integration.CacheTestOptVarOne(username),
		integration.CacheTestOptVarTwo(password),
		integration.CacheTestOptPreTest(func(tb testing.TB, ctx context.Context, testID string, vars *integration.CacheTestConfigVars) {
			require.NoError(t, createBucket(ctx, tb, servicePort, testID))
		}),
	)
}

func createBucket(ctx context.Context, tb testing.TB, port, bucket string) error {
	tb.Helper()

	cluster, err := gocb.Connect(fmt.Sprintf("couchbase://localhost:%v", port), gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		return err
	}

	err = cluster.Buckets().CreateBucket(gocb.CreateBucketSettings{
		BucketSettings: gocb.BucketSettings{
			Name:       bucket,
			RAMQuotaMB: 128,
			BucketType: gocb.CouchbaseBucketType,
		},
	}, nil)
	if err != nil {
		return err
	}

	for i := 0; i < 5; i++ { // try five time
		time.Sleep(time.Second)
		err = cluster.Bucket(bucket).WaitUntilReady(time.Second, nil)
		if err == nil {
			break
		}
	}

	return err
}
