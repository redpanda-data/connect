package couchbase_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/integration"
)

func TestIntegrationCouchbaseCache(t *testing.T) {
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
			require.NoError(tb, createBucket(ctx, tb, servicePort, testID))
			tb.Cleanup(func() {
				require.NoError(tb, removeBucket(ctx, tb, servicePort, testID))
			})
		}),
	)
}

func removeBucket(ctx context.Context, tb testing.TB, port, bucket string) error {
	cluster, err := gocb.Connect(fmt.Sprintf("couchbase://localhost:%v", port), gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: username,
			Password: password,
		},
	})
	if err != nil {
		return err
	}

	return cluster.Buckets().DropBucket(bucket, &gocb.DropBucketOptions{
		Context: ctx,
	})
}

func createBucket(ctx context.Context, tb testing.TB, port, bucket string) error {
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
			RAMQuotaMB: 100, // smallest value and allow max 10 running bucket with cluster-ramsize 1024 from setup script
			BucketType: gocb.CouchbaseBucketType,
		},
	}, nil)
	if err != nil {
		return err
	}

	for i := 0; i < 5; i++ { // try five time
		time.Sleep(time.Second)
		err = cluster.Bucket(bucket).WaitUntilReady(time.Second*10, nil)
		if err == nil {
			break
		}
	}

	return err
}
