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

package couchbase_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service/integration"
)

func TestIntegrationCouchbaseCache(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := requireCouchbase(t)

	template := `
cache_resources:
  - label: testcache
    couchbase:
      url: couchbase://localhost:$PORT
      username: $USER
      password: $PASS
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
		integration.CacheTestOptVarSet("USER", username),
		integration.CacheTestOptVarSet("PASS", password),
		integration.CacheTestOptPreTest(func(tb testing.TB, ctx context.Context, vars *integration.CacheTestConfigVars) {
			require.NoError(tb, createBucket(ctx, tb, servicePort, vars.ID))
			tb.Cleanup(func() {
				require.NoError(tb, removeBucket(ctx, tb, servicePort, vars.ID))
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
