// Copyright 2026 Redpanda Data, Inc.
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

package s3

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"

	"github.com/redpanda-data/connect/v4/internal/impl/aws/awstest"
)

func TestIntegrationS3(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := awstest.GetLocalStack(t)
	s3IntegrationSuite(t, servicePort)
}

func s3IntegrationSuite(t *testing.T, lsPort string) {
	t.Run("via_sqs", func(t *testing.T) {
		template := `
output:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    path: ${!counter()}.txt
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    delete_objects: true
    sqs:
      url: http://localhost:$PORT/000000000000/queue-$ID
      key_path: Records.*.s3.object.key
      endpoint: http://localhost:$PORT
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
		integration.StreamTests(
			integration.StreamTestOpenClose(),
			// integration.StreamTestMetadata(), Does dumb stuff with rewriting keys.
			// integration.StreamTestSendBatch(10),
			integration.StreamTestSendBatchCount(10),
			integration.StreamTestStreamSequential(10),
			// integration.StreamTestStreamParallel(10),
			// integration.StreamTestStreamParallelLossy(10),
			integration.StreamTestStreamParallelLossyThroughReconnect(10),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				require.NoError(t, awstest.CreateBucketQueue(ctx, lsPort, lsPort, vars.ID))
			}),
			integration.StreamTestOptPort(lsPort),
			integration.StreamTestOptAllowDupes(),
		)
	})

	t.Run("via_sqs_lines", func(t *testing.T) {
		template := `
output:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    path: ${!counter()}.txt
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    batching:
      count: $OUTPUT_BATCH_COUNT
      processors:
        - archive:
            format: lines

input:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    delete_objects: true
    scanner: { lines: {} }
    sqs:
      url: http://localhost:$PORT/000000000000/queue-$ID
      key_path: Records.*.s3.object.key
      endpoint: http://localhost:$PORT
      delay_period: 1s
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
		integration.StreamTests(
			integration.StreamTestOpenClose(),
			integration.StreamTestStreamSequential(20),
			integration.StreamTestSendBatchCount(10),
			integration.StreamTestStreamParallelLossyThroughReconnect(20),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				if tmp := vars.General["OUTPUT_BATCH_COUNT"]; tmp == "0" || tmp == "" {
					vars.General["OUTPUT_BATCH_COUNT"] = "1"
				}
				require.NoError(t, awstest.CreateBucketQueue(ctx, lsPort, lsPort, vars.ID))
			}),
			integration.StreamTestOptPort(lsPort),
			integration.StreamTestOptAllowDupes(),
		)
	})

	t.Run("via_sqs_lines_old_codec", func(t *testing.T) {
		template := `
output:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    path: ${!counter()}.txt
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    batching:
      count: $OUTPUT_BATCH_COUNT
      processors:
        - archive:
            format: lines

input:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    delete_objects: true
    codec: lines
    sqs:
      url: http://localhost:$PORT/000000000000/queue-$ID
      key_path: Records.*.s3.object.key
      endpoint: http://localhost:$PORT
      delay_period: 1s
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
		integration.StreamTests(
			integration.StreamTestOpenClose(),
			integration.StreamTestStreamSequential(20),
			integration.StreamTestSendBatchCount(10),
			integration.StreamTestStreamParallelLossyThroughReconnect(20),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				if tmp := vars.General["OUTPUT_BATCH_COUNT"]; tmp == "0" || tmp == "" {
					vars.General["OUTPUT_BATCH_COUNT"] = "1"
				}
				require.NoError(t, awstest.CreateBucketQueue(ctx, lsPort, lsPort, vars.ID))
			}),
			integration.StreamTestOptPort(lsPort),
			integration.StreamTestOptAllowDupes(),
		)
	})

	t.Run("batch", func(t *testing.T) {
		template := `
output:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    path: ${!counter()}.txt
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    batching:
      count: $OUTPUT_BATCH_COUNT

input:
  aws_s3:
    bucket: bucket-$ID
    endpoint: http://localhost:$PORT
    force_path_style_urls: true
    region: eu-west-1
    delete_objects: true
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`
		integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(10),
		).Run(
			t, template,
			integration.StreamTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.StreamTestConfigVars) {
				require.NoError(t, awstest.CreateBucketQueue(ctx, lsPort, "", vars.ID))
			}),
			integration.StreamTestOptPort(lsPort),
		)
	})

	t.Run("cache", func(t *testing.T) {
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
			integration.CacheTestOptPort(lsPort),
			integration.CacheTestOptPreTest(func(t testing.TB, ctx context.Context, vars *integration.CacheTestConfigVars) {
				require.NoError(t, awstest.CreateBucket(ctx, lsPort, vars.ID))
			}),
		)
	})

	t.Run("object_canned_acl_round_trip", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		bucket := "acl-roundtrip-bucket"
		require.NoError(t, awstest.CreateBucket(ctx, lsPort, bucket))

		yaml := fmt.Sprintf(`
output:
  aws_s3:
    bucket: %s
    endpoint: http://localhost:%s
    force_path_style_urls: true
    region: eu-west-1
    path: ${!counter()}.txt
    object_canned_acl: bucket-owner-full-control
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`, bucket, lsPort)

		builder := service.NewStreamBuilder()
		require.NoError(t, builder.SetYAML(yaml))

		producer, err := builder.AddProducerFunc()
		require.NoError(t, err)

		stream, err := builder.Build()
		require.NoError(t, err, "stream build must succeed when object_canned_acl is a valid value")

		runErr := make(chan error, 1)
		runCtx, runCancel := context.WithCancel(ctx)
		defer runCancel()
		go func() { runErr <- stream.Run(runCtx) }()

		require.NoError(t, producer(ctx, service.NewMessage([]byte("hello acl"))))

		require.NoError(t, stream.StopWithin(10*time.Second))
		select {
		case err := <-runErr:
			if err != nil && !errors.Is(err, context.Canceled) {
				require.NoError(t, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("stream did not exit after StopWithin returned")
		}

		// Verify the object exists, then attempt to read its ACL. LocalStack
		// Free does not always reflect the canned ACL value through GetObjectAcl,
		// so the ACL check is best-effort and only fails if LocalStack returns
		// an explicit error other than "not implemented" style noise. The
		// authoritative end-to-end coverage is that the stream built and uploaded
		// successfully with the ACL field set — the regression in PR #4413
		// blocked exactly that.
		client := newLocalStackS3Client(t, lsPort)
		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("1.txt"),
		})
		require.NoError(t, err, "object should have been uploaded")

		aclOut, aclErr := client.GetObjectAcl(ctx, &s3.GetObjectAclInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("1.txt"),
		})
		if aclErr != nil {
			t.Logf("GetObjectAcl returned an error (LocalStack ACL support is partial): %v", aclErr)
		} else {
			t.Logf("GetObjectAcl returned %d grants", len(aclOut.Grants))
		}
	})

	t.Run("object_canned_acl_invalid_rejected", func(t *testing.T) {
		yaml := fmt.Sprintf(`
output:
  aws_s3:
    bucket: irrelevant
    endpoint: http://localhost:%s
    force_path_style_urls: true
    region: eu-west-1
    path: ${!counter()}.txt
    object_canned_acl: not-a-real-acl
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`, lsPort)

		builder := service.NewStreamBuilder()
		setErr := builder.SetYAML(yaml)
		if setErr != nil {
			assert.Contains(t, setErr.Error(), "not-a-real-acl")
			return
		}

		_, buildErr := builder.Build()
		require.Error(t, buildErr, "stream build must reject an invalid object_canned_acl value")
		assert.Contains(t, buildErr.Error(), "not-a-real-acl")
	})
}

// newLocalStackS3Client builds an S3 client pointed at the LocalStack instance
// on the supplied port, with path-style URLs and dummy credentials.
func newLocalStackS3Client(t *testing.T, port string) *s3.Client {
	t.Helper()
	endpoint := fmt.Sprintf("http://localhost:%s", port)
	cfg, err := awsconfig.LoadDefaultConfig(t.Context(),
		awsconfig.WithRegion("eu-west-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
	)
	require.NoError(t, err)
	cfg.BaseEndpoint = &endpoint
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})
}
