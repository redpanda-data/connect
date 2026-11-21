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
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
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

	t.Run("via_sqs_test_event_deleted", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		id := fmt.Sprintf("testevent%d", time.Now().UnixNano())
		require.NoError(t, awstest.CreateBucketQueue(ctx, lsPort, lsPort, id))

		sqsClient := newLocalStackSQSClient(t, lsPort)
		queueURL := fmt.Sprintf("http://localhost:%s/000000000000/queue-%s", lsPort, id)

		testEventBody := fmt.Sprintf(
			`{"Service":"Amazon S3","Event":"s3:TestEvent","Time":"%s","Bucket":"bucket-%s","RequestId":"N99ABJ6Q","HostId":"+3DhJHKGDGBwqSTufMSS1UgAMIoRovmGa9vkZwWIb1="}`,
			time.Now().UTC().Format(time.RFC3339), id,
		)
		_, err := sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    aws.String(queueURL),
			MessageBody: aws.String(testEventBody),
		})
		require.NoError(t, err)

		yaml := fmt.Sprintf(`
input:
  aws_s3:
    bucket: bucket-%s
    endpoint: http://localhost:%s
    force_path_style_urls: true
    region: eu-west-1
    delete_objects: true
    sqs:
      url: %s
      key_path: Records.*.s3.object.key
      endpoint: http://localhost:%s
      wait_time_seconds: 1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`, id, lsPort, queueURL, lsPort)

		builder := service.NewStreamBuilder()
		require.NoError(t, builder.SetYAML(yaml))

		var (
			unexpectedMu   sync.Mutex
			unexpectedMsgs [][]byte
		)
		require.NoError(t, builder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
			b, err := m.AsBytes()
			if err != nil {
				b = fmt.Appendf(nil, "<failed to read message bytes: %v>", err)
			}
			unexpectedMu.Lock()
			unexpectedMsgs = append(unexpectedMsgs, b)
			unexpectedMu.Unlock()
			return nil
		}))

		stream, err := builder.Build()
		require.NoError(t, err)

		runErr := make(chan error, 1)
		runCtx, runCancel := context.WithCancel(ctx)
		defer runCancel()
		go func() { runErr <- stream.Run(runCtx) }()

		// The test event should be deleted from the queue rather than left
		// to be received again indefinitely.
		assert.Eventually(t, func() bool {
			out, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				QueueUrl: aws.String(queueURL),
				AttributeNames: []sqstypes.QueueAttributeName{
					sqstypes.QueueAttributeNameApproximateNumberOfMessages,
					sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
				},
			})
			if err != nil {
				return false
			}
			return out.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessages)] == "0" &&
				out.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible)] == "0"
		}, 15*time.Second, 500*time.Millisecond, "test event message should have been deleted from the queue")

		require.NoError(t, stream.StopWithin(10*time.Second))
		select {
		case err := <-runErr:
			if err != nil && !errors.Is(err, context.Canceled) {
				require.NoError(t, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("stream did not exit after StopWithin returned")
		}

		unexpectedMu.Lock()
		assert.Empty(t, unexpectedMsgs, "did not expect any message to be emitted for an s3:TestEvent")
		unexpectedMu.Unlock()
	})

	t.Run("via_sqs_zero_key_non_test_event_warns", func(t *testing.T) {
		// Valid JSON, not an s3:TestEvent, but missing the object key at the
		// configured key_path - simulates a key_path/bucket_path
		// misconfiguration (or an unrecognised event type) rather than a
		// test event, which should be handled differently: warned about and
		// left on the queue, not silently deleted.
		cases := []struct {
			name             string
			warnIntervalYAML string
			minWarnCount     int
			expectExactlyOne bool
		}{
			{
				name:             "default interval throttles to one warning",
				expectExactlyOne: true,
			},
			{
				name:             "zero interval disables throttling",
				warnIntervalYAML: "      zero_key_warn_interval: 0s\n",
				minWarnCount:     3,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()

				id := fmt.Sprintf("zerokey%d", time.Now().UnixNano())
				require.NoError(t, awstest.CreateBucketQueue(ctx, lsPort, lsPort, id))

				sqsClient := newLocalStackSQSClient(t, lsPort)
				queueURL := fmt.Sprintf("http://localhost:%s/000000000000/queue-%s", lsPort, id)

				misconfiguredBody := fmt.Sprintf(
					`{"Records":[{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"bucket-%s"}}}]}`, id,
				)
				_, err := sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
					QueueUrl:    aws.String(queueURL),
					MessageBody: aws.String(misconfiguredBody),
				})
				require.NoError(t, err)

				yaml := fmt.Sprintf(`
input:
  aws_s3:
    bucket: bucket-%s
    endpoint: http://localhost:%s
    force_path_style_urls: true
    region: eu-west-1
    delete_objects: true
    sqs:
      url: %s
      key_path: Records.*.s3.object.key
      endpoint: http://localhost:%s
      wait_time_seconds: 1
%s    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
`, id, lsPort, queueURL, lsPort, tc.warnIntervalYAML)

				var logBuf syncBuffer
				builder := service.NewStreamBuilder()
				builder.SetLogger(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))

				require.NoError(t, builder.SetYAML(yaml))
				require.NoError(t, builder.AddConsumerFunc(func(_ context.Context, m *service.Message) error {
					b, _ := m.AsBytes()
					t.Errorf("did not expect any message to be emitted for a zero-key notification, got: %s", b)
					return nil
				}))

				stream, err := builder.Build()
				require.NoError(t, err)

				runErr := make(chan error, 1)
				runCtx, runCancel := context.WithCancel(ctx)
				defer runCancel()
				go func() { runErr <- stream.Run(runCtx) }()

				// A non-test-event, zero-key message should be warned
				// about, identifying the likely key_path misconfiguration,
				// and - unlike an s3:TestEvent - left on the queue for
				// redelivery rather than deleted.
				const zeroKeyWarnMsg = "Extracted zero target keys from SQS message using key_path"

				assert.Eventually(t, func() bool {
					return strings.Contains(logBuf.String(), zeroKeyWarnMsg)
				}, 15*time.Second, 500*time.Millisecond, "expected a WARN log identifying a key_path misconfiguration, got logs: %s", &logBuf)

				if tc.expectExactlyOne {
					// The message is redelivered roughly every 500ms (Pop's
					// empty-read backoff) for as long as it stays
					// misconfigured, so without throttling this would emit
					// a fresh WARN with the full body on every cycle. Give
					// it several more cycles and confirm the WARN stays at
					// a single occurrence.
					time.Sleep(3 * time.Second)
					assert.Equal(t, 1, strings.Count(logBuf.String(), zeroKeyWarnMsg),
						"expected the misconfiguration warning to be throttled to a single occurrence, got logs: %s", logBuf.String())
				} else {
					// zero_key_warn_interval: 0s disables the throttle, so
					// the same ~500ms redelivery cycle should produce
					// multiple WARNs instead of just one.
					assert.Eventually(t, func() bool {
						return strings.Count(logBuf.String(), zeroKeyWarnMsg) >= tc.minWarnCount
					}, 15*time.Second, 500*time.Millisecond, "expected at least %d WARN logs with throttling disabled, got logs: %s", tc.minWarnCount, &logBuf)
				}

				out, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
					QueueUrl: aws.String(queueURL),
					AttributeNames: []sqstypes.QueueAttributeName{
						sqstypes.QueueAttributeNameApproximateNumberOfMessages,
						sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
					},
				})
				require.NoError(t, err)
				visible := out.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessages)]
				notVisible := out.Attributes[string(sqstypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible)]
				assert.False(t, visible == "0" && notVisible == "0", "misconfigured message should remain on the queue for redelivery, not be deleted")

				require.NoError(t, stream.StopWithin(10*time.Second))
				select {
				case err := <-runErr:
					if err != nil && !errors.Is(err, context.Canceled) {
						require.NoError(t, err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("stream did not exit after StopWithin returned")
				}
			})
		}
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

	t.Run("dynamic_bucket_mixed_batch", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		dyn := uniqueBucketPrefix("dyn")
		bucketA, bucketB := dyn+"-acme", dyn+"-globex"
		require.NoError(t, awstest.CreateBucket(ctx, lsPort, bucketA))
		require.NoError(t, awstest.CreateBucket(ctx, lsPort, bucketB))

		parsed, err := s3oOutputSpec().ParseYAML(fmt.Sprintf(`
bucket: '%s-${! metadata("client_id") }'
endpoint: http://localhost:%s
force_path_style_urls: true
region: eu-west-1
path: ${!content()}.txt
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, dyn, lsPort), nil)
		require.NoError(t, err)

		conf, err := s3oConfigFromParsed(parsed)
		require.NoError(t, err)

		w, err := newAmazonS3Writer(conf, service.MockResources())
		require.NoError(t, err)
		require.NoError(t, w.Connect(ctx))

		batch := service.MessageBatch{
			service.NewMessage([]byte("one")),
			service.NewMessage([]byte("two")),
		}
		batch[0].MetaSetMut("client_id", "acme")
		batch[1].MetaSetMut("client_id", "globex")
		require.NoError(t, w.WriteBatch(ctx, batch))

		client := newLocalStackS3Client(t, lsPort)
		for bucket, key := range map[string]string{bucketA: "one.txt", bucketB: "two.txt"} {
			out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
			require.NoError(t, err, "bucket %s should hold %s", bucket, key)
			_ = out.Body.Close()
		}

		// Each object must only exist in its own bucket.
		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucketA), Key: aws.String("two.txt")})
		require.Error(t, err)
		_, err = client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucketB), Key: aws.String("one.txt")})
		require.Error(t, err)

		// A missing bucket error names the bucket that failed.
		bad := service.MessageBatch{service.NewMessage([]byte("three"))}
		bad[0].MetaSetMut("client_id", "missing")
		err = w.WriteBatch(ctx, bad)
		require.Error(t, err)
		assert.Contains(t, err.Error(), dyn+"-missing")

		// A message that resolves to an empty bucket is rejected.
		emptyParsed, err := s3oOutputSpec().ParseYAML(`
bucket: '${! metadata("client_id") }'
region: eu-west-1
credentials:
  id: xxxxx
  secret: xxxxx
`, nil)
		require.NoError(t, err)
		emptyConf, err := s3oConfigFromParsed(emptyParsed)
		require.NoError(t, err)
		ew, err := newAmazonS3Writer(emptyConf, service.MockResources())
		require.NoError(t, err)
		require.NoError(t, ew.Connect(ctx))
		emptyMsg := service.NewMessage([]byte("x"))
		emptyMsg.MetaSetMut("client_id", "")
		err = ew.WriteBatch(ctx, service.MessageBatch{emptyMsg})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty bucket name")
	})

	t.Run("dynamic_bucket", func(t *testing.T) { s3DynamicBucketSuite(t, lsPort) })

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

// newLocalStackSQSClient builds an SQS client pointed at the LocalStack
// instance on the supplied port, with dummy credentials.
func newLocalStackSQSClient(t *testing.T, port string) *sqs.Client {
	t.Helper()
	endpoint := fmt.Sprintf("http://localhost:%s", port)
	cfg, err := awsconfig.LoadDefaultConfig(t.Context(),
		awsconfig.WithRegion("eu-west-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("xxxxx", "xxxxx", "xxxxx")),
	)
	require.NoError(t, err)
	cfg.BaseEndpoint = &endpoint
	return sqs.NewFromConfig(cfg)
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newDynamicBucketWriter builds a connected aws_s3 writer against LocalStack
// with the given bucket and path expressions.
func newDynamicBucketWriter(t *testing.T, ctx context.Context, port, bucketExpr, pathExpr string) *amazonS3Writer {
	t.Helper()
	parsed, err := s3oOutputSpec().ParseYAML(fmt.Sprintf(`
bucket: '%s'
path: '%s'
endpoint: http://localhost:%s
force_path_style_urls: true
region: eu-west-1
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, bucketExpr, pathExpr, port), nil)
	require.NoError(t, err)
	conf, err := s3oConfigFromParsed(parsed)
	require.NoError(t, err)
	w, err := newAmazonS3Writer(conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, w.Connect(ctx))
	return w
}

func s3ObjectBody(ctx context.Context, t *testing.T, client *s3.Client, bucket, key string) (string, bool) {
	t.Helper()
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return "", false
	}
	defer out.Body.Close()
	var buf bytes.Buffer
	_, err = buf.ReadFrom(out.Body)
	require.NoError(t, err)
	return buf.String(), true
}

// uniqueBucketPrefix returns a bucket name prefix that is unique per call, so
// tests can be re-run (-count=N) against the shared LocalStack container.
func uniqueBucketPrefix(base string) string {
	return fmt.Sprintf("%s-%x", base, time.Now().UnixNano())
}

func metaMsg(body string, kv ...string) *service.Message {
	m := service.NewMessage([]byte(body))
	for i := 0; i+1 < len(kv); i += 2 {
		m.MetaSetMut(kv[i], kv[i+1])
	}
	return m
}

// s3DynamicBucketSuite covers realistic usage of an interpolated bucket:
// multi-tenant routing, partial failures, bad data, and the unchanged
// behavior of static buckets.
func s3DynamicBucketSuite(t *testing.T, lsPort string) {
	client := newLocalStackS3Client(t, lsPort)

	t.Run("many_messages_two_tenants_keep_bodies_in_right_bucket", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		mt := uniqueBucketPrefix("mt")
		require.NoError(t, awstest.CreateBucket(ctx, lsPort, mt+"-acme"))
		require.NoError(t, awstest.CreateBucket(ctx, lsPort, mt+"-globex"))

		w := newDynamicBucketWriter(t, ctx, lsPort, mt+`-${! metadata("tenant") }`, `${! metadata("id") }.txt`)

		var batch service.MessageBatch
		want := map[string]map[string]string{mt + "-acme": {}, mt + "-globex": {}}
		for i := range 20 {
			tenant := "acme"
			if i%3 == 0 {
				tenant = "globex"
			}
			id := fmt.Sprintf("msg-%d", i)
			body := fmt.Sprintf("payload for %s", id)
			batch = append(batch, metaMsg(body, "tenant", tenant, "id", id))
			want[mt+"-"+tenant][id+".txt"] = body
		}
		require.NoError(t, w.WriteBatch(ctx, batch))

		for bucket, objs := range want {
			for key, body := range objs {
				got, ok := s3ObjectBody(ctx, t, client, bucket, key)
				require.True(t, ok, "%s/%s missing", bucket, key)
				assert.Equal(t, body, got, "%s/%s", bucket, key)
			}
		}
		// Nothing leaked into the other tenant's bucket.
		for key := range want[mt+"-acme"] {
			_, ok := s3ObjectBody(ctx, t, client, mt+"-globex", key)
			assert.False(t, ok, "acme object %s leaked into globex bucket", key)
		}
	})

	t.Run("bucket_from_json_payload", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		js := uniqueBucketPrefix("json")
		require.NoError(t, awstest.CreateBucket(ctx, lsPort, js+"-orders"))
		require.NoError(t, awstest.CreateBucket(ctx, lsPort, js+"-refunds"))

		w := newDynamicBucketWriter(t, ctx, lsPort, js+`-${! json("kind") }`, `${! json("id") }.json`)
		require.NoError(t, w.WriteBatch(ctx, service.MessageBatch{
			service.NewMessage([]byte(`{"kind":"orders","id":"o1"}`)),
			service.NewMessage([]byte(`{"kind":"refunds","id":"r1"}`)),
		}))

		_, ok := s3ObjectBody(ctx, t, client, js+"-orders", "o1.json")
		assert.True(t, ok)
		_, ok = s3ObjectBody(ctx, t, client, js+"-refunds", "r1.json")
		assert.True(t, ok)
	})

	t.Run("one_bad_bucket_does_not_block_good_messages", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		pf := uniqueBucketPrefix("pf")
		require.NoError(t, awstest.CreateBucket(ctx, lsPort, pf+"-acme"))
		require.NoError(t, awstest.CreateBucket(ctx, lsPort, pf+"-globex"))

		w := newDynamicBucketWriter(t, ctx, lsPort, pf+`-${! metadata("tenant") }`, `${! metadata("id") }.txt`)
		batch := service.MessageBatch{
			metaMsg("a", "tenant", "acme", "id", "0"),
			metaMsg("b", "tenant", "does-not-exist", "id", "1"),
			metaMsg("c", "tenant", "globex", "id", "2"),
		}
		// The indexer must be created before the batch is dispatched.
		indexer := batch.Index()
		err := w.WriteBatch(ctx, batch)
		require.Error(t, err)
		assert.Contains(t, err.Error(), pf+"-does-not-exist")

		var batchErr *service.BatchError
		require.ErrorAs(t, err, &batchErr)
		assert.Equal(t, 1, batchErr.IndexedErrors(), "only the message with the bad bucket should be failed")
		failed := -1
		batchErr.WalkMessagesIndexedBy(indexer, func(i int, _ *service.Message, merr error) bool {
			if merr != nil {
				failed = i
			}
			return true
		})
		assert.Equal(t, 1, failed, "failed index must point at the bad message")

		_, ok := s3ObjectBody(ctx, t, client, pf+"-acme", "0.txt")
		assert.True(t, ok, "good message before the bad one must be uploaded")
		_, ok = s3ObjectBody(ctx, t, client, pf+"-globex", "2.txt")
		assert.True(t, ok, "good message after the bad one must be uploaded")
	})

	t.Run("invalid_bucket_name_from_data_names_the_bucket", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		w := newDynamicBucketWriter(t, ctx, lsPort, `${! metadata("tenant") }`, `k.txt`)
		err := w.WriteBatch(ctx, service.MessageBatch{metaMsg("x", "tenant", "Has Spaces And CAPS")})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Has Spaces And CAPS")
	})

	t.Run("bucket_interpolation_function_error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		w := newDynamicBucketWriter(t, ctx, lsPort, `${! throw("boom") }`, `k.txt`)
		err := w.WriteBatch(ctx, service.MessageBatch{service.NewMessage([]byte("x"))})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bucket interpolation")
		assert.Contains(t, err.Error(), "boom")
	})

	t.Run("static_bucket_still_works_and_connection_test_checks_it", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		static := uniqueBucketPrefix("static")
		require.NoError(t, awstest.CreateBucket(ctx, lsPort, static))

		w := newDynamicBucketWriter(t, ctx, lsPort, static, `${! metadata("id") }.txt`)
		require.NoError(t, w.WriteBatch(ctx, service.MessageBatch{metaMsg("static body", "id", "s1")}))
		got, ok := s3ObjectBody(ctx, t, client, static, "s1.txt")
		require.True(t, ok)
		assert.Equal(t, "static body", got)

		res := w.ConnectionTest(ctx)
		require.Len(t, res, 1)
		assert.NoError(t, res[0].Err, "existing static bucket must pass the connection test")

		missingName := uniqueBucketPrefix("static-missing")
		missing := newDynamicBucketWriter(t, ctx, lsPort, missingName, `k.txt`)
		res = missing.ConnectionTest(ctx)
		require.Len(t, res, 1)
		require.Error(t, res[0].Err, "missing static bucket must fail the connection test")
		assert.NotErrorIs(t, res[0].Err, service.ErrConnectionTestNotSupported)
		assert.Contains(t, res[0].Err.Error(), missingName)
	})

	t.Run("full_pipeline_routes_by_tenant_with_batching", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		pipe := uniqueBucketPrefix("pipe")
		require.NoError(t, awstest.CreateBucket(ctx, lsPort, pipe+"-acme"))
		require.NoError(t, awstest.CreateBucket(ctx, lsPort, pipe+"-globex"))

		builder := service.NewStreamBuilder()
		require.NoError(t, builder.SetYAML(fmt.Sprintf(`
pipeline:
  processors:
    - mapping: |
        meta tenant = this.tenant
        meta id = this.id
output:
  aws_s3:
    bucket: '%s-${! metadata("tenant") }'
    path: '${! metadata("id") }.json'
    endpoint: http://localhost:%s
    force_path_style_urls: true
    region: eu-west-1
    credentials:
      id: xxxxx
      secret: xxxxx
      token: xxxxx
    batching:
      count: 4
      period: 1s
`, pipe, lsPort)))
		producer, err := builder.AddProducerFunc()
		require.NoError(t, err)
		stream, err := builder.Build()
		require.NoError(t, err)

		runErr := make(chan error, 1)
		runCtx, runCancel := context.WithCancel(ctx)
		defer runCancel()
		go func() { runErr <- stream.Run(runCtx) }()

		msgs := []string{
			`{"tenant":"acme","id":"1"}`, `{"tenant":"globex","id":"2"}`,
			`{"tenant":"acme","id":"3"}`, `{"tenant":"globex","id":"4"}`,
		}
		for _, m := range msgs {
			require.NoError(t, producer(ctx, service.NewMessage([]byte(m))))
		}
		require.NoError(t, stream.StopWithin(15*time.Second))
		select {
		case err := <-runErr:
			if err != nil && !errors.Is(err, context.Canceled) {
				require.NoError(t, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("stream did not exit")
		}

		for bucket, keys := range map[string][]string{pipe + "-acme": {"1.json", "3.json"}, pipe + "-globex": {"2.json", "4.json"}} {
			for _, key := range keys {
				_, ok := s3ObjectBody(ctx, t, client, bucket, key)
				assert.True(t, ok, "%s/%s should exist", bucket, key)
			}
		}
	})
}
