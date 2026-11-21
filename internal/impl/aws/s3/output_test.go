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
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// TestObjectCannedACLConfigParsing exercises the full parse path for the
// object_canned_acl field: schema lint plus the runtime validator in
// s3oConfigFromParsed. It guards the regression fixed in PR #4413 where the
// default value "" was rejected by the validator.
func TestObjectCannedACLConfigParsing(t *testing.T) {
	const baseConfig = `
bucket: foo
region: eu-west-1
credentials:
  id: xxxxx
  secret: xxxxx
`

	tests := []struct {
		name        string
		aclYAML     string
		expectedACL types.ObjectCannedACL
	}{
		{
			name:        "field omitted uses empty default",
			aclYAML:     "",
			expectedACL: types.ObjectCannedACL(""),
		},
		{
			name:        "explicit empty string",
			aclYAML:     `object_canned_acl: ""`,
			expectedACL: types.ObjectCannedACL(""),
		},
		{
			name:        "valid canned ACL bucket-owner-full-control",
			aclYAML:     `object_canned_acl: bucket-owner-full-control`,
			expectedACL: types.ObjectCannedACLBucketOwnerFullControl,
		},
		{
			name:        "valid canned ACL private",
			aclYAML:     `object_canned_acl: private`,
			expectedACL: types.ObjectCannedACLPrivate,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			yaml := baseConfig + tt.aclYAML + "\n"

			parsed, err := s3oOutputSpec().ParseYAML(yaml, nil)
			require.NoError(t, err)

			conf, err := s3oConfigFromParsed(parsed)
			require.NoError(t, err)
			assert.Equal(t, tt.expectedACL, conf.ObjectCannedACL)
		})
	}
}

// TestObjectCannedACLLinting verifies that the schema's enum linter accepts
// the empty string as well as valid SDK canned ACL values, and rejects bogus
// ones. This locks in the agreement between the schema and the runtime
// validator after PR #4413.
func TestObjectCannedACLLinting(t *testing.T) {
	const baseConfig = `
aws_s3:
  bucket: foo
  region: eu-west-1
  credentials:
    id: xxxxx
    secret: xxxxx
`

	tests := []struct {
		name         string
		aclYAML      string
		lintContains string // empty means: expect no lint errors
	}{
		{
			name:    "field omitted",
			aclYAML: "",
		},
		{
			name:    "explicit empty string",
			aclYAML: `  object_canned_acl: ""`,
		},
		{
			name:    "valid canned ACL bucket-owner-full-control",
			aclYAML: `  object_canned_acl: bucket-owner-full-control`,
		},
		{
			name:    "valid canned ACL private",
			aclYAML: `  object_canned_acl: private`,
		},
		{
			name:         "invalid canned ACL value is rejected",
			aclYAML:      `  object_canned_acl: not-a-real-acl`,
			lintContains: "not-a-real-acl",
		},
	}

	linter := service.GlobalEnvironment().NewComponentConfigLinter()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			yaml := baseConfig + tt.aclYAML + "\n"
			lints, err := linter.LintOutputYAML([]byte(yaml))
			require.NoError(t, err)

			if tt.lintContains == "" {
				assert.Empty(t, lints, "expected no lint errors, got: %v", lints)
				return
			}

			require.NotEmpty(t, lints, "expected a lint error containing %q", tt.lintContains)
			var combined strings.Builder
			for _, l := range lints {
				fmt.Fprintf(&combined, "%v\n", l)
			}
			assert.Contains(t, combined.String(), tt.lintContains)
		})
	}
}

func TestBucketInterpolationConfigParsing(t *testing.T) {
	const baseConfig = `
region: eu-west-1
credentials:
  id: xxxxx
  secret: xxxxx
`

	tests := []struct {
		name         string
		bucketYAML   string
		expectStatic bool
		errContains  string
	}{
		{
			name:         "static bucket",
			bucketYAML:   `bucket: foo`,
			expectStatic: true,
		},
		{
			name:       "interpolated bucket",
			bucketYAML: `bucket: '${! metadata("client_id") }-logs'`,
		},
		{
			name:        "empty static bucket is rejected",
			bucketYAML:  `bucket: ""`,
			errContains: "must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := s3oOutputSpec().ParseYAML(tt.bucketYAML+"\n"+baseConfig, nil)
			require.NoError(t, err)

			conf, err := s3oConfigFromParsed(parsed)
			if tt.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
				return
			}
			require.NoError(t, err)

			_, isStatic := conf.Bucket.Static()
			assert.Equal(t, tt.expectStatic, isStatic)
		})
	}
}

func TestBucketInterpolationResolvesPerMessage(t *testing.T) {
	parsed, err := s3oOutputSpec().ParseYAML(`
bucket: '${! metadata("client_id") }-logs'
region: eu-west-1
credentials:
  id: xxxxx
  secret: xxxxx
`, nil)
	require.NoError(t, err)

	conf, err := s3oConfigFromParsed(parsed)
	require.NoError(t, err)

	batch := service.MessageBatch{service.NewMessage(nil), service.NewMessage(nil)}
	batch[0].MetaSetMut("client_id", "acme")
	batch[1].MetaSetMut("client_id", "globex")

	for i, expected := range []string{"acme-logs", "globex-logs"} {
		got, err := batch.TryInterpolatedString(i, conf.Bucket)
		require.NoError(t, err)
		assert.Equal(t, expected, got)
	}
}

func TestBucketInterpolationConnectionTestDynamic(t *testing.T) {
	parsed, err := s3oOutputSpec().ParseYAML(`
bucket: '${! metadata("client_id") }-logs'
region: eu-west-1
credentials:
  id: xxxxx
  secret: xxxxx
`, nil)
	require.NoError(t, err)

	conf, err := s3oConfigFromParsed(parsed)
	require.NoError(t, err)

	w, err := newAmazonS3Writer(conf, service.MockResources())
	require.NoError(t, err)

	res := w.ConnectionTest(t.Context())
	require.Len(t, res, 1)
	assert.ErrorIs(t, res[0].Err, service.ErrConnectionTestNotSupported)
}

// TestBucketInDifferentRegionFailsWithBucketName reproduces the S3 response for
// a bucket that lives in a region other than the one the client is configured
// for. The output uses a single client, so such a bucket cannot be written to,
// and the error must say which bucket failed without affecting other messages.
func TestBucketInDifferentRegionFailsWithBucketName(t *testing.T) {
	var mu sync.Mutex
	hits := map[string]int{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		mu.Lock()
		hits[bucket]++
		mu.Unlock()
		_, _ = io.Copy(io.Discard, r.Body)

		if bucket == "other-region-bucket" {
			w.Header().Set("x-amz-bucket-region", "us-west-2")
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusMovedPermanently)
			_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>PermanentRedirect</Code><Message>The bucket you are attempting to access must be addressed using the specified endpoint. Please send all future requests to this endpoint.</Message><Endpoint>other-region-bucket.s3.us-west-2.amazonaws.com</Endpoint><Bucket>other-region-bucket</Bucket></Error>`)
			return
		}
		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	parsed, err := s3oOutputSpec().ParseYAML(fmt.Sprintf(`
bucket: '${! metadata("bucket") }'
path: key.txt
endpoint: %s
force_path_style_urls: true
region: eu-west-1
credentials:
  id: xxxxx
  secret: xxxxx
  token: xxxxx
`, srv.URL), nil)
	require.NoError(t, err)

	conf, err := s3oConfigFromParsed(parsed)
	require.NoError(t, err)

	w, err := newAmazonS3Writer(conf, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, w.Connect(t.Context()))

	batch := service.MessageBatch{
		service.NewMessage([]byte("a")),
		service.NewMessage([]byte("b")),
	}
	batch[0].MetaSetMut("bucket", "same-region-bucket")
	batch[1].MetaSetMut("bucket", "other-region-bucket")
	indexer := batch.Index()

	start := time.Now()
	err = w.WriteBatch(t.Context(), batch)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "uploading to bucket other-region-bucket")
	assert.Contains(t, err.Error(), "PermanentRedirect")
	assert.Less(t, elapsed, 10*time.Second, "a redirect must fail fast, not retry for long")

	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)
	assert.Equal(t, 1, batchErr.IndexedErrors(), "only the other-region message should fail")
	failed := -1
	batchErr.WalkMessagesIndexedBy(indexer, func(i int, _ *service.Message, merr error) bool {
		if merr != nil {
			failed = i
		}
		return true
	})
	assert.Equal(t, 1, failed)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, 1, hits["same-region-bucket"], "the good bucket must be written exactly once")
	t.Logf("requests per bucket: %v, elapsed: %v", hits, elapsed)
}

// TestBucketInterpolationFailsBeforeUpload covers the bucket expressions that
// cannot be resolved into a bucket name. They fail before any request is sent,
// so no S3 server is needed.
func TestBucketInterpolationFailsBeforeUpload(t *testing.T) {
	tests := []struct {
		name        string
		bucket      string
		meta        map[string]any
		errContains []string
	}{
		{
			name:        "empty bucket name",
			bucket:      `${! metadata("tenant") }`,
			meta:        map[string]any{"tenant": ""},
			errContains: []string{"empty bucket name"},
		},
		{
			name:        "interpolation function fails",
			bucket:      `${! throw("boom") }`,
			errContains: []string{"bucket interpolation", "boom"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := s3oOutputSpec().ParseYAML(fmt.Sprintf(`
bucket: '%s'
endpoint: http://127.0.0.1:1
force_path_style_urls: true
region: eu-west-1
credentials:
  id: xxxxx
  secret: xxxxx
`, tt.bucket), nil)
			require.NoError(t, err)

			conf, err := s3oConfigFromParsed(parsed)
			require.NoError(t, err)

			w, err := newAmazonS3Writer(conf, service.MockResources())
			require.NoError(t, err)
			require.NoError(t, w.Connect(t.Context()))

			msg := service.NewMessage([]byte("x"))
			for k, v := range tt.meta {
				msg.MetaSetMut(k, v)
			}

			err = w.WriteBatch(t.Context(), service.MessageBatch{msg})
			require.Error(t, err)
			for _, want := range tt.errContains {
				assert.Contains(t, err.Error(), want)
			}
		})
	}
}
