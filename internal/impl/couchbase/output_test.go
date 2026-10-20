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
	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/couchbase"
)

func TestOutputConfigLinting(t *testing.T) {
	configTests := []struct {
		name        string
		config      string
		errContains string
	}{
		{
			name: "remove content not required",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'remove'
`,
		},
		{
			name: "missing insert content",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'insert'
`,
			errContains: `content must be set for insert, replace and upsert operations.`,
		},
		{
			name: "missing replace content",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'replace'
`,
			errContains: `content must be set for insert, replace and upsert operations.`,
		},
		{
			name: "missing upsert content",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'upsert'
`,
			errContains: `content must be set for insert, replace and upsert operations.`,
		},
		{
			name: "insert with content",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  content: 'root = this'
  operation: 'insert'
`,
		},
	}

	env := service.NewEnvironment()
	for _, test := range configTests {
		t.Run(test.name, func(t *testing.T) {
			strm := env.NewStreamBuilder()
			err := strm.AddProcessorYAML(test.config)
			if test.errContains == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			}
		})
	}
}

func TestIntegrationCouchbaseOutput(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := requireCouchbase(t)

	bucket := fmt.Sprintf("testing-output-%d", time.Now().Unix())
	require.NoError(t, createBucket(t.Context(), servicePort, bucket))
	t.Cleanup(func() {
		require.NoError(t, removeBucket(context.Background(), servicePort, bucket))
	})

	uid := faker.UUIDHyphenated()
	payload := fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())

	t.Run("Insert", func(t *testing.T) {
		testCouchbaseOutputInsert(payload, bucket, servicePort, t)
	})
	t.Run("Insert existing", func(t *testing.T) {
		testCouchbaseOutputInsertExisting(payload, bucket, servicePort, t)
	})
	t.Run("Remove", func(t *testing.T) {
		testCouchbaseOutputRemove(uid, bucket, servicePort, t)
	})

	payload = fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())
	t.Run("Upsert", func(t *testing.T) {
		testCouchbaseOutputUpsert(payload, bucket, servicePort, t)
	})

	payload = fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())
	t.Run("Replace", func(t *testing.T) {
		testCouchbaseOutputReplace(payload, bucket, servicePort, t)
	})
	t.Run("Upsert TTL", func(t *testing.T) {
		testCouchbaseOutputUpsertTTL(payload, bucket, servicePort, t)
	})
}

func getOutput(tb testing.TB, config string) service.BatchOutput {
	tb.Helper()

	confSpec := couchbase.ProcessorConfig()
	env := service.NewEnvironment()

	pConf, err := confSpec.ParseYAML(config, env)
	require.NoError(tb, err)
	output, err := couchbase.NewOutput(pConf, service.MockResources())
	require.NoError(tb, err)
	require.NotNil(tb, output)

	require.NoError(tb, output.Connect(tb.Context()))

	return output
}

func testCouchbaseOutputInsert(payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'insert'
`, port, bucket, username, password)

	err := getOutput(t, config).WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	assert.NoError(t, err)
}

// testCouchbaseOutputInsertExisting writes a batch with a new document and a document that already exists. Only the
// message of the existing document must fail.
func testCouchbaseOutputInsertExisting(existingPayload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'insert'
`, port, bucket, username, password)

	newPayload := fmt.Sprintf(`{"id": %q, "data": %q}`, faker.UUIDHyphenated(), faker.Sentence())
	batch := service.MessageBatch{
		service.NewMessage([]byte(newPayload)),
		service.NewMessage([]byte(existingPayload)),
	}

	indexer := batch.Index()

	err := getOutput(t, config).WriteBatch(t.Context(), batch)
	require.Error(t, err)

	var batchErr *service.BatchError
	require.ErrorAs(t, err, &batchErr)
	assert.Equal(t, 1, batchErr.IndexedErrors())

	var failed []int
	batchErr.WalkMessagesIndexedBy(indexer, func(i int, _ *service.Message, err error) bool {
		if err != nil {
			failed = append(failed, i)
			assert.ErrorIs(t, err, gocb.ErrDocumentExists)
		}
		return true
	})
	assert.Equal(t, []int{1}, failed)
}

func testCouchbaseOutputUpsert(payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'upsert'
`, port, bucket, username, password)

	err := getOutput(t, config).WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	assert.NoError(t, err)
}

func testCouchbaseOutputReplace(payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'replace'
`, port, bucket, username, password)

	err := getOutput(t, config).WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	assert.NoError(t, err)
}

func testCouchbaseOutputRemove(uid, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! content() }'
operation: 'remove'
`, port, bucket, username, password)

	err := getOutput(t, config).WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(uid)),
	})

	assert.NoError(t, err)
}

func testCouchbaseOutputUpsertTTL(payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'upsert'
ttl: 1s
`, port, bucket, username, password)

	err := getOutput(t, config).WriteBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})
	assert.NoError(t, err)

	time.Sleep(2 * time.Second)
}
