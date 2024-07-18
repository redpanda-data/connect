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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/couchbase"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
)

func TestProcessorConfigLinting(t *testing.T) {
	configTests := []struct {
		name        string
		config      string
		errContains string
	}{
		{
			name: "get content not required",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'get'
`,
		},
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

func TestIntegrationCouchbaseProcessor(t *testing.T) {
	integration.CheckSkip(t)

	servicePort := requireCouchbase(t)

	bucket := fmt.Sprintf("testing-processor-%d", time.Now().Unix())
	require.NoError(t, createBucket(t.Context(), servicePort, bucket))
	t.Cleanup(func() {
		require.NoError(t, removeBucket(t.Context(), servicePort, bucket))
	})

	uid := faker.UUIDHyphenated()
	payload := fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())

	t.Run("Insert", func(t *testing.T) {
		testCouchbaseProcessorInsert(payload, bucket, servicePort, t)
	})
	t.Run("Get", func(t *testing.T) {
		testCouchbaseProcessorGet(uid, payload, bucket, servicePort, t)
	})
	t.Run("Remove", func(t *testing.T) {
		testCouchbaseProcessorRemove(uid, bucket, servicePort, t)
	})
	t.Run("GetMissing", func(t *testing.T) {
		testCouchbaseProcessorGetMissing(uid, bucket, servicePort, t)
	})

	payload = fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())
	t.Run("Upsert", func(t *testing.T) {
		testCouchbaseProcessorUpsert(payload, bucket, servicePort, t)
	})
	t.Run("Get", func(t *testing.T) {
		testCouchbaseProcessorGet(uid, payload, bucket, servicePort, t)
	})

	payload = fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())
	t.Run("Replace", func(t *testing.T) {
		testCouchbaseProcessorReplace(payload, bucket, servicePort, t)
	})
	t.Run("Get", func(t *testing.T) {
		testCouchbaseProcessorGet(uid, payload, bucket, servicePort, t)
	})
}

func getProc(tb testing.TB, config string) *couchbase.Processor {
	tb.Helper()

	confSpec := couchbase.ProcessorConfig()
	env := service.NewEnvironment()

	pConf, err := confSpec.ParseYAML(config, env)
	require.NoError(tb, err)
	proc, err := couchbase.NewProcessor(pConf, service.MockResources())
	require.NoError(tb, err)
	require.NotNil(tb, proc)

	return proc
}

func testCouchbaseProcessorInsert(payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'insert'
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// check CAS
	cas, ok := msgOut[0][0].MetaGetMut(couchbase.MetaCASKey)
	assert.True(t, ok)
	assert.NotEmpty(t, cas)

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.JSONEq(t, payload, string(dataOut))
}

func testCouchbaseProcessorUpsert(payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'upsert'
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// check CAS
	cas, ok := msgOut[0][0].MetaGetMut(couchbase.MetaCASKey)
	assert.True(t, ok)
	assert.NotEmpty(t, cas)

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.JSONEq(t, payload, string(dataOut))
}

func testCouchbaseProcessorReplace(payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'replace'
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// check CAS
	cas, ok := msgOut[0][0].MetaGetMut(couchbase.MetaCASKey)
	assert.True(t, ok)
	assert.NotEmpty(t, cas)

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.JSONEq(t, payload, string(dataOut))
}

func testCouchbaseProcessorGet(uid, payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! content() }'
operation: 'get'
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(uid)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// check CAS
	cas, ok := msgOut[0][0].MetaGetMut(couchbase.MetaCASKey)
	assert.True(t, ok)
	assert.NotEmpty(t, cas)

	// message should contain expected payload.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.JSONEq(t, payload, string(dataOut))
}

func testCouchbaseProcessorRemove(uid, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! content() }'
operation: 'remove'
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(uid)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// check CAS
	cas, ok := msgOut[0][0].MetaGetMut(couchbase.MetaCASKey)
	assert.True(t, ok)
	assert.NotEmpty(t, cas)

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.Equal(t, uid, string(dataOut))
}

func testCouchbaseProcessorGetMissing(uid, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! content() }'
operation: 'get'
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(uid)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// message should contain an error.
	assert.Error(t, msgOut[0][0].GetError())

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.Equal(t, uid, string(dataOut))
}

func TestIntegrationCouchbaseStream(t *testing.T) {
	ctx := context.Background()

	integration.CheckSkip(t)

	servicePort := requireCouchbase(t)
	bucket := fmt.Sprintf("testing-stream-%d", time.Now().Unix())
	require.NoError(t, createBucket(context.Background(), t, servicePort, bucket))
	t.Cleanup(func() {
		require.NoError(t, removeBucket(context.Background(), t, servicePort, bucket))
	})

	for _, clearCAS := range []bool{true, false} {
		t.Run(fmt.Sprintf("%t", clearCAS), func(t *testing.T) {
			streamOutBuilder := service.NewStreamBuilder()
			require.NoError(t, streamOutBuilder.SetLoggerYAML(`level: OFF`))

			inFn, err := streamOutBuilder.AddBatchProducerFunc()
			require.NoError(t, err)

			var outBatches []service.MessageBatch
			var outBatchMut sync.Mutex
			require.NoError(t, streamOutBuilder.AddBatchConsumerFunc(func(c context.Context, mb service.MessageBatch) error {
				outBatchMut.Lock()
				outBatches = append(outBatches, mb)
				outBatchMut.Unlock()
				return nil
			}))

			// insert
			require.NoError(t, streamOutBuilder.AddProcessorYAML(fmt.Sprintf(`
couchbase:
  url: 'couchbase://localhost:%s'
  bucket: %s
  username: %s
  password: %s
  id: '${! json("key") }'
  content: 'root = this'
  operation: 'insert'
`, servicePort, bucket, username, password)))

			if clearCAS { // ignore cas check
				require.NoError(t, streamOutBuilder.AddProcessorYAML(`
mapping: |
  meta couchbase_cas = deleted()
`))
			}

			// upsert
			require.NoError(t, streamOutBuilder.AddProcessorYAML(fmt.Sprintf(`
couchbase:
  url: 'couchbase://localhost:%s'
  bucket: %s
  username: %s
  password: %s
  id: '${! json("key") }'
  content: 'root = this'
  operation: 'upsert'
`, servicePort, bucket, username, password)))

			if clearCAS { // ignore cas check
				require.NoError(t, streamOutBuilder.AddProcessorYAML(`
mapping: |
  meta couchbase_cas = deleted()
`))
			}
			// remove
			require.NoError(t, streamOutBuilder.AddProcessorYAML(fmt.Sprintf(`
couchbase:
  url: 'couchbase://localhost:%s'
  bucket: %s
  username: %s
  password: %s
  id: '${! json("key") }'
  operation: 'remove'
`, servicePort, bucket, username, password)))

			streamOut, err := streamOutBuilder.Build()
			require.NoError(t, err)
			go func() {
				err = streamOut.Run(context.Background())
				require.NoError(t, err)
			}()

			require.NoError(t, inFn(ctx, service.MessageBatch{
				service.NewMessage([]byte(`{"key":"hello","value":"word"}`)),
			}))
			require.NoError(t, streamOut.StopWithin(time.Second*15))

			assert.Eventually(t, func() bool {
				outBatchMut.Lock()
				defer outBatchMut.Unlock()
				return len(outBatches) == 1
			}, time.Second*5, time.Millisecond*100)

			// batch processing should be fine and contain one message.
			assert.NoError(t, err)
			assert.Len(t, outBatches, 1)
			assert.Len(t, outBatches[0], 1)

			// message should contain an error.
			assert.NoError(t, outBatches[0][0].GetError())
		})
	}
}
