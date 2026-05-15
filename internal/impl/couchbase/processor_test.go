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

	"github.com/go-faker/faker/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	"github.com/redpanda-data/connect/v4/internal/impl/couchbase"
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
			errContains: `content must be set for insert, replace, upsert, increment and decrement operations.`,
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
			errContains: `content must be set for insert, replace, upsert, increment and decrement operations.`,
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
			errContains: `content must be set for insert, replace, upsert, increment and decrement operations.`,
		},
		{
			name: "insert with content",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  content: 'root = this'
  operation: 'insert'`,
		},
		{
			name: "increment",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'increment'
  content: '1'
`,
		},
		{
			name: "decrement",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'decrement'
  content: '1'
`,
		},
		{
			name: "increment without content",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'increment'
`,
			errContains: `content must be set for insert, replace, upsert, increment and decrement operations.`,
		},
		{
			name: "decrement without content",
			config: `
couchbase:
  url: 'url'
  bucket: 'bucket'
  id: '${! json("id") }'
  operation: 'decrement'
`,
			errContains: `content must be set for insert, replace, upsert, increment and decrement operations.`,
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
		require.NoError(t, removeBucket(context.Background(), servicePort, bucket))
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
	t.Run("TTL", func(t *testing.T) {
		testCouchbaseProcessorUpsertTTL(payload, bucket, servicePort, t)
		testCouchbaseProcessorGet(uid, payload, bucket, servicePort, t)
		time.Sleep(5 * time.Second)
		testCouchbaseProcessorGetMissing(uid, bucket, servicePort, t)
	})
	// counters tests (entry cleared by ttl before)
	t.Run("Increment non existing", func(t *testing.T) {
		testCouchbaseProcessorCounter(uid, "increment", "1", "1", bucket, servicePort, t)
	})
	t.Run("Decrement to minimal value", func(t *testing.T) { // minimum value of counter is zero
		testCouchbaseProcessorCounter(uid, "decrement", "2", "0", bucket, servicePort, t)
	})
	t.Run("Increment", func(t *testing.T) {
		testCouchbaseProcessorCounter(uid, "increment", "8", "8", bucket, servicePort, t)
	})
	t.Run("Decrement", func(t *testing.T) {
		testCouchbaseProcessorCounter(uid, "decrement", "2", "6", bucket, servicePort, t)
	})
	// noop only retrive value
	t.Run("Increment zero", func(t *testing.T) {
		testCouchbaseProcessorCounter(uid, "increment", "0", "6", bucket, servicePort, t)
	})
	t.Run("Decrement zero", func(t *testing.T) {
		testCouchbaseProcessorCounter(uid, "decrement", "0", "6", bucket, servicePort, t)
	})

	t.Run("Remove", func(t *testing.T) {
		testCouchbaseProcessorRemove(uid, bucket, servicePort, t)
	})
	t.Run("Decrement non existing with negative initial", func(t *testing.T) {
		testCouchbaseProcessorCounter(uid, "decrement", "-10", "10", bucket, servicePort, t)
	})

	t.Run("Error increment empty", func(t *testing.T) {
		testCouchbaseProcessorCounterError(uid, "increment", "", bucket, servicePort, t)
	})
	t.Run("Error decrement empty", func(t *testing.T) {
		testCouchbaseProcessorCounterError(uid, "decrement", "", bucket, servicePort, t)
	})
	t.Run("Error increment float", func(t *testing.T) {
		testCouchbaseProcessorCounterError(uid, "increment", "0.1", bucket, servicePort, t)
	})
	t.Run("Error decrement float", func(t *testing.T) {
		testCouchbaseProcessorCounterError(uid, "decrement", "0.1", bucket, servicePort, t)
	})
	t.Run("Error increment invalid", func(t *testing.T) {
		testCouchbaseProcessorCounterError(uid, "increment", "invalid", bucket, servicePort, t)
	})
	t.Run("Error decrement invalid", func(t *testing.T) {
		testCouchbaseProcessorCounterError(uid, "decrement", "invalid", bucket, servicePort, t)
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
	assert.Error(t, msgOut[0][0].GetError(), "TODO")

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.Equal(t, uid, string(dataOut))
}

func testCouchbaseProcessorUpsertTTL(payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'upsert'
ttl: 3s
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(t.Context(), service.MessageBatch{
		service.NewMessage([]byte(payload)),
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// message content should stay the same.
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	assert.JSONEq(t, payload, string(dataOut))
}

func testCouchbaseProcessorCounter(uid, operation, value, expected, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! meta("id") }'
content: 'root = this.or(null)'
operation: '%s'
`, port, bucket, username, password, operation)

	msg := service.NewMessage([]byte(value))
	msg.MetaSetMut("id", uid)
	msgOut, err := getProc(t, config).ProcessBatch(t.Context(), service.MessageBatch{
		msg,
	})

	// batch processing should be fine and contain one message.
	assert.NoError(t, err)
	assert.Len(t, msgOut, 1)
	assert.Len(t, msgOut[0], 1)

	// message content should be the counter value
	dataOut, err := msgOut[0][0].AsBytes()
	assert.NoError(t, err)
	// The result of increment is the new value.
	assert.Equal(t, expected, string(dataOut))
}

func testCouchbaseProcessorCounterError(uid, operation, value, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! meta("id") }'
content: 'root = this.or(null)'
operation: '%s'
`, port, bucket, username, password, operation)

	msg := service.NewMessage([]byte(value))
	msg.MetaSetMut("id", uid)
	_, err := getProc(t, config).ProcessBatch(t.Context(), service.MessageBatch{
		msg,
	})

	// batch processing should fail.
	require.Error(t, err)
}
