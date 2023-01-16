package couchbase_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bxcodec/faker/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/impl/couchbase"
	"github.com/benthosdev/benthos/v4/internal/integration"
	"github.com/benthosdev/benthos/v4/public/service"
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
	require.NoError(t, createBucket(context.Background(), t, servicePort, bucket))
	t.Cleanup(func() {
		require.NoError(t, removeBucket(context.Background(), t, servicePort, bucket))
	})

	uid := faker.UUIDHyphenated()
	payload := fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())

	t.Run("Insert", func(t *testing.T) {
		testCouchbaseProcessorInsert(uid, payload, bucket, servicePort, t)
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
		testCouchbaseProcessorUpsert(uid, payload, bucket, servicePort, t)
	})
	t.Run("Get", func(t *testing.T) {
		testCouchbaseProcessorGet(uid, payload, bucket, servicePort, t)
	})

	payload = fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())
	t.Run("Replace", func(t *testing.T) {
		testCouchbaseProcessorReplace(uid, payload, bucket, servicePort, t)
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

func testCouchbaseProcessorInsert(uid, payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'insert'
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
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

func testCouchbaseProcessorUpsert(uid, payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'upsert'
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
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

func testCouchbaseProcessorReplace(uid, payload, bucket, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: %s
username: %s
password: %s
id: '${! json("id") }'
content: 'root = this'
operation: 'replace'
`, port, bucket, username, password)

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
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

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
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

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
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

	msgOut, err := getProc(t, config).ProcessBatch(context.Background(), service.MessageBatch{
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
