package couchbase_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/bxcodec/faker/v3"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
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

func TestProcessorIntegration(t *testing.T) {
	integration.CheckSkip(t)

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = 30 * time.Second
	if deadline, ok := t.Deadline(); ok {
		pool.MaxWait = time.Until(deadline) - 100*time.Millisecond
	}

	pwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("failed to get working directory: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "couchbase",
		Tag:        "latest",
		Cmd:        []string{"/opt/couchbase/configure-server.sh"},
		Env: []string{
			"CLUSTER_NAME=couchbase",
			"COUCHBASE_ADMINISTRATOR_USERNAME=benthos",
			"COUCHBASE_ADMINISTRATOR_PASSWORD=password",
		},
		Mounts: []string{
			fmt.Sprintf("%s/testdata/configure-server.sh:/opt/couchbase/configure-server.sh", pwd),
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"8091/tcp": {
				{
					HostIP: "0.0.0.0", HostPort: "8091",
				},
			},
			"11210/tcp": {
				{
					HostIP: "0.0.0.0", HostPort: "11210",
				},
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	// Look for readyness
	var stderr bytes.Buffer
	time.Sleep(15 * time.Second)
	for {
		time.Sleep(time.Second)
		exitCode, err := resource.Exec([]string{"/usr/bin/cat", "/is-ready"}, dockertest.ExecOptions{
			StdErr: &stderr, // without stderr exit code is not reported
		})
		if exitCode == 0 && err == nil {
			break
		}
		t.Logf("exit code: %d, err: %s", exitCode, err)
		errB, err := io.ReadAll(&stderr)
		require.NoError(t, err)
		t.Logf("stderr: %s", string(errB))
	}

	t.Logf("couchbase cluster is ready")

	port := resource.GetPort("11210/tcp")
	require.NotEmpty(t, port)

	uid := faker.UUIDHyphenated()
	payload := fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())

	t.Run("Insert", func(t *testing.T) {
		testCouchbaseProcessorInsert(uid, payload, port, t)
	})
	t.Run("Get", func(t *testing.T) {
		testCouchbaseProcessorGet(uid, payload, port, t)
	})
	t.Run("Remove", func(t *testing.T) {
		testCouchbaseProcessorRemove(uid, port, t)
	})
	t.Run("GetMissing", func(t *testing.T) {
		testCouchbaseProcessorGetMissing(uid, port, t)
	})

	payload = fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())
	t.Run("Upsert", func(t *testing.T) {
		testCouchbaseProcessorUpsert(uid, payload, port, t)
	})
	t.Run("Get", func(t *testing.T) {
		testCouchbaseProcessorGet(uid, payload, port, t)
	})

	payload = fmt.Sprintf(`{"id": %q, "data": %q}`, uid, faker.Sentence())
	t.Run("Replace", func(t *testing.T) {
		testCouchbaseProcessorReplace(uid, payload, port, t)
	})
	t.Run("Get", func(t *testing.T) {
		testCouchbaseProcessorGet(uid, payload, port, t)
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

func testCouchbaseProcessorInsert(uid, payload, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: 'testing'
username: benthos
password: password
id: '${! json("id") }'
content: 'root = this'
operation: 'insert'
`, port)

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

func testCouchbaseProcessorUpsert(uid, payload, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: 'testing'
username: benthos
password: password
id: '${! json("id") }'
content: 'root = this'
operation: 'upsert'
`, port)

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

func testCouchbaseProcessorReplace(uid, payload, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: 'testing'
username: benthos
password: password
id: '${! json("id") }'
content: 'root = this'
operation: 'replace'
`, port)

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

func testCouchbaseProcessorGet(uid, payload, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: 'testing'
username: benthos
password: password
id: '${! content() }'
operation: 'get'
`, port)

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

func testCouchbaseProcessorRemove(uid, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: 'testing'
username: benthos
password: password
id: '${! content() }'
operation: 'remove'
`, port)

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

func testCouchbaseProcessorGetMissing(uid, port string, t *testing.T) {
	config := fmt.Sprintf(`
url: 'couchbase://localhost:%s'
bucket: 'testing'
username: benthos
password: password
id: '${! content() }'
operation: 'get'
`, port)

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
