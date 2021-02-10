package integration

import (
	"log"
	"testing"
	"time"

	sftpSetup "github.com/Jeffail/benthos/v3/internal/service/sftp"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/ory/dockertest/v3"
	"github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	dialTimeout = 10 * time.Second
)

var sftpUsername = "foo"
var sftpPassword = "pass"

var sftpClient *sftp.Client
var sftpPort string

var _ = registerIntegrationTest("sftp", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "atmoz/sftp",
		Tag:        "alpine",
		Cmd: []string{
			sftpUsername + ":" + sftpPassword + ":1001:100:upload",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	resource.Expire(900)
	require.NoError(t, pool.Retry(func() error {
		return nil
	}))

	resource.Expire(900)

	creds := sftpSetup.Credentials{
		Username: sftpUsername,
		Password: sftpPassword,
	}

	sftpPort = resource.GetPort("22/tcp")
	sftpEndpoint := "localhost:" + sftpPort
	deadline := time.Now().Add(dialTimeout)
	for time.Now().Before(deadline) {
		t.Logf("waiting for SFTP server to come up on '%v'...", "localhost:"+sftpPort)

		sftpClient, err = creds.GetClient(sftpEndpoint)
		if err != nil {
			t.Logf("err: %v", err)
			time.Sleep(time.Second)
			continue
		}

		break
	}

	t.Run("sftp", func(t *testing.T) {
		template := `
output:
  sftp:
    address: localhost:$PORT
    path: /upload/test-$ID/$VAR2.txt
    credentials:
      username: foo
      password: pass
    codec: $VAR1
    max_in_flight: 1

input:
  sftp:
    address: localhost:$PORT
    paths:
      - /upload/test-$ID/*.txt
    credentials:
      username: foo
      password: pass
    codec: $VAR1
    delete_on_finish: false
    watcher:
      enabled: $VAR3
      poll_interval: 1s
      cache: files-memory

resources:
  caches:
    files-memory:
      memory:
        ttl: 900
`
		suite := integrationTests(
			integrationTestOpenCloseIsolated(),
			integrationTestStreamIsolated(100),
		)
		suite.Run(
			t, template,
			testOptPort(sftpPort),
			testOptVarOne("all-bytes"),
			testOptVarTwo(`${!count("$ID")}`),
			testOptVarThree("false"),
		)
		suite.Run(
			t, template,
			testOptPort(sftpPort),
			testOptVarOne("lines"),
			testOptVarTwo(`all-in-one-file`),
			testOptVarThree("false"),
		)

		watcherSuite := integrationTests(
			integrationTestWatcherMode(),
		)
		watcherSuite.Run(
			t, template,
			testOptPort(sftpPort),
			testOptVarOne("all-bytes"),
			testOptVarTwo(`${!count("$ID")}`),
			testOptVarThree("true"),
		)
		watcherSuite.Run(
			t, template,
			testOptPort(sftpPort),
			testOptVarOne("lines"),
			testOptVarTwo(`all-in-one-file`),
			testOptVarThree("true"),
		)
	})
})

func integrationTestWatcherMode() testDefinition {
	return namedTest(
		"test watcher mode",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			output := initOutput(t, tranChan, env)
			t.Cleanup(func() {
				closeConnectors(t, nil, output)
			})
			require.NoError(t, sendMessage(env.ctx, t, tranChan, "hello world"))

			input := initInput(t, env)
			t.Cleanup(func() {
				closeConnectors(t, input, nil)
			})
			messageMatch(t, receiveMessage(env.ctx, t, input.TransactionChan(), nil), "hello world")

			generateTestFile("/upload/test-"+env.configVars.id+"/watcher_test.txt", "hello world 2")
			messageMatch(t, receiveMessage(env.ctx, t, input.TransactionChan(), nil), "hello world 2")

			input.CloseAsync()
		},
	)
}

func generateTestFile(filepath string, data string) {
	file, err := sftpClient.Create(filepath)
	if err != nil {
		log.Fatalf("Error creating file %s on SSH server", filepath)
		return
	}
	_, err = file.Write([]byte(data))
	if err != nil {
		log.Fatalf("Error writing to file %s on SSH server", filepath)
	}
}
