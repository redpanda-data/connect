package integration

import (
	"testing"
	"time"

	sftpSetup "github.com/Jeffail/benthos/v3/internal/service/sftp"
	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var sftpUsername = "foo"
var sftpPassword = "pass"

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

	creds := sftpSetup.Credentials{
		Username: sftpUsername,
		Password: sftpPassword,
	}

	require.NoError(t, pool.Retry(func() error {
		_, err = creds.GetClient("localhost:" + resource.GetPort("22/tcp"))
		return err
	}))

	t.Run("sftp", func(t *testing.T) {
		template := `
output:
  sftp:
    address: localhost:$PORT
    path: /upload/test-$ID/${!uuid_v4()}.txt
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
      enabled: $VAR2
      minimum_age: 100ms
      poll_interval: 100ms
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
			testOptPort(resource.GetPort("22/tcp")),
			testOptVarOne("all-bytes"),
			testOptVarTwo("false"),
		)

		watcherSuite := integrationTests(
			integrationTestOpenClose(),
			integrationTestStreamParallel(50),
			integrationTestStreamSequential(20),
			integrationTestStreamParallelLossyThroughReconnect(20),
		)
		watcherSuite.Run(
			t, template,
			testOptPort(resource.GetPort("22/tcp")),
			testOptVarOne("all-bytes"),
			testOptVarTwo("true"),
		)
	})
})
