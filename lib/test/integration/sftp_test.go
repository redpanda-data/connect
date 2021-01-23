package integration

import (
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ = registerIntegrationTest("sftp", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "atmoz/sftp",
		Tag:        "alpine",
		Cmd: []string{
			"foo:pass:1001:100:upload",
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
`
		suite := integrationTests(
			integrationTestOpenCloseIsolated(),
			integrationTestStreamIsolated(100),
		)
		suite.Run(
			t, template,
			testOptPort(resource.GetPort("22/tcp")),
			testOptVarOne("all-bytes"),
			testOptVarTwo(`${!count("$ID")}`),
		)
		suite.Run(
			t, template,
			testOptPort(resource.GetPort("22/tcp")),
			testOptVarOne("lines"),
			testOptVarTwo(`all-in-one-file`),
		)
	})
})
