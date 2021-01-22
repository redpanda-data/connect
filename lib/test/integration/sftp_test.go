package integration

import (
	"log"
	"strconv"
	"testing"

	"github.com/ory/dockertest/v3"
)

var _ = registerIntegrationTest("sftp", func(t *testing.T) {
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "atmoz/sftp",
		Tag:        "alpine",
		Cmd: []string{
			"foo:pass:1001:100:upload",
		},
	})

	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	var sftpPort int

	if err := pool.Retry(func() error {
		sftpPort, err = strconv.Atoi(resource.GetPort("22/tcp"))
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	resource.Expire(900)

	t.Run("sftp", func(t *testing.T) {
		template := `
output:
  sftp:
    address: http://localhost:$VAR1
    path: $VAR2/test-$ID/${!count("$ID")}.txt
    credentials:
        username: foo
        password: pass
    max_in_flight: 1
    max_connection_attempts: 3
    retry_sleep_duration: 5s

input:
  sftp:
    address: http://localhost:$VAR1
    paths:
      - /$VAR2/test-$ID/*.txt
    credentials:
        username: foo
        password: pass
    codec: all-bytes
    delete_on_finish: false
    max_connection_attempts: 3
    retry_sleep_duration: 5s
`
		integrationTests(
			integrationTestOpenCloseIsolated(),
			integrationTestStreamIsolated(2),
		).Run(
			t, template,
			testOptVarOne(strconv.Itoa(sftpPort)),
			testOptVarTwo("upload"),
		)
	})
})
