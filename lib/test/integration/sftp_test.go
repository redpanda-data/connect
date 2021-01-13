package integration

import (
	"log"
	"strconv"
	"testing"

	"github.com/ory/dockertest/v3"
)

var _ = registerIntegrationTest("sftp", func(t *testing.T) {
	//t.Skip()

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

	var sshPort int

	if err := pool.Retry(func() error {
		sshPort, err = strconv.Atoi(resource.GetPort("22/tcp"))
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
  stdout:
    delimiter: \n

input:
  sftp:
    server: localhost
    port: $VAR1
    filepath: upload/test.txt
    credentials:
        username: foo
        secret: pass
    watcher_mode: false
    process_existing_records: true
    include_header: true
    message_delimiter: \n
    max_connection_attempts: 10
    file_check_sleep_duration: 5
    file_check_max_attempts: 10
    directory_mode: false
`
		integrationTests(
			integrationTestOpenCloseIsolated(),
			integrationTestStreamIsolated(10),
		).Run(
			t, template,
			testOptVarOne(strconv.Itoa(sshPort)),
		)
	})
})
