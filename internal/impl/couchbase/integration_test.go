package couchbase_test

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"
)

var (
	username           = "benthos"
	password           = "password"
	port               = ""
	integrationCleanup func() error
	integrationOnce    sync.Once
)

// TestMain cleanup couchbase cluster if required by tests.
func TestMain(m *testing.M) {
	code := m.Run()
	if integrationCleanup != nil {
		if err := integrationCleanup(); err != nil {
			panic(err)
		}
	}

	os.Exit(code)
}

func requireCouchbase(tb testing.TB) string {
	integrationOnce.Do(func() {
		pool, resource, err := setupCouchbase(tb)
		require.NoError(tb, err)

		port = resource.GetPort("11210/tcp")
		integrationCleanup = func() error {
			return pool.Purge(resource)
		}
	})

	return port
}

func setupCouchbase(tb testing.TB) (*dockertest.Pool, *dockertest.Resource, error) {
	tb.Log("setup couchbase cluster")

	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, nil, err
	}

	pwd, err := os.Getwd()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get working directory: %s", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "couchbase",
		Tag:        "latest",
		Cmd:        []string{"/opt/couchbase/configure-server.sh"},
		Env: []string{
			"CLUSTER_NAME=couchbase",
			fmt.Sprintf("COUCHBASE_ADMINISTRATOR_USERNAME=%s", username),
			fmt.Sprintf("COUCHBASE_ADMINISTRATOR_PASSWORD=%s", password),
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
	if err != nil {
		return nil, nil, err
	}

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
	}

	tb.Log("couchbase cluster ready")

	return pool, resource, nil
}
