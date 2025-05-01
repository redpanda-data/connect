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

package sftp

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	// Bring in memory cache.
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
)

var (
	sftpUsername = "foo"
	sftpPassword = "pass"
)

func TestIntegrationSFTP(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	resource := setupDockerPool(t)

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
      cache: files_memory

cache_resources:
  - label: files_memory
    memory:
      default_ttl: 900s
`
		suite := integration.StreamTests(
			integration.StreamTestOpenCloseIsolated(),
			integration.StreamTestStreamIsolated(100),
		)
		suite.Run(
			t, template,
			integration.StreamTestOptPort(resource.GetPort("22/tcp")),
			integration.StreamTestOptVarSet("VAR1", "all-bytes"),
			integration.StreamTestOptVarSet("VAR2", "false"),
		)

		t.Run("watcher", func(t *testing.T) {
			watcherSuite := integration.StreamTests(
				integration.StreamTestOpenClose(),
				integration.StreamTestStreamParallel(50),
				integration.StreamTestStreamSequential(20),
				integration.StreamTestStreamParallelLossyThroughReconnect(20),
			)
			watcherSuite.Run(
				t, template,
				integration.StreamTestOptPort(resource.GetPort("22/tcp")),
				integration.StreamTestOptVarSet("VAR1", "all-bytes"),
				integration.StreamTestOptVarSet("VAR2", "true"),
			)
		})
	})
}

func TestIntegrationSFTPDeleteOnFinish(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	resource := setupDockerPool(t)

	client, err := getClient(resource)
	require.NoError(t, err)

	writeSFTPFile(t, client, "/upload/1.txt", "data-1")
	writeSFTPFile(t, client, "/upload/2.txt", "data-2")
	writeSFTPFile(t, client, "/upload/3.txt", "data-3")

	config := `
output:
  drop: {}

input:
  sftp:
    address: localhost:$PORT
    paths:
      - /upload/*.txt
    credentials:
      username: foo
      password: pass
    delete_on_finish: true
    watcher:
      enabled: true
      poll_interval: 100ms
      cache: files_memory

cache_resources:
  - label: files_memory
    memory:
      default_ttl: 900s
`
	config = strings.NewReplacer(
		"$PORT", resource.GetPort("22/tcp"),
	).Replace(config)

	var receivedPathsMut sync.Mutex
	var receivedPaths []string

	builder := service.NewStreamBuilder()
	require.NoError(t, builder.SetYAML(config))
	require.NoError(t, builder.AddConsumerFunc(func(_ context.Context, msg *service.Message) error {
		receivedPathsMut.Lock()
		defer receivedPathsMut.Unlock()
		path, ok := msg.MetaGet("sftp_path")
		if !ok {
			return errors.New("sftp_path metadata not found")
		}
		receivedPaths = append(receivedPaths, path)
		return nil
	}))
	stream, err := builder.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	runErr := make(chan error)
	go func() { runErr <- stream.Run(ctx) }()
	defer func() {
		cancel()
		err := <-runErr
		if err != context.Canceled {
			require.NoError(t, err, "stream.Run() failed")
		}
	}()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		receivedPathsMut.Lock()
		defer receivedPathsMut.Unlock()
		assert.Len(c, receivedPaths, 3)

		files, err := client.Glob("/upload/*.txt")
		assert.NoError(c, err)
		assert.Empty(c, files)
	}, time.Second*10, time.Millisecond*100)
}

func setupDockerPool(t *testing.T) *dockertest.Resource {
	t.Helper()

	pool, err := dockertest.NewPool("")
	require.NoError(t, err)

	pool.MaxWait = time.Second * 30
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "atmoz/sftp",
		Tag:        "alpine",
		Cmd: []string{
			// https://github.com/atmoz/sftp/issues/401
			"/bin/sh", "-c", "ulimit -n 65535 && exec /entrypoint " + sftpUsername + ":" + sftpPassword + ":1001:100:upload",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	// wait for server to be ready to accept connections
	require.NoError(t, pool.Retry(func() error {
		_, err := getClient(resource)
		return err
	}))

	return resource
}
func getClient(resource *dockertest.Resource) (*sftp.Client, error) {
	creds := credentials{
		Username: sftpUsername,
		Password: sftpPassword,
	}
	return creds.GetClient(&osPT{}, "localhost:"+resource.GetPort("22/tcp"))
}

func writeSFTPFile(t *testing.T, client *sftp.Client, path, data string) {
	t.Helper()
	file, err := client.Create(path)
	require.NoError(t, err, "creating file")
	defer file.Close()
	_, err = fmt.Fprint(file, data, "writing file contents")
	require.NoError(t, err)
}

type osPT struct{}

func (o *osPT) Open(name string) (fs.File, error) {
	return os.Open(name)
}

func (o *osPT) OpenFile(name string, flag int, perm fs.FileMode) (fs.File, error) {
	return os.OpenFile(name, flag, perm)
}

func (o *osPT) Stat(name string) (fs.FileInfo, error) {
	return os.Stat(name)
}

func (o *osPT) Remove(name string) error {
	return os.Remove(name)
}

func (o *osPT) MkdirAll(path string, perm fs.FileMode) error {
	return os.MkdirAll(path, perm)
}
