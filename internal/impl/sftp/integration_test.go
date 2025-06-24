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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/integration"

	// Bring in memory cache.
	_ "github.com/redpanda-data/benthos/v4/public/components/pure"
)

var (
	sftpUsername = "admin"
	sftpPassword = "password"
)

func TestIntegrationSFTP(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	emulator := runEmulator(t)

	t.Run("sftp", func(t *testing.T) {
		template := `
output:
  sftp:
    address: $VAR1
    path: /upload/test-$ID/${!uuid_v4()}.txt
    credentials:
      username: $VAR2
      password: $VAR3
      host_public_key: $VAR4
    codec: all-bytes
    max_in_flight: 1

input:
  sftp:
    address: $VAR1
    paths:
      - /upload/test-$ID/*.txt
    credentials:
      username: $VAR2
      password: $VAR3
      host_public_key: $VAR4
    scanner:
      to_the_end: {}
    delete_on_finish: false
    watcher:
      enabled: $VAR5
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
			integration.StreamTestOptPort(emulator.address),
			integration.StreamTestOptVarSet("VAR1", emulator.address),
			integration.StreamTestOptVarSet("VAR2", sftpUsername),
			integration.StreamTestOptVarSet("VAR3", sftpPassword),
			integration.StreamTestOptVarSet("VAR4", emulator.hostKey),
			integration.StreamTestOptVarSet("VAR5", "false"),
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
				integration.StreamTestOptPort(emulator.address),
				integration.StreamTestOptVarSet("VAR1", emulator.address),
				integration.StreamTestOptVarSet("VAR2", sftpUsername),
				integration.StreamTestOptVarSet("VAR3", sftpPassword),
				integration.StreamTestOptVarSet("VAR4", emulator.hostKey),
				integration.StreamTestOptVarSet("VAR5", "true"),
			)
		})
	})
}

func TestIntegrationSFTPDeleteOnFinish(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	emulator := runEmulator(t)

	err := emulator.client.MkdirAll("/upload")
	require.NoError(t, err)

	writeSFTPFile(t, emulator.client, "/upload/1.txt", "data-1")
	writeSFTPFile(t, emulator.client, "/upload/2.txt", "data-2")
	writeSFTPFile(t, emulator.client, "/upload/3.txt", "data-3")

	config := `
output:
  drop: {}

input:
  sftp:
    address: $VAR1
    paths:
      - /upload/*.txt
    credentials:
      username: $VAR2
      password: $VAR3
      host_public_key: $VAR4
    scanner:
      to_the_end: {}
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
		"$VAR1", emulator.address,
		"$VAR2", sftpUsername,
		"$VAR3", sftpPassword,
		"$VAR4", emulator.hostKey,
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

		files, err := emulator.client.Glob("/upload/*.txt")
		assert.NoError(c, err)
		assert.Empty(c, files)
	}, time.Second*10, time.Millisecond*100)
}

type emulator struct {
	client  *sftp.Client
	address string
	hostKey string
}

func runEmulator(t *testing.T) emulator {
	pool, err := dockertest.NewPool("")
	require.NoError(t, err)
	pool.MaxWait = time.Second * 30

	adminUsername := "admin"
	adminPassword := "password"
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "drakkan/sftpgo",
		Tag:        "edge-alpine-slim",
		Env: []string{
			"SFTPGO_DATA_PROVIDER__CREATE_DEFAULT_ADMIN=true",
			"SFTPGO_DEFAULT_ADMIN_USERNAME=" + adminUsername,
			"SFTPGO_DEFAULT_ADMIN_PASSWORD=" + adminPassword,
		},
		ExposedPorts: []string{
			"2022/tcp",
			"8080/tcp",
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, pool.Purge(resource))
	})

	_ = resource.Expire(900)

	require.NoError(t, pool.Retry(func() error {
		resp, err := http.Get("http://" + resource.GetHostPort("8080/tcp") + "/healthz")
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("failed to query healthz, got status: %d", resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		if !bytes.Equal(body, []byte("ok")) {
			return errors.New("failed healthz check, expected 'ok' response, got %s" + string(body))
		}

		return nil
	}))

	// Get an access token for the admin user
	req, err := http.NewRequest(http.MethodGet, "http://"+resource.GetHostPort("8080/tcp")+"/api/v2/token", nil)
	require.NoError(t, err)
	req.SetBasicAuth(adminUsername, adminPassword)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var tokenResponse struct {
		AccessToken string `json:"access_token"`
	}
	require.NoError(t, json.Unmarshal(body, &tokenResponse))
	require.NotEmpty(t, tokenResponse.AccessToken)

	// Create a user for SFTP access
	req, err = http.NewRequest(
		http.MethodPost,
		"http://"+resource.GetHostPort("8080/tcp")+"/api/v2/users",
		strings.NewReader(
			fmt.Sprintf(
				`{"id": 1, "status": 1, "username": "%s", "password": "%s", "permissions": {"/": ["*"]}}`,
				sftpUsername, sftpPassword,
			),
		),
	)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tokenResponse.AccessToken)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	address := resource.GetHostPort("2022/tcp")
	var hostPubKey string
	var sshClient *ssh.Client
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var pubKey ssh.PublicKey
		cb := func(_ string, _ net.Addr, key ssh.PublicKey) error {
			pubKey = key
			return nil
		}

		var err error
		sshClient, err = ssh.Dial("tcp", address, &ssh.ClientConfig{
			User:            sftpUsername,
			Auth:            []ssh.AuthMethod{ssh.Password(sftpPassword)},
			HostKeyCallback: cb,
			Timeout:         2 * time.Second,
		})
		require.NoError(c, err)
		require.NotEmpty(c, pubKey)

		hostPubKey = string(ssh.MarshalAuthorizedKey(pubKey))
	}, time.Second*6, time.Millisecond*100)

	client, err := sftp.NewClient(sshClient)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, client.Close())
		require.NoError(t, sshClient.Close())
	})

	return emulator{
		client:  client,
		address: address,
		hostKey: hostPubKey,
	}
}

func writeSFTPFile(t *testing.T, client *sftp.Client, path, data string) {
	t.Helper()
	file, err := client.Create(path)
	require.NoError(t, err, "creating file")
	defer file.Close()
	_, err = fmt.Fprint(file, data, "writing file contents")
	require.NoError(t, err)
}
