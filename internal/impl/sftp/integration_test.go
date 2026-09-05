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

	"github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
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
	adminUsername := "admin"
	adminPassword := "password"

	ctr, err := testcontainers.Run(t.Context(), "drakkan/sftpgo:edge-alpine-slim",
		testcontainers.WithExposedPorts("2022/tcp", "8080/tcp"),
		testcontainers.WithEnv(map[string]string{
			"SFTPGO_DATA_PROVIDER__CREATE_DEFAULT_ADMIN": "true",
			"SFTPGO_DEFAULT_ADMIN_USERNAME":              adminUsername,
			"SFTPGO_DEFAULT_ADMIN_PASSWORD":              adminPassword,
		}),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/healthz").WithPort("8080/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	testcontainers.CleanupContainer(t, ctr)
	require.NoError(t, err)

	host, err := ctr.Host(t.Context())
	require.NoError(t, err)
	httpPort, err := ctr.MappedPort(t.Context(), "8080/tcp")
	require.NoError(t, err)
	sshPort, err := ctr.MappedPort(t.Context(), "2022/tcp")
	require.NoError(t, err)

	httpAddr := host + ":" + httpPort.Port()

	// Get an access token for the admin user
	req, err := http.NewRequest(http.MethodGet, "http://"+httpAddr+"/api/v2/token", nil)
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
		"http://"+httpAddr+"/api/v2/users",
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

	address := host + ":" + sshPort.Port()
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
	_, err = fmt.Fprint(file, data)
	require.NoError(t, err, "writing file contents")
}

// readSFTPInput builds an sftp input for the emulator and connects it.
func readSFTPInput(t *testing.T, emu emulator, path, scanner string, watcher bool) *sftpReader {
	t.Helper()
	conf := fmt.Sprintf(`
address: %s
paths:
  - %s
credentials:
  username: %s
  password: %s
  host_public_key: %s
scanner:
  %s: {}
watcher:
  enabled: %t
  minimum_age: 0s
  poll_interval: 100ms
  cache: files_memory
`, emu.address, path, sftpUsername, sftpPassword, emu.hostKey, scanner, watcher)

	parsed, err := sftpInputSpec().ParseYAML(conf, nil)
	require.NoError(t, err)

	reader, err := newSFTPReaderFromParsed(parsed, service.MockResources(service.MockResourcesOptAddCache("files_memory")))
	require.NoError(t, err)
	require.NoError(t, reader.Connect(t.Context()))
	t.Cleanup(func() { require.NoError(t, reader.Close(context.Background())) })
	return reader
}

// readOneFile reads one batch and fails the test if the input reports a lost
// connection. A file boundary is not a lost connection.
func readOneFile(t *testing.T, ctx context.Context, reader *sftpReader) string {
	t.Helper()
	batch, ackFn, err := reader.ReadBatch(ctx)
	require.NotErrorIs(t, err, service.ErrNotConnected, "file boundary must not be reported as a lost connection")
	require.NoError(t, err)
	require.Len(t, batch, 1)
	content, err := batch[0].AsBytes()
	require.NoError(t, err)
	require.NoError(t, ackFn(ctx, nil))
	return string(content)
}

func TestIntegrationSFTPReadBatchRotatesFiles(t *testing.T) {
	integration.CheckSkip(t)

	emu := runEmulator(t)
	require.NoError(t, emu.client.MkdirAll("/upload"))

	t.Run("static paths", func(t *testing.T) {
		dir := "/upload/static"
		require.NoError(t, emu.client.MkdirAll(dir))
		writeSFTPFile(t, emu.client, dir+"/1.txt", "data-1")
		writeSFTPFile(t, emu.client, dir+"/2.txt", "data-2")
		writeSFTPFile(t, emu.client, dir+"/3.txt", "data-3")

		reader := readSFTPInput(t, emu, dir+"/*.txt", "to_the_end", false)
		ctx := t.Context()

		// A cancelled context must stop the rotation before a file is opened.
		cancelledCtx, cancel := context.WithCancel(ctx)
		cancel()
		_, _, err := reader.ReadBatch(cancelledCtx)
		require.ErrorIs(t, err, context.Canceled)

		var contents []string
		for range 3 {
			contents = append(contents, readOneFile(t, ctx, reader))
		}
		// The SFTP server does not sort glob results, so only the set is checked.
		assert.ElementsMatch(t, []string{"data-1", "data-2", "data-3"}, contents)

		_, _, err = reader.ReadBatch(ctx)
		require.ErrorIs(t, err, service.ErrEndOfInput)
	})

	t.Run("empty files", func(t *testing.T) {
		// With the lines scanner an empty file yields EOF at once. The reader
		// must skip it inside one ReadBatch call, and an empty last file must
		// end the input instead of returning an empty batch.
		dir := "/upload/empty"
		require.NoError(t, emu.client.MkdirAll(dir))
		writeSFTPFile(t, emu.client, dir+"/1.txt", "data-1\n")
		writeSFTPFile(t, emu.client, dir+"/2.txt", "")
		writeSFTPFile(t, emu.client, dir+"/3.txt", "data-3\n")
		writeSFTPFile(t, emu.client, dir+"/4.txt", "")

		// Fix the order so the empty files sit where the test expects.
		// Needed because we want to test that the reader skips empty files, so the order matters.
		reader := readSFTPInput(t, emu, dir+"/*.txt", "lines", false)
		reader.pathProvider = &staticPathProvider{expandedPaths: []string{
			dir + "/1.txt", dir + "/2.txt", dir + "/3.txt", dir + "/4.txt",
		}}
		ctx := t.Context()

		var contents []string
		for {
			batch, ackFn, err := reader.ReadBatch(ctx)
			if errors.Is(err, service.ErrEndOfInput) {
				break
			}
			require.NotErrorIs(t, err, service.ErrNotConnected)
			require.NoError(t, err)
			require.NotEmpty(t, batch, "ReadBatch must not return an empty batch")
			for _, msg := range batch {
				content, err := msg.AsBytes()
				require.NoError(t, err)
				contents = append(contents, string(content))
			}
			require.NoError(t, ackFn(ctx, nil))
		}
		assert.Equal(t, []string{"data-1", "data-3"}, contents)
	})

	t.Run("cancel mid rotation", func(t *testing.T) {
		// A shutdown while the reader works through a run of empty files
		// must stop the rotation at once, not drain the remaining files.
		dir := "/upload/cancel"
		require.NoError(t, emu.client.MkdirAll(dir))
		writeSFTPFile(t, emu.client, dir+"/empty.txt", "")

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		const (
			totalFiles   = 10
			cancelAtFile = 3
		)
		provider := &countingPathProvider{path: dir + "/empty.txt", total: totalFiles}
		provider.onNext = func(calls int) {
			if calls == cancelAtFile {
				cancel()
			}
		}

		reader := readSFTPInput(t, emu, dir+"/*.txt", "lines", false)
		reader.pathProvider = provider

		_, _, err := reader.ReadBatch(ctx)
		require.ErrorIs(t, err, context.Canceled)
		assert.Equal(t, cancelAtFile, provider.calls)
	})

	t.Run("watcher", func(t *testing.T) {
		dir := "/upload/watcher"
		require.NoError(t, emu.client.MkdirAll(dir))
		writeSFTPFile(t, emu.client, dir+"/1.txt", "data-1")
		writeSFTPFile(t, emu.client, dir+"/2.txt", "data-2")

		reader := readSFTPInput(t, emu, dir+"/*.txt", "to_the_end", true)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var contents []string
		for range 2 {
			contents = append(contents, readOneFile(t, ctx, reader))
		}
		assert.ElementsMatch(t, []string{"data-1", "data-2"}, contents)

		// The watcher waits for a new file. It must not end the input.
		results := make(chan string, 1)
		go func() { results <- readOneFile(t, ctx, reader) }()
		select {
		case content := <-results:
			t.Fatalf("watcher returned %q before a new file was written", content)
		case <-time.After(500 * time.Millisecond):
		}

		writeSFTPFile(t, emu.client, dir+"/3.txt", "data-3")
		select {
		case content := <-results:
			assert.Equal(t, "data-3", content)
		case <-time.After(5 * time.Second):
			t.Fatal("watcher did not pick up the new file")
		}

		// A shutdown must unblock the waiting watcher.
		errs := make(chan error, 1)
		go func() {
			_, _, err := reader.ReadBatch(ctx)
			errs <- err
		}()
		cancel()
		select {
		case err := <-errs:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("watcher did not stop on context cancel")
		}
	})
}

// countingPathProvider returns the same path a fixed number of times and
// counts the calls. It caps the run so a test cannot loop for ever.
type countingPathProvider struct {
	path   string
	total  int
	calls  int
	onNext func(calls int)
}

func (c *countingPathProvider) Next(context.Context) (string, bool, error) {
	if c.calls >= c.total {
		return "", false, nil
	}
	c.calls++
	c.onNext(c.calls)
	return c.path, true, nil
}

func (*countingPathProvider) Ack(context.Context, string, error) error {
	return nil
}
