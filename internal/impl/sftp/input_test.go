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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func newTestSFTPReader(t *testing.T, srv *testSFTPServer, watcher bool) *sftpReader {
	t.Helper()

	conf := fmt.Sprintf(`
address: %s
paths:
  - "*.txt"
credentials:
  username: blobfish
  password: secret
  host_public_key: %s
scanner:
  to_the_end: {}
watcher:
  enabled: %t
  minimum_age: 0s
  poll_interval: 50ms
  cache: files_memory
`, srv.address, srv.hostKey, watcher)

	parsed, err := sftpInputSpec().ParseYAML(conf, nil)
	require.NoError(t, err)
	r, err := newSFTPReaderFromParsed(parsed, service.MockResources(service.MockResourcesOptAddCache("files_memory")))
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })
	return r
}

// readLikeInput reads and acks one single-message batch the way benthos
// drives an input: on service.ErrNotConnected it reconnects and reads again.
// It returns the error of the last attempt.
func readLikeInput(ctx context.Context, r *sftpReader) (string, error) {
	var err error
	for range 3 {
		var batch service.MessageBatch
		var ackFn service.AckFunc
		batch, ackFn, err = r.ReadBatch(ctx)
		if errors.Is(err, service.ErrNotConnected) {
			if err = r.Connect(ctx); err != nil {
				return "", err
			}
			continue
		}
		if err != nil {
			return "", err
		}
		if len(batch) != 1 {
			return "", fmt.Errorf("expected a batch of 1 message, got %d", len(batch))
		}
		content, err := batch[0].AsBytes()
		if err != nil {
			return "", err
		}
		if err := ackFn(ctx, nil); err != nil {
			return "", err
		}
		return string(content), nil
	}
	return "", err
}

// waitForSSHClose waits until the reader has seen its connection close, as
// happens when the server drops an idle connection.
func waitForSSHClose(t *testing.T, r *sftpReader) {
	t.Helper()

	r.stateLock.Lock()
	closed := r.sshClosed
	r.stateLock.Unlock()
	require.Eventually(t, func() bool { return isClosed(closed) }, 5*time.Second, 10*time.Millisecond)
}

// After the SFTP server dropped the connection, the input never reconnected:
// it kept the dead SSH client, so static paths not read yet were skipped as
// unopenable and the watcher logged "Failed to acquire SFTP client" on every
// poll forever.
func TestSFTPInputReconnectsAfterConnectionDrop(t *testing.T) {
	for _, watcher := range []bool{false, true} {
		for _, waitForClose := range []bool{true, false} {
			t.Run(fmt.Sprintf("watcher=%t/wait_for_close=%t", watcher, waitForClose), func(t *testing.T) {
				srv := startTestSFTPServer(t)
				want := map[string]bool{"a": true, "b": true, "c": true}
				for name := range want {
					srv.writeFile(t, name+".txt", name)
				}

				r := newTestSFTPReader(t, srv, watcher)
				ctx := t.Context()
				require.NoError(t, r.Connect(ctx))

				got := map[string]int{}
				// The file being read when the connection drops may be read
				// again after reconnecting.
				first, err := readLikeInput(ctx, r)
				require.NoError(t, err)
				got[first]++

				srv.dropConnections()
				if waitForClose {
					waitForSSHClose(t, r)
				}

				// Read until every file has been seen. A read may fail while
				// the reader finds out the connection is gone, but it must
				// not stick.
				for i := 0; len(got) < len(want); i++ {
					require.Less(t, i, 10, "reads never recovered after the connection dropped, got %v", got)

					readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
					content, err := readLikeInput(readCtx, r)
					cancel()
					if errors.Is(err, service.ErrEndOfInput) {
						require.Fail(t, "input ended without reading every file", "got %v", got)
					}
					if err == nil {
						got[content]++
					}
				}

				for name := range want {
					assert.Positive(t, got[name], "file %s was never read", name)
				}
				if !watcher {
					// Reconnecting must not expand the static paths again
					// and read the remaining files twice.
					_, err := readLikeInput(ctx, r)
					require.ErrorIs(t, err, service.ErrEndOfInput)
					for name, n := range got {
						if name != first {
							assert.Equal(t, 1, n, "file %s read more than once", name)
						}
					}
				}
			})
		}
	}
}
