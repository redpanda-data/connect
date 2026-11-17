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

func newTestSFTPWriter(t *testing.T, srv *testSFTPServer, path string) *sftpWriter {
	t.Helper()

	conf := fmt.Sprintf(`
address: %s
path: %s
credentials:
  username: blobfish
  password: secret
  host_public_key: %s
codec: all-bytes
`, srv.address, path, srv.hostKey)

	parsed, err := sftpOutputSpec().ParseYAML(conf, nil)
	require.NoError(t, err)
	w, err := newWriterFromParsed(parsed, service.MockResources())
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close(context.Background()) })
	return w
}

// writeLikeOutput writes msg the way benthos drives an output: on
// service.ErrNotConnected it reconnects and retries. It returns the error of
// the last attempt.
func writeLikeOutput(ctx context.Context, w *sftpWriter, msg *service.Message) error {
	var err error
	for range 3 {
		if err = w.Write(ctx, msg); !errors.Is(err, service.ErrNotConnected) {
			return err
		}
		if err = w.Connect(ctx); err != nil {
			return err
		}
	}
	return err
}

// After the SFTP server drops the connection, writes used to fail forever with
// "creating SFTP client: EOF" because the writer kept the dead SSH client and
// never reconnected.
func TestSFTPOutputReconnectsAfterConnectionDrop(t *testing.T) {
	tests := []struct {
		name string
		// waitForClose waits until the writer has seen the connection close
		// before writing, as happens when the server drops an idle
		// connection. Otherwise the write races the close, as when the
		// connection fails while a write is starting.
		waitForClose bool
	}{
		{name: "connection closed while idle", waitForClose: true},
		{name: "connection closed during write"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := startTestSFTPServer(t)
			w := newTestSFTPWriter(t, srv, `${! meta("name") }.txt`)
			ctx := t.Context()

			msg := func(name, body string) *service.Message {
				m := service.NewMessage([]byte(body))
				m.MetaSetMut("name", name)
				return m
			}

			require.NoError(t, w.Connect(ctx))
			require.NoError(t, w.Write(ctx, msg("first", "one")))

			srv.dropConnections()
			if tt.waitForClose {
				closed := w.sshClosed
				require.Eventually(t, func() bool {
					select {
					case <-closed:
						return true
					default:
						return false
					}
				}, 5*time.Second, 10*time.Millisecond)
			}

			// The first write after the drop may fail while the writer finds
			// out the connection is gone, but it must not stick.
			for i := range 5 {
				err := writeLikeOutput(ctx, w, msg(fmt.Sprintf("second-%d", i), "two"))
				if err == nil {
					break
				}
				require.Less(t, i, 4, "writes never recovered after the connection dropped: %v", err)
			}

			require.NoError(t, writeLikeOutput(ctx, w, msg("third", "three")))
			assert.Equal(t, "three", srv.readFile(t, "third.txt"))
		})
	}
}

// A write to an already open file handle fails when the connection drops.
// The writer must reconnect rather than keep the dead connection.
func TestSFTPOutputReconnectsAfterConnectionDropWithOpenHandle(t *testing.T) {
	srv := startTestSFTPServer(t)
	w := newTestSFTPWriter(t, srv, "same.txt")
	ctx := t.Context()

	require.NoError(t, w.Connect(ctx))
	require.NoError(t, w.Write(ctx, service.NewMessage([]byte("one"))))
	require.NotNil(t, w.handle)

	srv.dropConnections()

	var err error
	for range 5 {
		if err = writeLikeOutput(ctx, w, service.NewMessage([]byte("two"))); err == nil {
			break
		}
	}
	require.NoError(t, err)
	require.NoError(t, w.Close(ctx))
	assert.Equal(t, "two", srv.readFile(t, "same.txt"))
}
