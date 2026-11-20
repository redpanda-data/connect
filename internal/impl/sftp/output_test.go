// Copyright 2026 Redpanda Data, Inc.
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
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"

	"github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// newTestSFTPServer starts an SSH server on a local port that serves the SFTP
// subsystem from handlers. It returns the address and the host public key.
func newTestSFTPServer(t *testing.T, handlers sftp.Handlers) (addr, hostKey string) {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(priv)
	require.NoError(t, err)
	conf := &ssh.ServerConfig{
		PasswordCallback: func(ssh.ConnMetadata, []byte) (*ssh.Permissions, error) { return nil, nil },
	}
	conf.AddHostKey(signer)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveTestSSHConn(conn, conf, handlers)
		}
	}()
	return ln.Addr().String(), string(ssh.MarshalAuthorizedKey(signer.PublicKey()))
}

func serveTestSSHConn(conn net.Conn, conf *ssh.ServerConfig, handlers sftp.Handlers) {
	_, chans, reqs, err := ssh.NewServerConn(conn, conf)
	if err != nil {
		return
	}
	go ssh.DiscardRequests(reqs)
	for newCh := range chans {
		ch, chReqs, err := newCh.Accept()
		if err != nil {
			return
		}
		go func() {
			for req := range chReqs {
				// The payload is the subsystem name as an SSH string.
				ok := req.Type == "subsystem" && len(req.Payload) > 4 && string(req.Payload[4:]) == "sftp"
				_ = req.Reply(ok, nil)
				if ok {
					go func() {
						_ = sftp.NewRequestServer(ch, handlers).Serve()
						_ = ch.Close()
					}()
				}
			}
		}()
	}
}

// watchedPut is an SFTP file writer that calls check after each open for
// write and before each write request, so the test sees each state of the
// file system that a reader can see.
type watchedPut struct {
	sftp.FileWriter
	check func()
}

func (p watchedPut) Filewrite(r *sftp.Request) (io.WriterAt, error) {
	w, err := p.FileWriter.Filewrite(r)
	if err != nil {
		return nil, err
	}
	p.check()
	return watchedWriterAt{w, p.check}, nil
}

type watchedWriterAt struct {
	io.WriterAt
	check func()
}

func (w watchedWriterAt) WriteAt(b []byte, off int64) (int, error) {
	w.check()
	return w.WriterAt.WriteAt(b, off)
}

// readMemFile reads path from the in-memory file system of h. It does not call
// t.FailNow, because the SFTP server goroutine also calls it.
func readMemFile(h sftp.Handlers, path string) (content string, exists bool, err error) {
	req := sftp.NewRequest("Get", path)
	req.Flags = 1 // SSH_FXF_READ
	r, err := h.FileGet.Fileread(req)
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	buf := make([]byte, 64)
	n, err := r.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return "", true, err
	}
	return string(buf[:n]), true, nil
}

// newTestSFTPWriter returns a connected sftp output that writes to path on the
// server at addr. extraConf holds more config fields, one per line.
func newTestSFTPWriter(t *testing.T, addr, hostKey, path, extraConf string) *sftpWriter {
	t.Helper()
	parsed, err := sftpOutputSpec().ParseYAML(fmt.Sprintf(`
address: %s
path: %s
credentials:
  username: user
  password: pass
  host_public_key: %q
%s
`, addr, path, hostKey, extraConf), nil)
	require.NoError(t, err)
	w, err := newWriterFromParsed(parsed, service.MockResources())
	require.NoError(t, err)
	require.NoError(t, w.Connect(t.Context()))
	t.Cleanup(func() { require.NoError(t, w.Close(context.Background())) })
	return w
}

func TestSFTPOutputSamePathWrites(t *testing.T) {
	tests := []struct {
		name  string
		codec string
		want  string
	}{
		{
			name:  "all-bytes replaces the file",
			codec: "all-bytes",
			want:  "world",
		},
		{
			name:  "lines appends to the file",
			codec: "lines",
			want:  "hello\nworld\n",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const path = "/upload/a.txt"
			mem := sftp.InMemHandler()
			addr, hostKey := newTestSFTPServer(t, mem)
			w := newTestSFTPWriter(t, addr, hostKey, path, "codec: "+tt.codec)

			require.NoError(t, w.Write(t.Context(), service.NewMessage([]byte("hello"))))
			require.NoError(t, w.Write(t.Context(), service.NewMessage([]byte("world"))))

			content, ok, err := readMemFile(mem, path)
			require.NoError(t, err)
			require.True(t, ok)
			assert.Equal(t, tt.want, content)
		})
	}
}

func TestSFTPOutputAtomicWrites(t *testing.T) {
	const path = "/upload/a.txt"
	mem := sftp.InMemHandler()

	var seenMut sync.Mutex
	var seen []string
	handlers := mem
	handlers.FilePut = watchedPut{FileWriter: mem.FilePut, check: func() {
		content, ok, err := readMemFile(mem, path)
		if !assert.NoError(t, err) || !ok {
			return
		}
		seenMut.Lock()
		seen = append(seen, content)
		seenMut.Unlock()
	}}
	addr, hostKey := newTestSFTPServer(t, handlers)

	w := newTestSFTPWriter(t, addr, hostKey, path, "codec: all-bytes\natomic_writes: true")

	// The second write replaces the file that the first write made.
	require.NoError(t, w.Write(t.Context(), service.NewMessage([]byte("hello"))))
	require.NoError(t, w.Write(t.Context(), service.NewMessage([]byte("world"))))

	content, ok, err := readMemFile(mem, path)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "world", content)

	// A reader must see only full files, never an empty or partial file.
	seenMut.Lock()
	defer seenMut.Unlock()
	for _, s := range seen {
		assert.Contains(t, []string{"hello", "world"}, s)
	}

	// No temporary file stays in the directory.
	lister, err := mem.FileList.Filelist(sftp.NewRequest("List", "/upload"))
	require.NoError(t, err)
	files := make([]os.FileInfo, 10)
	n, err := lister.ListAt(files, 0)
	if !errors.Is(err, io.EOF) {
		require.NoError(t, err)
	}
	require.Equal(t, 1, n)
	assert.Equal(t, "a.txt", files[0].Name())
}

func TestSFTPOutputAtomicWritesRejectsAppendCodec(t *testing.T) {
	parsed, err := sftpOutputSpec().ParseYAML(`
address: localhost:22
path: /upload/a.txt
credentials:
  username: user
  password: pass
codec: lines
atomic_writes: true
`, nil)
	require.NoError(t, err)
	_, err = newWriterFromParsed(parsed, service.MockResources())
	require.ErrorContains(t, err, "atomic_writes")
}
