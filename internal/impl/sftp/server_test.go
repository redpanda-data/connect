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
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/pkg/sftp"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/ssh"
)

// testSFTPServer is an in-process SSH server that serves SFTP from a
// temporary directory and can drop every open connection on demand. Paths
// written by tests must be relative, so that they resolve inside dir.
type testSFTPServer struct {
	address string
	hostKey string
	dir     string

	mu    sync.Mutex
	conns []net.Conn
}

func startTestSFTPServer(t *testing.T) *testSFTPServer {
	t.Helper()

	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	signer, err := ssh.NewSignerFromKey(key)
	require.NoError(t, err)

	conf := &ssh.ServerConfig{
		PasswordCallback: func(ssh.ConnMetadata, []byte) (*ssh.Permissions, error) {
			return nil, nil
		},
	}
	conf.AddHostKey(signer)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := &testSFTPServer{
		address: l.Addr().String(),
		hostKey: strings.TrimSpace(string(ssh.MarshalAuthorizedKey(signer.PublicKey()))),
		dir:     t.TempDir(),
	}
	t.Cleanup(func() {
		l.Close()
		srv.dropConnections()
	})

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			srv.mu.Lock()
			srv.conns = append(srv.conns, conn)
			srv.mu.Unlock()
			go srv.serve(conn, conf)
		}
	}()
	return srv
}

func (s *testSFTPServer) serve(conn net.Conn, conf *ssh.ServerConfig) {
	_, chans, reqs, err := ssh.NewServerConn(conn, conf)
	if err != nil {
		conn.Close()
		return
	}
	go ssh.DiscardRequests(reqs)

	for newCh := range chans {
		if newCh.ChannelType() != "session" {
			_ = newCh.Reject(ssh.UnknownChannelType, "unsupported channel type")
			continue
		}
		ch, chReqs, err := newCh.Accept()
		if err != nil {
			continue
		}
		go func() {
			for req := range chReqs {
				ok := req.Type == "subsystem" && len(req.Payload) > 4 && string(req.Payload[4:]) == "sftp"
				_ = req.Reply(ok, nil)
				if ok {
					go func() {
						srv, err := sftp.NewServer(ch, sftp.WithServerWorkingDirectory(s.dir))
						if err != nil {
							ch.Close()
							return
						}
						_ = srv.Serve()
						srv.Close()
					}()
				}
			}
		}()
	}
}

// dropConnections closes every connection the server has accepted, as an
// idle timeout or a network failure would.
func (s *testSFTPServer) dropConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		c.Close()
	}
	s.conns = nil
}

func (s *testSFTPServer) readFile(t *testing.T, path string) string {
	t.Helper()

	b, err := os.ReadFile(filepath.Join(s.dir, path))
	require.NoError(t, err)
	return string(b)
}

func (s *testSFTPServer) writeFile(t *testing.T, path, data string) {
	t.Helper()

	require.NoError(t, os.WriteFile(filepath.Join(s.dir, path), []byte(data), 0o644))
}
