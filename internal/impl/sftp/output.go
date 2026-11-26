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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/writecodec"
)

const (
	soFieldPath  = "path"
	soFieldCodec = "codec"
)

func sftpOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Version("3.39.0").
		Summary(`Writes files to an SFTP server.`).
		Description(`In order to have a different path for each object you should use function interpolations described xref:configuration:interpolation.adoc#bloblang-queries[here].`+service.OutputPerformanceDocs(true, false)).
		Fields(connectionFields()...).
		Fields(
			service.NewInterpolatedStringField(soFieldPath).
				Description("The file to save the messages to on the SFTP server."),
			writecodec.Field(soFieldCodec).
				Default("all-bytes"),
			service.NewOutputMaxInFlightField(),
		)
}

func init() {
	service.MustRegisterOutput(
		"sftp", sftpOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			out, err = newWriterFromParsed(conf, mgr)
			return
		})
}

//------------------------------------------------------------------------------

type sftpWriter struct {
	log *service.Logger

	address    string
	sshConfig  *ssh.ClientConfig
	path       *service.InterpolatedString
	suffixFn   codecSuffixFn
	appendMode bool

	handleMut  sync.Mutex
	sshClient  *ssh.Client
	sshClosed  <-chan struct{} // closed once sshClient's connection has shut down
	sftpClient *sftp.Client
	handlePath string
	handle     io.WriteCloser
}

func newWriterFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (s *sftpWriter, err error) {
	s = &sftpWriter{
		log: mgr.Logger(),
	}

	var codecStr string
	if codecStr, err = conf.FieldString(soFieldCodec); err != nil {
		return
	}
	if s.suffixFn, s.appendMode, err = codecGetWriter(codecStr); err != nil {
		return nil, err
	}

	if s.address, err = conf.FieldString(sFieldAddress); err != nil {
		return
	}
	if s.sshConfig, err = sshAuthConfigFromParsed(conf.Namespace(sFieldCredentials), mgr); err != nil {
		return
	}
	if conf.Contains(sFieldConnectionTimeout) {
		if s.sshConfig.Timeout, err = conf.FieldDuration(sFieldConnectionTimeout); err != nil {
			return
		}
	}
	if s.path, err = conf.FieldInterpolatedString(soFieldPath); err != nil {
		return
	}

	return s, nil
}

func (s *sftpWriter) Connect(context.Context) error {
	s.handleMut.Lock()
	defer s.handleMut.Unlock()

	if s.sshClient != nil {
		return nil
	}

	client, err := ssh.Dial("tcp", s.address, s.sshConfig)
	if err != nil {
		return fmt.Errorf("connecting to SFTP server: %s", err)
	}

	s.sshClient = client
	s.sshClosed = watchSSHClose(client)
	return nil
}

// isConnLost reports whether err, returned while using the current SSH
// connection, means that connection is gone. A connection-closed io.EOF can
// arrive just before the connection is reported as closed, so both are
// checked.
func (s *sftpWriter) isConnLost(err error) bool {
	return isConnLostErr(err) || isClosed(s.sshClosed)
}

// closeConn closes the file handle, the SFTP client and the SSH connection,
// and clears them so that the next write reports service.ErrNotConnected and
// the output reconnects. Errors are logged at debug level only, since this is
// usually called on a connection that has already failed.
func (s *sftpWriter) closeConn() {
	if s.handle != nil {
		if err := s.handle.Close(); err != nil {
			s.log.With("error", err).Debug("Failed to close written file")
		}
		s.handle = nil
		s.handlePath = ""
	}
	if s.sftpClient != nil {
		if err := s.sftpClient.Close(); err != nil {
			s.log.With("error", err).Debug("Failed to close SFTP client")
		}
		s.sftpClient = nil
	}
	if s.sshClient != nil {
		if err := s.sshClient.Close(); err != nil {
			s.log.With("error", err).Debug("Failed to close SSH client")
		}
		s.sshClient = nil
		s.sshClosed = nil
	}
}

func (s *sftpWriter) writeTo(wtr io.Writer, p *service.Message) error {
	mBytes, err := p.AsBytes()
	if err != nil {
		return err
	}

	suffix, addSuffix := s.suffixFn(mBytes)

	if _, err := wtr.Write(mBytes); err != nil {
		return err
	}
	if addSuffix {
		if _, err := wtr.Write(suffix); err != nil {
			return err
		}
	}
	return nil
}

// Write stores the file handle and SFTP session in the writer, and writes the message to the file. This approach allows
// us to reuse the same session across multiple writes, which is particularly useful when the codec requires appending
// to files. The current implementation does not support parallel writes.
func (s *sftpWriter) Write(_ context.Context, msg *service.Message) (wErr error) {
	s.handleMut.Lock()
	defer s.handleMut.Unlock()

	if s.sshClient != nil && isClosed(s.sshClosed) {
		s.log.Warnf("Connection to SFTP server at %s was closed, reconnecting", s.address)
		s.closeConn()
	}
	if s.sshClient == nil {
		return service.ErrNotConnected
	}

	// A write that failed because the connection is gone must drop the whole
	// connection, not just the SFTP client, otherwise every later write
	// fails against the same dead SSH client (e.g. "creating SFTP client:
	// EOF") and the output never recovers. Reporting ErrNotConnected makes
	// the output reconnect and retry the message.
	defer func() {
		if wErr != nil && s.isConnLost(wErr) {
			s.log.With("error", wErr).Warnf("Lost connection to SFTP server at %s, reconnecting", s.address)
			s.closeConn()
			wErr = service.ErrNotConnected
		}
	}()

	path, err := s.path.TryString(msg)
	if err != nil {
		return fmt.Errorf("path interpolation error: %w", err)
	}

	if s.handle != nil && path == s.handlePath {
		if err := s.writeTo(s.handle, msg); err != nil {
			s.closeHandle(s.handle)
			s.closeClient()
			return fmt.Errorf("writing message to SFTP server: %w", err)
		}
		return nil
	}

	// No usable handle for this path. If a handle for a different path is
	// still open, close it and its SFTP client before opening the new file.
	if s.handle != nil {
		s.closeHandle(s.handle)
		s.closeClient()
	}

	flag := os.O_CREATE | os.O_WRONLY
	if s.appendMode {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}

	s.sftpClient, err = sftp.NewClient(s.sshClient)
	if err != nil {
		s.sftpClient = nil
		if !s.isConnLost(err) {
			// The SSH connection may still be up but unable to open SFTP
			// sessions (e.g. the server's per-connection session limit is
			// exhausted). Start the next write on a fresh connection.
			s.closeConn()
		}
		return fmt.Errorf("creating SFTP client: %w", err)
	}

	if err := s.sftpClient.MkdirAll(filepath.Dir(path)); err != nil {
		s.closeClient()
		return fmt.Errorf("creating remote directory: %w", err)
	}

	handle, err := s.sftpClient.OpenFile(path, flag)
	if err != nil {
		s.closeClient()
		return fmt.Errorf("opening remote file: %w", err)
	}

	if s.appendMode {
		// Need to seek to the end when appending to an existing file.
		// Details here: https://github.com/pkg/sftp/issues/295
		fi, err := s.sftpClient.Lstat(path)
		if err != nil {
			s.closeHandle(handle)
			s.closeClient()
			return fmt.Errorf("statting remote file: %w", err)
		}
		if _, err := handle.Seek(fi.Size(), 0); err != nil {
			s.closeHandle(handle)
			s.closeClient()
			return fmt.Errorf("seeking remote file: %w", err)
		}
	}

	if err := s.writeTo(handle, msg); err != nil {
		s.closeHandle(handle)
		s.closeClient()
		return fmt.Errorf("writing message to SFTP server: %w", err)
	}

	s.handle = handle
	s.handlePath = path

	return nil
}

// closeClient closes the writer's current SFTP client, logging any error. It
// must be called on every failure path after sftp.NewClient has succeeded, so
// that a stale, unclosed client (and its underlying SSH channel) isn't left
// dangling for the next attempt to discover.
func (s *sftpWriter) closeClient() {
	if s.sftpClient == nil {
		return
	}
	if err := s.sftpClient.Close(); err != nil {
		s.log.With("error", err).Error("Failed to close SFTP client")
	}
	s.sftpClient = nil
}

// closeHandle closes the given file handle, logging any error, and clears the
// cached handle so the next write opens the file again.
func (s *sftpWriter) closeHandle(h io.WriteCloser) {
	if err := h.Close(); err != nil {
		s.log.With("error", err).Error("Failed to close written file")
	}
	s.handle = nil
	s.handlePath = ""
}

func (s *sftpWriter) Close(context.Context) error {
	s.handleMut.Lock()
	defer s.handleMut.Unlock()

	if s.sshClient == nil {
		return nil
	}

	if s.handle != nil {
		s.closeHandle(s.handle)
	}
	s.closeClient()

	err := s.sshClient.Close()
	s.sshClient = nil
	s.sshClosed = nil
	if err != nil {
		return fmt.Errorf("closing SSH client: %w", err)
	}

	return nil
}
