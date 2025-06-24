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
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	soFieldPath  = "path"
	soFieldCodec = "codec"
)

func sftpOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Network").
		Version("3.39.0").
		Summary(`Writes files to an SFTP server.`).
		Description(`In order to have a different path for each object you should use function interpolations described xref:configuration:interpolation.adoc#bloblang-queries[here].`+service.OutputPerformanceDocs(true, false)).
		Fields(connectionFields()...).
		Fields(
			service.NewInterpolatedStringField(soFieldPath).
				Description("The file to save the messages to on the server."),
			service.NewStringAnnotatedEnumField(soFieldCodec, map[string]string{
				"all-bytes": "Only applicable to file based outputs. Writes each message to a file in full, if the file already exists the old content is deleted.",
				"append":    "Append each message to the output stream without any delimiter or special encoding.",
				"lines":     "Append each message to the output stream followed by a line break.",
				"delim:x":   "Append each message to the output stream followed by a custom delimiter.",
			}).
				Description("The way in which the bytes of messages should be written out into the output data stream. It's possible to write lines using a custom delimiter with the `delim:x` codec, where x is the character sequence custom delimiter.").
				LintRule("").
				Examples("lines", "delim:\t", "delim:foobar").
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

	var err error
	s.sshClient, err = ssh.Dial("tcp", s.address, s.sshConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to SFTP server: %s", err)
	}

	return nil
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

	defer func() {
		if wErr != nil && errors.Is(wErr, sftp.ErrSSHFxConnectionLost) {
			s.sshClient = nil
			wErr = service.ErrNotConnected
		}
	}()

	if s.sshClient == nil {
		return service.ErrNotConnected
	}

	path, err := s.path.TryString(msg)
	if err != nil {
		return fmt.Errorf("path interpolation error: %w", err)
	}

	if s.handle != nil {
		if path == s.handlePath {
			return s.writeTo(s.handle, msg)
		}

		// If the path changes, we reset the handle and open the new file.
		if err := s.handle.Close(); err != nil {
			s.log.With("error", err).Error("Failed to close written file")
		}
		if err := s.sftpClient.Close(); err != nil {
			s.log.With("error", err).Error("Failed to close SFTP client")
		}

		s.handle = nil
		s.handlePath = ""
	}

	flag := os.O_CREATE | os.O_WRONLY
	if s.appendMode {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}

	s.sftpClient, err = sftp.NewClient(s.sshClient)
	if err != nil {
		return fmt.Errorf("failed to create SFTP client: %w", err)
	}

	if err := s.sftpClient.MkdirAll(filepath.Dir(path)); err != nil {
		return fmt.Errorf("failed to create remote directory: %w", err)
	}

	handle, err := s.sftpClient.OpenFile(path, flag)
	if err != nil {
		return fmt.Errorf("failed to open remote file: %w", err)
	}
	s.handle = handle
	s.handlePath = path

	if s.appendMode {
		// Need to seek to the end when appending to an existing file.
		// Details here: https://github.com/pkg/sftp/issues/295
		fi, err := s.sftpClient.Lstat(path)
		if err != nil {
			return fmt.Errorf("failed to stat remote file: %w", err)
		}
		_, err = handle.Seek(fi.Size(), 0)
		if err != nil {
			return fmt.Errorf("failed to seek remote file: %w", err)
		}
	}

	if err := s.writeTo(s.handle, msg); err != nil {
		if err := s.handle.Close(); err != nil {
			s.log.With("error", err).Error("Failed to close written file")
		}
		if err := s.sftpClient.Close(); err != nil {
			s.log.With("error", err).Error("Failed to close SFTP client")
		}
		return fmt.Errorf("failed to write message to SFTP server: %w", err)
	}

	return nil
}

func (s *sftpWriter) Close(context.Context) error {
	s.handleMut.Lock()
	defer s.handleMut.Unlock()

	if s.sshClient == nil {
		return nil
	}

	if s.handle != nil {
		if err := s.handle.Close(); err != nil {
			s.log.With("error", err).Error("Failed to close written file")
		}
		s.handle = nil
	}

	if s.sftpClient != nil {
		if err := s.sftpClient.Close(); err != nil {
			s.log.With("error", err).Error("Failed to close SFTP client")
		}
	}

	if err := s.sshClient.Close(); err != nil {
		return fmt.Errorf("failed to close SSH client: %w", err)
	}
	s.sshClient = nil

	return nil
}
