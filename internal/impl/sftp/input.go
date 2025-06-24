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
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/codec"
	"github.com/redpanda-data/connect/v4/internal/pool"
)

const (
	siFieldMaxSFTPSessions     = "max_sftp_sessions"
	siFieldPaths               = "paths"
	siFieldDeleteOnFinish      = "delete_on_finish"
	siFieldWatcher             = "watcher"
	siFieldWatcherEnabled      = "enabled"
	siFieldWatcherMinimumAge   = "minimum_age"
	siFieldWatcherPollInterval = "poll_interval"
	siFieldWatcherCache        = "cache"
)

func sftpInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Network").
		Version("3.39.0").
		Summary(`Consumes files from an SFTP server.`).
		Description(`
== Metadata

This input adds the following metadata fields to each message:

- sftp_path
- sftp_mod_time

You can access these metadata fields using xref:configuration:interpolation.adoc#bloblang-queries[function interpolation].`).
		Fields(connectionFields()...).
		Field(service.NewIntField(siFieldMaxSFTPSessions).
			Description("The maximum number of SFTP sessions.").
			// See `MaxSessions` and `MaxStartups` in the server `sshd_config`.
			// Details here: https://serverfault.com/questions/392749/sftp-concurrent-connection
			Default(10).
			Advanced()).
		Fields(
			service.NewStringListField(siFieldPaths).
				Description("A list of paths to consume sequentially. Glob patterns are supported."),
			service.NewAutoRetryNacksToggleField(),
		).
		Fields(codec.DeprecatedCodecFields("to_the_end")...).
		Fields(
			service.NewBoolField(siFieldDeleteOnFinish).
				Description("Whether to delete files from the server once they are processed.").
				Advanced().
				Default(false),
			service.NewObjectField(siFieldWatcher,
				service.NewBoolField(siFieldWatcherEnabled).
					Description("Whether file watching is enabled.").
					Default(false),
				service.NewDurationField(siFieldWatcherMinimumAge).
					Description("The minimum period of time since a file was last updated before attempting to consume it. Increasing this period decreases the likelihood that a file will be consumed whilst it is still being written to.").
					Default("1s").
					Examples("10s", "1m", "10m"),
				service.NewDurationField(siFieldWatcherPollInterval).
					Description("The interval between each attempt to scan the target paths for new files.").
					Default("1s").
					Examples("100ms", "1s"),
				service.NewStringField(siFieldWatcherCache).
					Description("A xref:components:caches/about.adoc[cache resource] for storing the paths of files already consumed.").
					Default(""),
			).Description("An experimental mode whereby the input will periodically scan the target paths for new files and consume them, when all files are consumed the input will continue polling for new files.").
				Version("3.42.0"),
		)
}

func init() {
	service.MustRegisterBatchInput("sftp", sftpInputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
		r, err := newSFTPReaderFromParsed(conf, mgr)
		if err != nil {
			return nil, err
		}
		return service.AutoRetryNacksBatchedToggled(conf, r)
	})
}

//------------------------------------------------------------------------------

type fileInfo struct {
	path    string
	modTime time.Time
}

type sftpReader struct {
	log *service.Logger
	mgr *service.Resources

	address        string
	paths          []string
	sshConfig      *ssh.ClientConfig
	scannerCtor    codec.DeprecatedFallbackCodec
	deleteOnFinish bool

	watcherEnabled      bool
	watcherCache        string
	watcherPollInterval time.Duration
	watcherMinAge       time.Duration

	stateLock       sync.Mutex
	scanner         codec.DeprecatedFallbackStream
	currentFileInfo fileInfo

	sshClient      *ssh.Client
	sftpClientPool pool.Capped[*sftp.Client]
	pathProvider   pathProvider
}

func newSFTPReaderFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (s *sftpReader, err error) {
	s = &sftpReader{
		log: mgr.Logger(),
		mgr: mgr,
	}

	if s.address, err = conf.FieldString(sFieldAddress); err != nil {
		return nil, err
	}
	if s.paths, err = conf.FieldStringList(siFieldPaths); err != nil {
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
	if s.scannerCtor, err = codec.DeprecatedCodecFromParsed(conf); err != nil {
		return
	}
	if s.deleteOnFinish, err = conf.FieldBool(siFieldDeleteOnFinish); err != nil {
		return
	}

	{
		wConf := conf.Namespace(siFieldWatcher)
		if s.watcherEnabled, _ = wConf.FieldBool(siFieldWatcherEnabled); s.watcherEnabled {
			if s.watcherCache, err = wConf.FieldString(siFieldWatcherCache); err != nil {
				return
			}
			if s.watcherPollInterval, err = wConf.FieldDuration(siFieldWatcherPollInterval); err != nil {
				return
			}
			if s.watcherMinAge, err = wConf.FieldDuration(siFieldWatcherMinimumAge); err != nil {
				return
			}
			if !mgr.HasCache(s.watcherCache) {
				return nil, fmt.Errorf("cache resource %q was not found", s.watcherCache)
			}
		}
	}

	var maxSFTPSessions int
	if maxSFTPSessions, err = conf.FieldInt(siFieldMaxSFTPSessions); err != nil {
		return nil, err
	}
	s.sftpClientPool = pool.NewCapped(maxSFTPSessions, func(context.Context, int) (*sftp.Client, error) {
		if s.sshClient == nil {
			return nil, service.ErrNotConnected
		}

		client, err := sftp.NewClient(s.sshClient)
		if err != nil {
			return nil, fmt.Errorf("failed to create SFTP client: %w", err)
		}

		return client, nil
	})

	return
}

func (s *sftpReader) Connect(ctx context.Context) error {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	if s.sshClient != nil {
		s.log.Warnf("Already connected to SFTP server at %s", s.address)
		return nil
	}

	// Clear any existing SFTP sessions
	s.sftpClientPool.Reset()

	var err error
	s.sshClient, err = ssh.Dial("tcp", s.address, s.sshConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to SFTP server: %w", err)
	}

	if s.watcherEnabled && s.pathProvider == nil {
		s.pathProvider = &watcherPathProvider{
			clientPool:   s.sftpClientPool,
			mgr:          s.mgr,
			cacheName:    s.watcherCache,
			pollInterval: s.watcherPollInterval,
			minAge:       s.watcherMinAge,
			targetPaths:  s.paths,
		}

		return nil
	}

	client, err := s.sftpClientPool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer s.sftpClientPool.Release(client)

	var spp *staticPathProvider
	switch pp := s.pathProvider.(type) {
	case *staticPathProvider:
		spp = pp
	default:
		spp = new(staticPathProvider)
		s.pathProvider = spp
	}

	for _, path := range s.paths {
		expandedPaths, err := client.Glob(path)
		if err != nil {
			s.log.Warnf("Failed to scan files from path %v: %s", path, err)
			continue
		}
		spp.expandedPaths = append(spp.expandedPaths, expandedPaths...)
	}

	return nil
}

func (s *sftpReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	parts, codecAckFn, err := s.tryReadBatch(ctx)
	if err != nil {
		if errors.Is(err, sftp.ErrSSHFxConnectionLost) {
			s.stateLock.Lock()
			defer s.stateLock.Unlock()

			if s.scanner != nil {
				if err := s.scanner.Close(ctx); err != nil {
					s.log.With("error", err).Error("Failed to close scanner")
				}
				s.scanner = nil
			}
			err = service.ErrNotConnected
		}
		return nil, nil, err
	}
	return parts, codecAckFn, nil
}

func (s *sftpReader) Close(ctx context.Context) error {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	if s.sshClient == nil {
		return nil
	}

	if s.scanner != nil {
		if err := s.scanner.Close(ctx); err != nil {
			s.log.With("error", err).Error("Failed to close scanner")
		}

		s.scanner = nil
	}

	s.sftpClientPool.Reset()

	if err := s.sshClient.Close(); err != nil {
		return fmt.Errorf("failed to close SSH client: %s", err)
	}

	s.sshClient = nil

	return nil
}

func (s *sftpReader) tryReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	scanner, err := s.initScanner(ctx)
	if err != nil {
		return nil, nil, err
	}

	parts, codecAckFn, err := scanner.NextBatch(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		s.stateLock.Lock()
		scanner = s.scanner
		s.stateLock.Unlock()

		if scanner != nil {
			if err := scanner.Close(ctx); err != nil {
				s.log.With("error", err).Error("Failed to close scanner")
			}

			s.stateLock.Lock()
			s.scanner = nil
			s.stateLock.Unlock()
		}

		if errors.Is(err, io.EOF) {
			err = service.ErrNotConnected
		}
		return nil, nil, err
	}

	for _, part := range parts {
		part.MetaSetMut("sftp_path", s.currentFileInfo.path)
		part.MetaSetMut("sftp_mod_time", s.currentFileInfo.modTime)
	}

	return parts, codecAckFn, nil
}

type sftpFile struct {
	file        *sftp.File
	postCloseFn func()
}

func (o *sftpFile) Read(p []byte) (int, error) {
	return o.file.Read(p)
}

func (o *sftpFile) Close() error {
	if o.file == nil {
		return nil
	}
	err := o.file.Close()
	o.file = nil // Prevent double close

	o.postCloseFn()

	return err
}

func (s *sftpReader) initScanner(ctx context.Context) (codec.DeprecatedFallbackStream, error) {
	s.stateLock.Lock()
	scanner := s.scanner
	isConnected := s.sshClient != nil
	s.stateLock.Unlock()
	if scanner != nil {
		return scanner, nil
	}

	if !isConnected {
		return nil, service.ErrNotConnected
	}

	var file *sftp.File
	var path string
	for {
		var ok bool
		var err error
		path, ok, err = s.pathProvider.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("finding next file path: %w", err)
		}
		if !ok {
			return nil, service.ErrEndOfInput
		}

		client, err := s.sftpClientPool.Acquire(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to acquire SFTP client: %w", err)
		}

		handleErr := func(err error) {
			s.log.With("path", path, "err", err.Error()).Warn("Failed to open previously identified file")

			if os.IsNotExist(err) {
				// If we failed to open the file because it no longer exists then we
				// can "ack" the path as we're done with it. Otherwise we "nack" it
				// with the error as we'll want to reprocess it again later.
				err = nil
			}
			if ackErr := s.pathProvider.Ack(ctx, path, err); ackErr != nil {
				s.log.With("error", ackErr).Warnf("Failed to acknowledge path: %w", path)
			}

			s.sftpClientPool.Release(client)
		}

		file, err = client.Open(path)
		if err != nil {
			handleErr(fmt.Errorf("failed to open file: %w", err))
			continue
		}

		stat, err := file.Stat()
		if err != nil {
			handleErr(fmt.Errorf("failed to stat file: %w", err))
			continue
		}

		f := &sftpFile{
			file: file,
			postCloseFn: func() {
				s.sftpClientPool.Release(client)
			},
		}

		details := service.NewScannerSourceDetails()
		details.SetName(path)
		scanner, err := s.scannerCtor.Create(f, s.newCodecAckFn(client, path), details)
		if err != nil {
			if err = f.Close(); err != nil {
				s.log.Errorf("Failed to close file %q: %s", path, err)
			}
			return nil, fmt.Errorf("failed to create scanner: %w", err)
		}

		s.stateLock.Lock()
		s.scanner = scanner
		s.currentFileInfo = fileInfo{
			path:    path,
			modTime: stat.ModTime(),
		}
		s.stateLock.Unlock()

		return scanner, nil
	}
}

func (s *sftpReader) newCodecAckFn(client *sftp.Client, path string) service.AckFunc {
	return func(ctx context.Context, aErr error) error {
		if err := s.pathProvider.Ack(ctx, path, aErr); err != nil {
			s.log.With("error", err).Warnf("Failed to acknowledge path: %s", path)
		}
		if aErr != nil {
			return nil
		}

		if s.deleteOnFinish {
			if s.sshClient == nil {
				return nil
			}

			if err := client.Remove(path); err != nil {
				return fmt.Errorf("failed to remove file %q: %w", path, err)
			}
		}

		return nil
	}
}

type pathProvider interface {
	Next(context.Context) (string, bool, error)
	Ack(context.Context, string, error) error
}

type staticPathProvider struct {
	expandedPaths []string
}

func (s *staticPathProvider) Next(context.Context) (string, bool, error) {
	if len(s.expandedPaths) == 0 {
		return "", false, nil
	}
	path := s.expandedPaths[0]
	s.expandedPaths = s.expandedPaths[1:]
	return path, true, nil
}

func (*staticPathProvider) Ack(context.Context, string, error) error {
	return nil
}

type watcherPathProvider struct {
	clientPool   pool.Capped[*sftp.Client]
	mgr          *service.Resources
	cacheName    string
	pollInterval time.Duration
	minAge       time.Duration
	targetPaths  []string

	expandedPaths []string
	nextPoll      time.Time
	followUpPoll  bool
}

func (w *watcherPathProvider) Next(ctx context.Context) (string, bool, error) {
	for {
		if len(w.expandedPaths) > 0 {
			nextPath := w.expandedPaths[0]
			w.expandedPaths = w.expandedPaths[1:]
			return nextPath, true, nil
		}

		if waitFor := time.Until(w.nextPoll); w.nextPoll.IsZero() || waitFor > 0 {
			select {
			case <-time.After(waitFor):
			case <-ctx.Done():
				return "", false, ctx.Err()
			}
		}
		w.nextPoll = time.Now().Add(w.pollInterval)

		if err := w.findNewPaths(ctx); err != nil {
			return "", false, fmt.Errorf("expanding new paths: %w", err)
		}
		w.followUpPoll = true
	}
}

func (w *watcherPathProvider) findNewPaths(ctx context.Context) error {
	if cerr := w.mgr.AccessCache(ctx, w.cacheName, func(cache service.Cache) {
		client, err := w.clientPool.Acquire(ctx)
		if err != nil {
			w.mgr.Logger().With("error", err).Warn("Failed to acquire SFTP client")
			return
		}
		defer w.clientPool.Release(client)
		for _, p := range w.targetPaths {
			select {
			case <-ctx.Done():
				return
			default:
			}

			paths, err := client.Glob(p)
			if err != nil {
				w.mgr.Logger().With("error", err, "path", p).Warn("Failed to scan files from path")
				continue
			}

			for _, path := range paths {
				select {
				case <-ctx.Done():
					return
				default:
				}

				info, err := client.Stat(path)
				if err != nil {
					w.mgr.Logger().With("error", err, "path", path).Warn("Failed to stat path")
					continue
				}
				if time.Since(info.ModTime()) < w.minAge {
					continue
				}

				// We process it if the marker is a pending symbol (!) and we're
				// polling for the first time, or if the path isn't found in the
				// cache.
				//
				// If we got an unexpected error obtaining a marker for this
				// path from the cache then we skip that path because the
				// watcher will eventually poll again, and the cache.Get
				// operation will re-run.
				if v, err := cache.Get(ctx, path); errors.Is(err, service.ErrKeyNotFound) || (!w.followUpPoll && string(v) == "!") {
					w.expandedPaths = append(w.expandedPaths, path)
					if err = cache.Set(ctx, path, []byte("!"), nil); err != nil {
						// Mark the file target as pending so that we do not reprocess it
						w.mgr.Logger().With("error", err, "path", path).Warn("Failed to mark path as pending")
					}
				}
			}
		}
	}); cerr != nil {
		return fmt.Errorf("error obtaining cache: %v", cerr)
	}

	return nil
}

func (w *watcherPathProvider) Ack(ctx context.Context, name string, err error) (outErr error) {
	if cerr := w.mgr.AccessCache(ctx, w.cacheName, func(cache service.Cache) {
		if err == nil {
			outErr = cache.Set(ctx, name, []byte("@"), nil)
		} else {
			_ = cache.Delete(ctx, name)
		}
	}); cerr != nil {
		outErr = cerr
	}
	return
}
