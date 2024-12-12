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

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/benthos/v4/public/service/codec"
)

const (
	siFieldAddress             = "address"
	siFieldCredentials         = "credentials"
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

You can access these metadata fields using xref:configuration:interpolation.adoc#bloblang-queries[function interpolation].`).
		Fields(
			service.NewStringField(siFieldAddress).
				Description("The address of the server to connect to."),
			service.NewObjectField(siFieldCredentials, credentialsFields()...).
				Description("The credentials to use to log into the target server."),
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
	err := service.RegisterBatchInput("sftp", sftpInputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
		r, err := newSFTPReaderFromParsed(conf, mgr)
		if err != nil {
			return nil, err
		}
		return service.AutoRetryNacksBatchedToggled(conf, r)
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type sftpReader struct {
	log *service.Logger
	mgr *service.Resources

	// Config
	address        string
	paths          []string
	creds          credentials
	scannerCtor    codec.DeprecatedFallbackCodec
	deleteOnFinish bool

	watcherEnabled      bool
	watcherCache        string
	watcherPollInterval time.Duration
	watcherMinAge       time.Duration

	// State
	stateLock   sync.Mutex
	client      *sftp.Client
	scanner     codec.DeprecatedFallbackStream
	currentPath string
	closed      bool

	pathProvider pathProvider
}

func newSFTPReaderFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (s *sftpReader, err error) {
	s = &sftpReader{
		log: mgr.Logger(),
		mgr: mgr,
	}

	if s.address, err = conf.FieldString(siFieldAddress); err != nil {
		return
	}
	if s.paths, err = conf.FieldStringList(siFieldPaths); err != nil {
		return
	}
	if s.creds, err = credentialsFromParsed(conf.Namespace(siFieldCredentials)); err != nil {
		return
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
				return nil, fmt.Errorf("cache resource '%v' was not found", s.watcherCache)
			}
		}
	}

	return
}

func (s *sftpReader) Connect(ctx context.Context) error {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	client, cleanup, err := s.initClient()
	if err != nil {
		if errors.Is(err, sftp.ErrSSHFxConnectionLost) {
			err = service.ErrNotConnected
		}
		return err
	}
	defer cleanup()

	if s.pathProvider == nil {
		s.pathProvider = s.getFilePathProvider(client)
	}
	return nil
}

func (s *sftpReader) initClient() (*sftp.Client, func(), error) {
	if s.client == nil {
		var err error
		if s.client, err = s.creds.GetClient(s.mgr.FS(), s.address); err != nil {
			return nil, nil, fmt.Errorf("initializing SFTP client: %w", err)
		}
	}

	cleanup := func() {}
	if s.closed {
		cleanup = s.closeClient
	}

	return s.client, cleanup, nil
}

func (s *sftpReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	parts, codecAckFn, err := s.tryReadBatchV2(ctx)
	if errors.Is(err, sftp.ErrSSHFxConnectionLost) {
		s.closeScanner(ctx)
		s.closeClient()
		return nil, nil, service.ErrNotConnected
	}
	return parts, codecAckFn, nil
}

func (s *sftpReader) tryReadBatchV2(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	client, cleanup, err := s.initClient()
	if err != nil {
		return nil, nil, err
	}
	defer cleanup()

	if err := s.initScanner(ctx, client); err != nil {
		return nil, nil, err
	}

	parts, codecAckFn, err := s.scanner.NextBatch(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		_ = s.scanner.Close(ctx)
		s.scanner = nil
		s.currentPath = ""
		if errors.Is(err, io.EOF) {
			err = service.ErrNotConnected
		}
		return nil, nil, err
	}

	for _, part := range parts {
		part.MetaSetMut("sftp_path", s.currentPath)
	}

	return parts, codecAckFn, nil
}

func (s *sftpReader) initScanner(ctx context.Context, client *sftp.Client) error {
	if s.scanner != nil {
		return nil
	}

	var file *sftp.File
	var path string
	for {
		var ok bool
		var err error
		path, ok, err = s.pathProvider.Next(ctx, client)
		if err != nil {
			return fmt.Errorf("finding next file path: %w", err)
		}
		if !ok {
			return service.ErrEndOfInput
		}

		file, err = client.Open(path)
		if err != nil {
			s.log.With("path", path, "err", err.Error()).Warn("Unable to open previously identified file")
			if os.IsNotExist(err) {
				// If we failed to open the file because it no longer exists then we
				// can "ack" the path as we're done with it. Otherwise we "nack" it
				// with the error as we'll want to reprocess it again later.
				err = nil
			}
			if ackErr := s.pathProvider.Ack(ctx, path, err); ackErr != nil {
				s.log.With("error", ackErr).Warnf("Failed to acknowledge path: %s", path)
			}
			continue
		}

		details := service.NewScannerSourceDetails()
		details.SetName(path)
		scanner, err := s.scannerCtor.Create(file, s.newCodecAckFn(path), details)
		if err != nil {
			_ = file.Close()
			return fmt.Errorf("creating scanner: %w", err)
		}

		s.scanner = scanner
		s.currentPath = path
		return nil
	}
}

func (s *sftpReader) newCodecAckFn(path string) service.AckFunc {
	return func(ctx context.Context, aErr error) error {
		s.stateLock.Lock()
		defer s.stateLock.Unlock()

		if err := s.pathProvider.Ack(ctx, path, aErr); err != nil {
			s.log.With("error", err).Warnf("Failed to acknowledge path: %s", path)
		}
		if aErr != nil {
			return nil
		}

		if s.deleteOnFinish {
			client, cleanup, err := s.initClient()
			if err != nil {
				return err
			}
			defer cleanup()

			if err := client.Remove(path); err != nil {
				return fmt.Errorf("remove %v: %w", path, err)
			}
		}

		return nil
	}
}

func (s *sftpReader) Close(ctx context.Context) error {
	s.stateLock.Lock()
	defer s.stateLock.Unlock()

	s.closeScanner(ctx)
	s.closeClient()
	s.closed = true
	return nil
}

func (s *sftpReader) closeScanner(ctx context.Context) {
	if s.scanner != nil {
		if err := s.scanner.Close(ctx); err != nil {
			s.log.With("error", err).Error("Failed to close scanner")
		}
		s.scanner = nil
		s.currentPath = ""
	}
}

func (s *sftpReader) closeClient() {
	if s.client == nil {
		if err := s.client.Close(); err != nil {
			s.log.With("error", err).Error("Failed to close client")
		}
		s.client = nil
	}
}

type pathProvider interface {
	Next(context.Context, *sftp.Client) (string, bool, error)
	Ack(context.Context, string, error) error
}

type staticPathProviderV2 struct {
	expandedPaths []string
}

func (s *staticPathProviderV2) Next(context.Context, *sftp.Client) (string, bool, error) {
	if len(s.expandedPaths) == 0 {
		return "", false, nil
	}
	path := s.expandedPaths[0]
	s.expandedPaths = s.expandedPaths[1:]
	return path, true, nil
}

func (s *staticPathProviderV2) Ack(context.Context, string, error) error {
	return nil
}

type watcherPathProvider struct {
	mgr          *service.Resources
	cacheName    string
	pollInterval time.Duration
	minAge       time.Duration
	targetPaths  []string

	expandedPaths []string
	nextPoll      time.Time
	followUpPoll  bool
}

func (w *watcherPathProvider) Next(ctx context.Context, client *sftp.Client) (string, bool, error) {
	for {
		if len(w.expandedPaths) > 0 {
			nextPath := w.expandedPaths[0]
			w.expandedPaths = w.expandedPaths[1:]
			return nextPath, true, nil
		}

		if waitFor := time.Until(w.nextPoll); w.nextPoll.IsZero() || waitFor > 0 {
			w.nextPoll = time.Now().Add(w.pollInterval)
			select {
			case <-time.After(waitFor):
			case <-ctx.Done():
				return "", false, ctx.Err()
			}
		}

		if cerr := w.mgr.AccessCache(ctx, w.cacheName, func(cache service.Cache) {
			for _, p := range w.targetPaths {
				paths, err := client.Glob(p)
				if err != nil {
					w.mgr.Logger().With("error", err, "path", p).Warn("Failed to scan files from path")
					continue
				}

				for _, path := range paths {
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
			return "", false, fmt.Errorf("error obtaining cache: %v", cerr)
		}
		w.followUpPoll = true
	}
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

func (s *sftpReader) getFilePathProvider(client *sftp.Client) pathProvider {
	if !s.watcherEnabled {
		provider := &staticPathProviderV2{}
		for _, path := range s.paths {
			expandedPaths, err := client.Glob(path)
			if err != nil {
				s.log.Warnf("Failed to scan files from path %v: %v", path, err)
				continue
			}
			provider.expandedPaths = append(provider.expandedPaths, expandedPaths...)
		}
		return provider
	}

	return &watcherPathProvider{
		mgr:          s.mgr,
		cacheName:    s.watcherCache,
		pollInterval: s.watcherPollInterval,
		minAge:       s.watcherMinAge,
		targetPaths:  s.paths,
	}
}
