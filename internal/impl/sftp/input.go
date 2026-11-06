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
	siFieldWatcherCacheMode    = "cache_mode"
)

func sftpInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
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
			Description("The maximum number of SFTP sessions.").Version("4.59.0").
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
					ShortDescription("How long since a file was last updated before it is consumed, avoiding partially written files.").
					Default("1s").
					Examples("10s", "1m", "10m"),
				service.NewDurationField(siFieldWatcherPollInterval).
					Description("The interval between each attempt to scan the target paths for new files.").
					Default("1s").
					Examples("100ms", "1s"),
				service.NewStringField(siFieldWatcherCache).
					Description("A xref:components:caches/about.adoc[cache resource] for storing the paths of files already consumed.").
					ShortDescription("A cache resource storing the paths of files already consumed.").
					Default(""),
				service.NewStringEnumField(siFieldWatcherCacheMode, watcherCacheModePathName, watcherCacheModePathAndModTimeName).
					Description("Controls what the watcher stores in the cache for each consumed file. "+
						"`path` records only the path, so a consumed file is never consumed again. "+
						"`path_and_mod_time` also records the file's modification time, and consumes a previously consumed file again if its modification time changes. "+
						"Re-consumed files are emitted again in full, so downstream consumers may see duplicate data; "+
						"the `sftp_mod_time` metadata field can be used to tell versions apart. "+
						"SFTP reports modification times in whole seconds, so changes are only reliably detected when `minimum_age` is at least `1s`. "+
						"When an existing cache is switched to `path_and_mod_time`, each entry written in `path` mode is overwritten in the cache with the file's current modification time the next time the file is polled. "+
						"Only the cache entry changes: the file is not read again, nothing is sent to the output, and the file on the SFTP server is not modified. "+
						"With a topic-backed cache such as `redpanda`, this produces one new record per file to the cache topic, not to the pipeline's output. "+
						"This cannot be undone, so a file that was modified after it was last consumed but before the switch is never consumed again unless its cache entry is removed. "+
						"Downgrading to a Redpanda Connect version without `cache_mode` while files are pending leaves those files unconsumed until their cache entries are removed.").
					ShortDescription("What the watcher stores in the cache per file: path alone, or path_and_mod_time to re-consume modified files.").
					Version("4.116.0").
					Default(watcherCacheModePathName).
					Advanced(),
			).Description("An experimental mode whereby the input will periodically scan the target paths for new files and consume them, when all files are consumed the input will continue polling for new files.").
				ShortDescription("An experimental mode that periodically rescans the target paths for new files to consume.").
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
	watcherCacheMode    watcherCacheMode

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
			var cacheModeStr string
			if cacheModeStr, err = wConf.FieldString(siFieldWatcherCacheMode); err != nil {
				return
			}
			if s.watcherCacheMode, err = parseWatcherCacheMode(cacheModeStr); err != nil {
				return nil, err
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
			return nil, fmt.Errorf("creating SFTP client: %w", err)
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
		return fmt.Errorf("connecting to SFTP server: %w", err)
	}

	if s.watcherEnabled && s.pathProvider == nil {
		s.pathProvider = &watcherPathProvider{
			clientPool:   s.sftpClientPool,
			mgr:          s.mgr,
			cacheName:    s.watcherCache,
			pollInterval: s.watcherPollInterval,
			minAge:       s.watcherMinAge,
			targetPaths:  s.paths,
			cacheMode:    s.watcherCacheMode,
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
		return fmt.Errorf("closing SSH client: %s", err)
	}

	s.sshClient = nil

	return nil
}

// tryReadBatch reads the next batch from the current file. When the current
// file is done it opens the next file and continues, so that a file boundary
// is never reported to the caller as a lost connection. It returns
// service.ErrEndOfInput when the path provider has no more files.
func (s *sftpReader) tryReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	for {
		// A shutdown must interrupt the rotation between files.
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}

		scanner, err := s.initScanner(ctx)
		if err != nil {
			return nil, nil, err
		}

		parts, codecAckFn, err := scanner.NextBatch(ctx)
		if err == nil {
			for _, part := range parts {
				part.MetaSetMut("sftp_path", s.currentFileInfo.path)
				part.MetaSetMut("sftp_mod_time", s.currentFileInfo.modTime)
			}
			return parts, codecAckFn, nil
		}

		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		s.closeScanner(ctx)

		if errors.Is(err, io.EOF) {
			// The current file is done. Move on to the next file.
			continue
		}
		return nil, nil, err
	}
}

// closeScanner closes the current file scanner, if any, and clears it.
func (s *sftpReader) closeScanner(ctx context.Context) {
	s.stateLock.Lock()
	scanner := s.scanner
	s.stateLock.Unlock()

	if scanner == nil {
		return
	}
	if err := scanner.Close(ctx); err != nil {
		s.log.With("error", err).Error("Failed to close scanner")
	}

	s.stateLock.Lock()
	s.scanner = nil
	s.stateLock.Unlock()
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
	for {
		// polled carries the modification time seen when the path was found,
		// which is what gets recorded on ack.
		polled, ok, err := s.pathProvider.Next(ctx)
		if err != nil {
			return nil, fmt.Errorf("finding next file path: %w", err)
		}
		if !ok {
			return nil, service.ErrEndOfInput
		}
		path := polled.path

		client, err := s.sftpClientPool.Acquire(ctx)
		if err != nil {
			return nil, fmt.Errorf("acquiring SFTP client: %w", err)
		}

		handleErr := func(err error) {
			s.log.With("path", path, "err", err.Error()).Warn("Failed to open previously identified file")

			if os.IsNotExist(err) {
				// If we failed to open the file because it no longer exists then we
				// can "ack" the path as we're done with it. Otherwise we "nack" it
				// with the error as we'll want to reprocess it again later.
				err = nil
			}
			if ackErr := s.pathProvider.Ack(ctx, polled, err); ackErr != nil {
				s.log.With("error", ackErr).Warnf("Failed to acknowledge path: %s", path)
			}

			s.sftpClientPool.Release(client)
		}

		file, err = client.Open(path)
		if err != nil {
			handleErr(fmt.Errorf("opening file: %w", err))
			continue
		}

		stat, err := file.Stat()
		if err != nil {
			handleErr(fmt.Errorf("statting file: %w", err))
			continue
		}

		f := &sftpFile{
			file: file,
			postCloseFn: func() {
				s.sftpClientPool.Release(client)
			},
		}

		// The ack records the poll-time modification time, because that is the
		// one that passed minimum_age. Modification times have one-second
		// resolution, so the open-time value can belong to a second in which a
		// writer is still changing the file, and recording it could hide that
		// change from a later poll. The open-time value identifies the
		// version that is actually read, so it is used for metadata only.
		info := fileInfo{
			path:    path,
			modTime: stat.ModTime(),
		}

		details := service.NewScannerSourceDetails()
		details.SetName(path)
		scanner, err := s.scannerCtor.Create(f, s.newCodecAckFn(client, polled), details)
		if err != nil {
			if err = f.Close(); err != nil {
				s.log.Errorf("Failed to close file %q: %s", path, err)
			}
			return nil, fmt.Errorf("creating scanner: %w", err)
		}

		s.stateLock.Lock()
		s.scanner = scanner
		s.currentFileInfo = info
		s.stateLock.Unlock()

		return scanner, nil
	}
}

func (s *sftpReader) newCodecAckFn(client *sftp.Client, info fileInfo) service.AckFunc {
	path := info.path
	return func(ctx context.Context, aErr error) error {
		if err := s.pathProvider.Ack(ctx, info, aErr); err != nil {
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
				return fmt.Errorf("removing file %q: %w", path, err)
			}
		}

		return nil
	}
}

type pathProvider interface {
	Next(context.Context) (fileInfo, bool, error)
	Ack(ctx context.Context, info fileInfo, err error) error
}

type staticPathProvider struct {
	expandedPaths []string
}

func (s *staticPathProvider) Next(context.Context) (fileInfo, bool, error) {
	if len(s.expandedPaths) == 0 {
		return fileInfo{}, false, nil
	}
	path := s.expandedPaths[0]
	s.expandedPaths = s.expandedPaths[1:]
	return fileInfo{path: path}, true, nil
}

func (*staticPathProvider) Ack(context.Context, fileInfo, error) error {
	return nil
}

type watcherCacheMode int

const (
	watcherCacheModePath           watcherCacheMode = iota // "path" (default)
	watcherCacheModePathAndModTime                         // "path_and_mod_time"
)

const (
	watcherCacheModePathName           = "path"
	watcherCacheModePathAndModTimeName = "path_and_mod_time"
)

func parseWatcherCacheMode(v string) (watcherCacheMode, error) {
	switch v {
	case watcherCacheModePathName:
		return watcherCacheModePath, nil
	case watcherCacheModePathAndModTimeName:
		return watcherCacheModePathAndModTime, nil
	default:
		return watcherCacheModePath, fmt.Errorf("unknown watcher cache_mode %q", v)
	}
}

const (
	watcherPendingPrefix = '!'
	watcherDonePrefix    = '@'
)

// parseWatcherMarker splits a stored cache value into its marker byte and, when
// present, its encoded modification time. hasModTime is false for the legacy
// bare markers, and modTimeErr is set when a modification time is present but
// cannot be parsed.
func parseWatcherMarker(val []byte) (marker byte, modTime time.Time, hasModTime bool, modTimeErr error) {
	if len(val) == 0 {
		return 0, time.Time{}, false, nil
	}
	marker = val[0]
	if len(val) == 1 {
		return marker, time.Time{}, false, nil
	}
	modTime, modTimeErr = time.Parse(time.RFC3339Nano, string(val[1:]))
	return marker, modTime, true, modTimeErr
}

type watcherAction int

const (
	watcherSkip     watcherAction = iota
	watcherProcess                // queue the path and write pending marker
	watcherBackfill               // do not queue; write done marker with current mtime
)

// decideWatcherAction classifies a cache entry for a polled file. found is
// false when cache.Get returned service.ErrKeyNotFound.
func decideWatcherAction(val []byte, found, followUpPoll bool, mode watcherCacheMode, current time.Time) watcherAction {
	if mode == watcherCacheModePath {
		// Pending values written in path_and_mod_time mode carry a modification
		// time, and are recovered on the first poll like a bare pending marker.
		if !found || (!followUpPoll && len(val) > 0 && val[0] == watcherPendingPrefix) {
			return watcherProcess
		}
		return watcherSkip
	}

	if !found {
		return watcherProcess
	}

	marker, stored, hasModTime, err := parseWatcherMarker(val)
	switch marker {
	case watcherPendingPrefix:
		// Any pending value, including legacy ones, is recovered on the first
		// poll and considered in flight afterwards.
		if followUpPoll {
			return watcherSkip
		}
		return watcherProcess
	case watcherDonePrefix:
		if !hasModTime {
			return watcherBackfill
		}
		if err != nil || !stored.Equal(current) {
			return watcherProcess
		}
		return watcherSkip
	default:
		return watcherProcess
	}
}

type watcherPathProvider struct {
	clientPool   pool.Capped[*sftp.Client]
	mgr          *service.Resources
	cacheName    string
	pollInterval time.Duration
	minAge       time.Duration
	targetPaths  []string
	cacheMode    watcherCacheMode

	expandedPaths []fileInfo
	nextPoll      time.Time
	followUpPoll  bool
}

func (w *watcherPathProvider) Next(ctx context.Context) (fileInfo, bool, error) {
	for {
		if len(w.expandedPaths) > 0 {
			next := w.expandedPaths[0]
			w.expandedPaths = w.expandedPaths[1:]
			return next, true, nil
		}

		if waitFor := time.Until(w.nextPoll); w.nextPoll.IsZero() || waitFor > 0 {
			select {
			case <-time.After(waitFor):
			case <-ctx.Done():
				return fileInfo{}, false, ctx.Err()
			}
		}
		w.nextPoll = time.Now().Add(w.pollInterval)

		if err := w.findNewPaths(ctx); err != nil {
			return fileInfo{}, false, fmt.Errorf("expanding new paths: %w", err)
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

				// If we got an unexpected error obtaining a marker for this
				// path from the cache then we skip that path because the
				// watcher will eventually poll again, and the cache.Get
				// operation will re-run.
				v, err := cache.Get(ctx, path)
				found := err == nil
				if err != nil && !errors.Is(err, service.ErrKeyNotFound) {
					continue
				}

				switch decideWatcherAction(v, found, w.followUpPoll, w.cacheMode, info.ModTime()) {
				case watcherProcess:
					w.expandedPaths = append(w.expandedPaths, fileInfo{path: path, modTime: info.ModTime()})
					if err = cache.Set(ctx, path, w.pendingMarker(info.ModTime()), nil); err != nil {
						// Mark the file target as pending so that we do not reprocess it
						w.mgr.Logger().With("error", err, "path", path).Warn("Failed to mark path as pending")
					}
				case watcherBackfill:
					w.mgr.Logger().With("path", path).Debug("Backfilling legacy done marker with modification time")
					if err = cache.Set(ctx, path, w.doneMarker(info.ModTime()), nil); err != nil {
						w.mgr.Logger().With("error", err, "path", path).Warn("Failed to backfill modification time for path")
					}
				}
			}
		}
	}); cerr != nil {
		return fmt.Errorf("error obtaining cache: %v", cerr)
	}

	return nil
}

func (w *watcherPathProvider) pendingMarker(modTime time.Time) []byte {
	return w.marker(watcherPendingPrefix, modTime)
}

func (w *watcherPathProvider) doneMarker(modTime time.Time) []byte {
	return w.marker(watcherDonePrefix, modTime)
}

func (w *watcherPathProvider) marker(prefix byte, modTime time.Time) []byte {
	if w.cacheMode == watcherCacheModePath {
		return []byte{prefix}
	}
	return append([]byte{prefix}, modTime.UTC().Format(time.RFC3339Nano)...)
}

func (w *watcherPathProvider) Ack(ctx context.Context, info fileInfo, err error) (outErr error) {
	if cerr := w.mgr.AccessCache(ctx, w.cacheName, func(cache service.Cache) {
		if err == nil {
			outErr = cache.Set(ctx, info.path, w.doneMarker(info.modTime), nil)
		} else {
			_ = cache.Delete(ctx, info.path)
		}
	}); cerr != nil {
		outErr = cerr
	}
	return
}
