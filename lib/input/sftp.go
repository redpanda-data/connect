package input

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/docs"
	sftpSetup "github.com/Jeffail/benthos/v3/internal/impl/sftp"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/pkg/sftp"
)

func init() {
	watcherDocs := docs.FieldSpecs{
		docs.FieldCommon(
			"enabled",
			"Whether file watching is enabled.",
		),
		docs.FieldCommon(
			"minimum_age",
			"The minimum period of time since a file was last updated before attempting to consume it. Increasing this period decreases the likelihood that a file will be consumed whilst it is still being written to.",
			"10s", "1m", "10m",
		),
		docs.FieldCommon(
			"poll_interval",
			"The interval between each attempt to scan the target paths for new files.",
			"100ms", "1s",
		),
		docs.FieldCommon(
			"cache",
			"A [cache resource](/docs/components/caches/about) for storing the paths of files already consumed.",
		),
	}

	Constructors[TypeSFTP] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			r, err := newSFTPReader(conf.SFTP, mgr, log, stats)
			if err != nil {
				return nil, err
			}
			return NewAsyncReader(
				TypeSFTP,
				true,
				reader.NewAsyncPreserver(r),
				log, stats,
			)
		}),
		Status:  docs.StatusExperimental,
		Version: "3.39.0",
		Summary: `Consumes files from a server over SFTP.`,
		Description: `
## Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- sftp_path
` + "```" + `

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#metadata).`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"address",
				"The address of the server to connect to that has the target files.",
			),
			docs.FieldCommon(
				"credentials",
				"The credentials to use to log into the server.",
			).WithChildren(sftpSetup.CredentialsDocs()...),
			docs.FieldString(
				"paths",
				"A list of paths to consume sequentially. Glob patterns are supported.",
			).Array(),
			codec.ReaderDocs,
			docs.FieldAdvanced("delete_on_finish", "Whether to delete files from the server once they are processed."),
			docs.FieldAdvanced("max_buffer", "The largest token size expected when consuming delimited files."),
			docs.FieldCommon(
				"watcher",
				"An experimental mode whereby the input will periodically scan the target paths for new files and consume them, when all files are consumed the input will continue polling for new files.",
			).WithChildren(watcherDocs...).AtVersion("3.42.0"),
		},
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

//------------------------------------------------------------------------------

type watcherConfig struct {
	Enabled      bool   `json:"enabled" yaml:"enabled"`
	MinimumAge   string `json:"minimum_age" yaml:"minimum_age"`
	PollInterval string `json:"poll_interval" yaml:"poll_interval"`
	Cache        string `json:"cache" yaml:"cache"`
}

// SFTPConfig contains configuration fields for the SFTP input type.
type SFTPConfig struct {
	Address        string                `json:"address" yaml:"address"`
	Credentials    sftpSetup.Credentials `json:"credentials" yaml:"credentials"`
	Paths          []string              `json:"paths" yaml:"paths"`
	Codec          string                `json:"codec" yaml:"codec"`
	DeleteOnFinish bool                  `json:"delete_on_finish" yaml:"delete_on_finish"`
	MaxBuffer      int                   `json:"max_buffer" yaml:"max_buffer"`
	Watcher        watcherConfig         `json:"watcher" yaml:"watcher"`
}

// NewSFTPConfig creates a new SFTPConfig with default values.
func NewSFTPConfig() SFTPConfig {
	return SFTPConfig{
		Address:        "",
		Credentials:    sftpSetup.Credentials{},
		Paths:          []string{},
		Codec:          "all-bytes",
		DeleteOnFinish: false,
		MaxBuffer:      1000000,
		Watcher: watcherConfig{
			Enabled:      false,
			MinimumAge:   "1s",
			PollInterval: "1s",
			Cache:        "",
		},
	}
}

//------------------------------------------------------------------------------

type sftpReader struct {
	conf SFTPConfig

	log   log.Modular
	stats metrics.Type
	mgr   types.Manager

	client *sftp.Client

	paths       []string
	scannerCtor codec.ReaderConstructor

	scannerMut  sync.Mutex
	scanner     codec.Reader
	currentPath string

	watcherPollInterval time.Duration
	watcherMinAge       time.Duration
}

func newSFTPReader(conf SFTPConfig, mgr types.Manager, log log.Modular, stats metrics.Type) (*sftpReader, error) {
	codecConf := codec.NewReaderConfig()
	codecConf.MaxScanTokenSize = conf.MaxBuffer
	ctor, err := codec.GetReader(conf.Codec, codecConf)
	if err != nil {
		return nil, err
	}

	var watcherPollInterval, watcherMinAge time.Duration
	if conf.Watcher.Enabled {
		if watcherPollInterval, err = time.ParseDuration(conf.Watcher.PollInterval); err != nil {
			return nil, fmt.Errorf("failed to parse watcher poll interval: %w", err)
		}

		if watcherMinAge, err = time.ParseDuration(conf.Watcher.MinimumAge); err != nil {
			return nil, fmt.Errorf("failed to parse watcher minimum age: %w", err)
		}

		if conf.Watcher.Cache == "" {
			return nil, errors.New("a cache must be specified when watcher mode is enabled")
		}

		if err := interop.ProbeCache(context.Background(), mgr, conf.Watcher.Cache); err != nil {
			return nil, err
		}
	}

	s := &sftpReader{
		conf:                conf,
		log:                 log,
		stats:               stats,
		mgr:                 mgr,
		scannerCtor:         ctor,
		watcherPollInterval: watcherPollInterval,
		watcherMinAge:       watcherMinAge,
	}

	return s, err
}

// ConnectWithContext attempts to establish a connection to the target SFTP server.
func (s *sftpReader) ConnectWithContext(ctx context.Context) error {
	var err error

	s.scannerMut.Lock()
	defer s.scannerMut.Unlock()

	if s.scanner != nil {
		return nil
	}

	if s.client == nil {
		if s.client, err = s.conf.Credentials.GetClient(s.conf.Address); err != nil {
			return err
		}
		s.log.Debugln("Finding more paths")
		s.paths, err = s.getFilePaths(ctx)
		if err != nil {
			return err
		}
	}

	if len(s.paths) == 0 {
		if !s.conf.Watcher.Enabled {
			s.client.Close()
			s.client = nil
			s.log.Debugln("Paths exhausted, closing input")
			return component.ErrTypeClosed
		}
		select {
		case <-time.After(s.watcherPollInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
		s.paths, err = s.getFilePaths(ctx)
		return err
	}

	nextPath := s.paths[0]

	file, err := s.client.Open(nextPath)
	if err != nil {
		return err
	}

	if s.scanner, err = s.scannerCtor(nextPath, file, func(ctx context.Context, err error) error {
		if err == nil && s.conf.DeleteOnFinish {
			return s.client.Remove(nextPath)
		}
		return nil
	}); err != nil {
		file.Close()
		return err
	}

	s.currentPath = nextPath
	s.paths = s.paths[1:]

	s.log.Infof("Consuming from file '%v'\n", nextPath)
	return err
}

// ReadWithContext attempts to read a new message from the target file(s) on the server.
func (s *sftpReader) ReadWithContext(ctx context.Context) (*message.Batch, reader.AsyncAckFn, error) {
	s.scannerMut.Lock()
	defer s.scannerMut.Unlock()

	if s.scanner == nil || s.client == nil {
		return nil, nil, component.ErrNotConnected
	}

	parts, codecAckFn, err := s.scanner.Next(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			err = component.ErrTimeout
		}
		if err != component.ErrTimeout {
			if s.conf.Watcher.Enabled {
				var setErr error
				if cerr := interop.AccessCache(ctx, s.mgr, s.conf.Watcher.Cache, func(cache types.Cache) {
					setErr = cache.Set(ctx, s.currentPath, []byte("@"), nil)
				}); cerr != nil {
					return nil, nil, fmt.Errorf("failed to get the cache for sftp watcher mode: %v", cerr)
				}
				if setErr != nil {
					return nil, nil, fmt.Errorf("failed to update path in cache %s: %v", s.currentPath, err)
				}
			}
			s.scanner.Close(ctx)
			s.scanner = nil
		}
		if errors.Is(err, io.EOF) {
			err = component.ErrTimeout
		}
		return nil, nil, err
	}

	for _, part := range parts {
		part.MetaSet("sftp_path", s.currentPath)
	}
	msg := message.QuickBatch(nil)
	msg.Append(parts...)

	return msg, func(ctx context.Context, res types.Response) error {
		return codecAckFn(ctx, res.AckError())
	}, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (s *sftpReader) CloseAsync() {
	go func() {
		s.scannerMut.Lock()
		if s.scanner != nil {
			s.scanner.Close(context.Background())
			s.scanner = nil
			s.paths = nil
		}
		if s.client != nil {
			s.client.Close()
			s.client = nil
		}
		s.scannerMut.Unlock()
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (s *sftpReader) WaitForClose(timeout time.Duration) error {
	return nil
}

func (s *sftpReader) getFilePaths(ctx context.Context) ([]string, error) {
	var filepaths []string
	if !s.conf.Watcher.Enabled {
		for _, p := range s.conf.Paths {
			paths, err := s.client.Glob(p)
			if err != nil {
				s.log.Warnf("Failed to scan files from path %v: %v\n", p, err)
				continue
			}
			filepaths = append(filepaths, paths...)
		}
		return filepaths, nil
	}

	if cerr := interop.AccessCache(ctx, s.mgr, s.conf.Watcher.Cache, func(cache types.Cache) {
		for _, p := range s.conf.Paths {
			paths, err := s.client.Glob(p)
			if err != nil {
				s.log.Warnf("Failed to scan files from path %v: %v\n", p, err)
				continue
			}

			for _, path := range paths {
				info, err := s.client.Stat(path)
				if err != nil {
					s.log.Warnf("Failed to stat path %v: %v\n", path, err)
					continue
				}
				if time.Since(info.ModTime()) < s.watcherMinAge {
					continue
				}
				if _, err := cache.Get(ctx, path); err != nil {
					filepaths = append(filepaths, path)
				} else if err = cache.Set(ctx, path, []byte("@"), nil); err != nil { // Reset the TTL for the path
					s.log.Warnf("Failed to set key in cache for path %v: %v\n", path, err)
				}
			}
		}
	}); cerr != nil {
		return nil, fmt.Errorf("error getting cache in getFilePaths: %v", cerr)
	}
	return filepaths, nil
}
