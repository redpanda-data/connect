package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/trace"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/api"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/stream"
	strmmgr "github.com/benthosdev/benthos/v4/internal/stream/manager"
)

var testSuffix = "_benthos_test"

type stoppable interface {
	Stop(ctx context.Context) error
}

//------------------------------------------------------------------------------

func readConfig(path string, streamsMode bool, resourcesPaths, streamsPaths, overrides []string) (mainPath string, inferred bool, conf *config.Reader) {
	if path == "" {
		// Iterate default config paths
		for _, dpath := range []string{
			"/benthos.yaml",
			"/etc/benthos/config.yaml",
			"/etc/benthos.yaml",
		} {
			if _, err := ifs.OS().Stat(dpath); err == nil {
				inferred = true
				path = dpath
				break
			}
		}
	}
	opts := []config.OptFunc{
		config.OptAddOverrides(overrides...),
		config.OptTestSuffix(testSuffix),
	}
	if streamsMode {
		opts = append(opts, config.OptSetStreamPaths(streamsPaths...))
	}
	return path, inferred, config.NewReader(path, resourcesPaths, opts...)
}

//------------------------------------------------------------------------------

func initStreamsMode(
	strict, watching, enableAPI bool,
	confReader *config.Reader,
	mgr *manager.Type,
) stoppable {
	logger := mgr.Logger()
	streamMgr := strmmgr.New(mgr, strmmgr.OptAPIEnabled(enableAPI))

	streamConfs := map[string]stream.Config{}
	lints, err := confReader.ReadStreams(streamConfs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Stream configuration file read error: %v\n", err)
		os.Exit(1)
	}

	for _, lint := range lints {
		if strict {
			logger.With("lint", lint).Errorln("Config lint error")
		} else {
			logger.With("lint", lint).Warnln("Config lint error")
		}
	}
	if strict && len(lints) > 0 {
		logger.Errorln("Shutting down due to stream linter errors, to prevent shutdown run Benthos with --chilled")
		os.Exit(1)
	}

	for id, conf := range streamConfs {
		if err := streamMgr.Create(id, conf); err != nil {
			logger.Errorf("Failed to create stream (%v): %v\n", id, err)
			os.Exit(1)
		}
	}
	logger.Infoln("Launching benthos in streams mode, use CTRL+C to close")

	if err := confReader.SubscribeStreamChanges(func(id string, newStreamConf *stream.Config) error {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		defer done()

		var updateErr error
		if newStreamConf != nil {
			if updateErr = streamMgr.Update(ctx, id, *newStreamConf); updateErr != nil && errors.Is(updateErr, strmmgr.ErrStreamDoesNotExist) {
				updateErr = streamMgr.Create(id, *newStreamConf)
			}
		} else {
			if updateErr = streamMgr.Delete(ctx, id); updateErr != nil && errors.Is(updateErr, strmmgr.ErrStreamDoesNotExist) {
				updateErr = nil
			}
		}
		return updateErr
	}); err != nil {
		logger.Errorf("Failed to create stream config watcher: %v", err)
		os.Exit(1)
	}

	if watching {
		if err := confReader.BeginFileWatching(mgr, strict); err != nil {
			logger.Errorf("Failed to create stream config watcher: %v", err)
			os.Exit(1)
		}
	}
	return streamMgr
}

type swappableStopper struct {
	stopped bool
	current stoppable
	mut     sync.Mutex
}

func (s *swappableStopper) Stop(ctx context.Context) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.stopped {
		return nil
	}

	s.stopped = true
	return s.current.Stop(ctx)
}

func (s *swappableStopper) Replace(fn func() (stoppable, error)) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.stopped {
		// If the outer stream has been stopped then do not create a new one.
		return nil
	}

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	if err := s.current.Stop(ctx); err != nil {
		return fmt.Errorf("failed to stop active stream: %w", err)
	}

	newStoppable, err := fn()
	if err != nil {
		return fmt.Errorf("failed to init updated stream: %w", err)
	}

	s.current = newStoppable
	return nil
}

func initNormalMode(
	conf config.Type,
	strict, watching bool,
	confReader *config.Reader,
	mgr *manager.Type,
) (newStream stoppable, stoppedChan chan struct{}) {
	logger := mgr.Logger()

	stoppedChan = make(chan struct{})
	streamInit := func() (stoppable, error) {
		return stream.New(conf.Config, mgr, stream.OptOnClose(func() {
			if !watching {
				close(stoppedChan)
			}
		}))
	}

	var stoppableStream swappableStopper

	var err error
	if stoppableStream.current, err = streamInit(); err != nil {
		logger.Errorf("Service closing due to: %v\n", err)
		os.Exit(1)
	}
	logger.Infoln("Launching a benthos instance, use CTRL+C to close")

	if err := confReader.SubscribeConfigChanges(func(newStreamConf stream.Config) error {
		return stoppableStream.Replace(func() (stoppable, error) {
			conf.Config = newStreamConf
			return streamInit()
		})
	}); err != nil {
		logger.Errorf("Failed to create config file watcher: %v", err)
		os.Exit(1)
	}

	if watching {
		if err := confReader.BeginFileWatching(mgr, strict); err != nil {
			logger.Errorf("Failed to create config file watcher: %v", err)
			os.Exit(1)
		}
	}

	newStream = &stoppableStream
	return
}

func cmdService(
	confPath string,
	resourcesPaths []string,
	confOverrides []string,
	overrideLogLevel string,
	strict, watching, enableStreamsAPI, namespaceStreamEndpoints bool,
	streamsMode bool,
	streamsPaths []string,
) int {
	mainPath, inferredMainPath, confReader := readConfig(confPath, streamsMode, resourcesPaths, streamsPaths, confOverrides)
	conf := config.New()

	lints, err := confReader.Read(&conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
		return 1
	}

	if len(overrideLogLevel) > 0 {
		conf.Logger.LogLevel = strings.ToUpper(overrideLogLevel)
	}

	// Logging and stats aggregation.
	var logger log.Modular

	if conf.Logger.File.Path != "" {
		var writer io.Writer
		if conf.Logger.File.Rotate {
			writer = &lumberjack.Logger{
				Filename:   conf.Logger.File.Path,
				MaxSize:    10,
				MaxAge:     conf.Logger.File.RotateMaxAge,
				MaxBackups: 1,
				Compress:   true,
			}
		} else {
			var fw fs.File
			if fw, err = ifs.OS().OpenFile(conf.Logger.File.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o666); err == nil {
				var isw bool
				if writer, isw = fw.(io.Writer); !isw {
					err = errors.New("failed to open a writable file")
				}
			}
		}
		if err == nil {
			logger, err = log.NewV2(writer, conf.Logger)
		}
	} else {
		// Note: Only log to Stderr if our output is stdout, brokers aren't counted
		// here as this is only a special circumstance for very basic use cases.
		if !streamsMode && conf.Output.Type == "stdout" {
			logger, err = log.NewV2(os.Stderr, conf.Logger)
		} else {
			logger, err = log.NewV2(os.Stdout, conf.Logger)
		}
	}
	if err != nil {
		fmt.Printf("Failed to create logger: %v\n", err)
		return 1
	}

	if mainPath == "" {
		logger.Infof("Running without a main config file")
	} else if inferredMainPath {
		logger.With("path", mainPath).Infof("Running main config from file found in a default path")
	} else {
		logger.With("path", mainPath).Infof("Running main config from specified file")
	}

	for _, lint := range lints {
		if strict {
			logger.With("lint", lint).Errorln("Config lint error")
		} else {
			logger.With("lint", lint).Warnln("Config lint error")
		}
	}
	if strict && len(lints) > 0 {
		logger.Errorln("Shutting down due to linter errors, to prevent shutdown run Benthos with --chilled")
		return 1
	}

	// We use a temporary manager with just the logger initialised for metrics
	// instantiation. Doing this means that metrics plugins will use a global
	// environment for child plugins and bloblang mappings, which we might want
	// to revise in future.
	tmpMgr := mock.NewManager()
	tmpMgr.L = logger

	// Create our metrics type.
	var stats *metrics.Namespaced
	stats, err = bundle.AllMetrics.Init(conf.Metrics, tmpMgr)
	for err != nil {
		logger.Errorf("Failed to connect to metrics aggregator: %v\n", err)
		return 1
	}
	defer func() {
		if sCloseErr := stats.Close(); sCloseErr != nil {
			logger.Errorf("Failed to cleanly close metrics aggregator: %v\n", sCloseErr)
		}
	}()

	// Create our tracer type.
	var trac trace.TracerProvider
	if trac, err = bundle.AllTracers.Init(conf.Tracer, tmpMgr); err != nil {
		logger.Errorf("Failed to initialise tracer: %v\n", err)
		return 1
	}
	defer func() {
		if shutter, ok := trac.(interface {
			Shutdown(context.Context) error
		}); ok {
			_ = shutter.Shutdown(context.Background())
		}
	}()

	// Create HTTP API with a sanitised service config.
	var sanitNode yaml.Node
	err = sanitNode.Encode(conf)
	if err == nil {
		sanitConf := docs.NewSanitiseConfig()
		sanitConf.RemoveTypeField = true
		sanitConf.ScrubSecrets = true
		err = config.Spec().SanitiseYAML(&sanitNode, sanitConf)
	}
	if err != nil {
		logger.Warnf("Failed to generate sanitised config: %v\n", err)
	}
	var httpServer *api.Type
	if httpServer, err = api.New(Version, DateBuilt, conf.HTTP, sanitNode, logger, stats); err != nil {
		logger.Errorf("Failed to initialise API: %v\n", err)
		return 1
	}

	// Create resource manager.
	manager, err := manager.New(
		conf.ResourceConfig,
		manager.OptSetAPIReg(httpServer),
		manager.OptSetStreamHTTPNamespacing(namespaceStreamEndpoints),
		manager.OptSetLogger(logger),
		manager.OptSetMetrics(stats),
		manager.OptSetTracer(trac),
		manager.OptSetStreamsMode(streamsMode),
	)
	if err != nil {
		logger.Errorf("Failed to create resource: %v\n", err)
		return 1
	}

	var stoppableStream stoppable
	var dataStreamClosedChan chan struct{}

	// Create data streams.
	if streamsMode {
		stoppableStream = initStreamsMode(strict, watching, enableStreamsAPI, confReader, manager)
	} else {
		stoppableStream, dataStreamClosedChan = initNormalMode(conf, strict, watching, confReader, manager)
	}

	// Start HTTP server.
	httpServerClosedChan := make(chan struct{})
	go func() {
		httpErr := httpServer.ListenAndServe()
		if httpErr != nil && httpErr != http.ErrServerClosed {
			logger.Errorf("HTTP Server error: %v\n", httpErr)
		}
		close(httpServerClosedChan)
	}()

	var exitDelay time.Duration
	if td := conf.SystemCloseDelay; len(td) > 0 {
		var err error
		if exitDelay, err = time.ParseDuration(td); err != nil {
			logger.Errorf("Failed to parse shutdown delay period string: %v\n", err)
			return 1
		}
	}

	var exitTimeout time.Duration
	if tout := conf.SystemCloseTimeout; len(tout) > 0 {
		var err error
		if exitTimeout, err = time.ParseDuration(tout); err != nil {
			logger.Errorf("Failed to parse shutdown timeout period string: %v\n", err)
			return 1
		}
	}

	// Defer clean up.
	defer func() {
		if exitDelay > 0 {
			logger.Infof("Shutdown delay is in effect for %s\n", exitDelay)
			if err := delayShutdown(context.Background(), exitDelay); err != nil {
				logger.Errorf("Shutdown delay failed: %s", err)
			}
		}

		go func() {
			_ = httpServer.Shutdown(context.Background())
			select {
			case <-httpServerClosedChan:
			case <-time.After(exitTimeout / 2):
				logger.Warnln("Service failed to close HTTP server gracefully in time")
			}
		}()

		go func() {
			<-time.After(exitTimeout + time.Second)
			logger.Warnln(
				"Service failed to close cleanly within allocated time." +
					" Exiting forcefully and dumping stack trace to stderr",
			)
			_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			os.Exit(1)
		}()

		if err := confReader.Close(context.Background()); err != nil {
			logger.Warnf("Failed to cleanly shut down file watcher: %v", err)
			os.Exit(1)
		}

		ctx, done := context.WithTimeout(context.Background(), exitTimeout)
		if err := stoppableStream.Stop(ctx); err != nil {
			os.Exit(1)
		}

		manager.TriggerStopConsuming()
		if err := manager.WaitForClose(ctx); err != nil {
			logger.Warnf(
				"Service failed to close cleanly within allocated time: %v."+
					" Exiting forcefully and dumping stack trace to stderr\n", err,
			)
			_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			os.Exit(1)
		}
		done()
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for termination signal
	select {
	case sig := <-sigChan:
		var sigName string
		switch sig {
		case os.Interrupt:
			sigName = "SIGINT"
		case syscall.SIGTERM:
			sigName = "SIGTERM"
		default:
			sigName = sig.String()
		}
		logger.Infof("Received %s, the service is closing", sigName)
	case <-dataStreamClosedChan:
		logger.Infoln("Pipeline has terminated. Shutting down the service")
	case <-httpServerClosedChan:
		logger.Infoln("HTTP Server has terminated. Shutting down the service")
	case <-optContext.Done():
		logger.Infoln("Run context was cancelled. Shutting down the service")
	}
	return 0
}

func delayShutdown(ctx context.Context, duration time.Duration) error {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	delayCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	select {
	case <-delayCtx.Done():
		err := delayCtx.Err()
		if err != nil && err != context.DeadlineExceeded {
			return err
		}
	case sig := <-sigChan:
		return fmt.Errorf("shutdown delay interrupted by signal: %s", sig)
	}

	return nil
}
