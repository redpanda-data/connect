package service

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"sync"
	"syscall"
	"time"

	iconfig "github.com/Jeffail/benthos/v3/internal/config"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/api"
	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/stream"
	strmmgr "github.com/Jeffail/benthos/v3/lib/stream/manager"
	"github.com/Jeffail/benthos/v3/lib/tracer"
	"gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

var conf = config.New()
var testSuffix = "_benthos_test"

// OptSetServiceName creates an opt func that allows the default service name
// config fields such as metrics and logging prefixes to be overridden.
func OptSetServiceName(name string) func() {
	return func() {
		testSuffix = fmt.Sprintf("_%v_test", name)
		conf.HTTP.RootPath = "/" + name
		conf.Logger.Prefix = name
		conf.Logger.StaticFields["@service"] = name
		conf.Metrics.HTTP.Prefix = name
		conf.Metrics.Prometheus.Prefix = name
		conf.Metrics.Statsd.Prefix = name
	}
}

// OptOverrideConfigDefaults creates an opt func that allows the provided func
// to override config struct default values before the user config is parsed.
func OptOverrideConfigDefaults(fn func(c *config.Type)) func() {
	return func() {
		fn(&conf)
	}
}

var apiOpts []api.OptFunc

// OptWithAPIMiddleware adds an HTTP middleware to the Benthos API.
func OptWithAPIMiddleware(m func(http.Handler) http.Handler) func() {
	return func() {
		apiOpts = append(apiOpts, api.OptWithMiddleware(m))
	}
}

// OptWithAPITLS replaces the default TLS options of the Benthos API server.
func OptWithAPITLS(c *tls.Config) func() {
	return func() {
		apiOpts = append(apiOpts, api.OptWithTLS(c))
	}
}

type stoppable interface {
	Stop(timeout time.Duration) error
}

// ManagerInitFunc is a function to be called once the Benthos service manager,
// which manages resources shared across all components, is initialised. This is
// a useful time to add additional resources that might be required for custom
// plugins. If a non-nil error is returned the service will terminate.
type ManagerInitFunc func(manager interop.Manager, logger log.Modular, stats metrics.Type) error

// TODO: V4 remove this
var onManagerInit ManagerInitFunc = func(manager interop.Manager, logger log.Modular, stats metrics.Type) error {
	return nil
}

// OptOnManagerInit creates an opt func that allows you to specify a function to
// be called once the service manager is constructed.
func OptOnManagerInit(fn ManagerInitFunc) func() {
	return func() {
		onManagerInit = fn
	}
}

//------------------------------------------------------------------------------

func readConfig(path string, streamsMode bool, resourcesPaths, streamsPaths, overrides []string) *iconfig.Reader {
	if path == "" {
		// Iterate default config paths
		for _, dpath := range []string{
			"/benthos.yaml",
			"/etc/benthos/config.yaml",
			"/etc/benthos.yaml",
		} {
			if _, err := os.Stat(dpath); err == nil {
				fmt.Fprintf(os.Stderr, "Config file not specified, reading from %v\n", dpath)
				path = dpath
				break
			}
		}
	}
	opts := []iconfig.OptFunc{
		iconfig.OptAddOverrides(overrides...),
		iconfig.OptTestSuffix(testSuffix),
	}
	if streamsMode {
		opts = append(opts, iconfig.OptSetStreamPaths(streamsPaths...))
	}
	return iconfig.NewReader(path, resourcesPaths, opts...)
}

//------------------------------------------------------------------------------

func initStreamsMode(
	strict, watching, enableAPI bool,
	confReader *iconfig.Reader,
	strmAPITimeout time.Duration,
	manager *manager.Type,
	logger log.Modular,
	stats metrics.Type,
) stoppable {
	lintlog := logger.NewModule(".linter")

	streamMgr := strmmgr.New(
		strmmgr.OptSetAPITimeout(strmAPITimeout),
		strmmgr.OptSetLogger(logger),
		strmmgr.OptSetManager(manager),
		strmmgr.OptSetStats(stats),
		strmmgr.OptAPIEnabled(enableAPI),
	)

	streamConfs := map[string]stream.Config{}
	lints, err := confReader.ReadStreams(streamConfs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Stream configuration file read error: %v\n", err)
		os.Exit(1)
	}
	if strict && len(lints) > 0 {
		for _, lint := range lints {
			fmt.Fprintln(os.Stderr, lint)
		}
		fmt.Println("Shutting down due to stream linter errors, to prevent shutdown run Benthos with --chilled")
		os.Exit(1)
	}
	for _, lint := range lints {
		lintlog.Infoln(lint)
	}

	for id, conf := range streamConfs {
		if err := streamMgr.Create(id, conf); err != nil {
			logger.Errorf("Failed to create stream (%v): %v\n", id, err)
			os.Exit(1)
		}
	}
	logger.Infoln("Launching benthos in streams mode, use CTRL+C to close.")

	if err := confReader.SubscribeStreamChanges(func(id string, newStreamConf stream.Config) bool {
		if err = streamMgr.Update(id, newStreamConf, time.Second*30); err != nil && errors.Is(err, strmmgr.ErrStreamDoesNotExist) {
			err = streamMgr.Create(id, newStreamConf)
		}
		if err != nil {
			logger.Errorf("Failed to update stream %v: %v", id, err)
			return false
		}
		logger.Infof("Updated stream %v config from file.", id)
		return true
	}); err != nil {
		logger.Errorf("Failed to create stream config watcher: %v", err)
		os.Exit(1)
	}

	if watching {
		if err := confReader.BeginFileWatching(manager, strict); err != nil {
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

func (s *swappableStopper) Stop(timeout time.Duration) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.stopped {
		return nil
	}

	s.stopped = true
	return s.current.Stop(timeout)
}

func (s *swappableStopper) Replace(fn func() (stoppable, error)) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.stopped {
		// If the outter stream has been stopped then do not create a new one.
		return nil
	}

	if err := s.current.Stop(time.Second * 30); err != nil {
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
	strict, watching bool,
	confReader *iconfig.Reader,
	manager *manager.Type,
	logger log.Modular,
	stats metrics.Type,
) (newStream stoppable, stoppedChan chan struct{}) {
	stoppedChan = make(chan struct{})

	streamInit := func() (stoppable, error) {
		return stream.New(
			conf.Config,
			stream.OptSetLogger(logger),
			stream.OptSetStats(stats),
			stream.OptSetManager(manager),
			stream.OptOnClose(func() {
				if !watching {
					close(stoppedChan)
				}
			}),
		)
	}

	var stoppableStream swappableStopper

	var err error
	if stoppableStream.current, err = streamInit(); err != nil {
		logger.Errorf("Service closing due to: %v\n", err)
		os.Exit(1)
	}
	logger.Infoln("Launching a benthos instance, use CTRL+C to close.")

	if err := confReader.SubscribeConfigChanges(func(newStreamConf stream.Config) bool {
		if err := stoppableStream.Replace(func() (stoppable, error) {
			conf.Config = newStreamConf
			return streamInit()
		}); err != nil {
			logger.Errorf("Failed to update stream: %v", err)
			return false
		}

		logger.Infoln("Updated main config from file.")
		return true
	}); err != nil {
		logger.Errorf("Failed to create config file watcher: %v", err)
		os.Exit(1)
	}

	if watching {
		if err := confReader.BeginFileWatching(manager, strict); err != nil {
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
	strict, watching, enableStreamsAPI bool,
	streamsMode bool,
	streamsPaths []string,
) int {
	confReader := readConfig(confPath, streamsMode, resourcesPaths, streamsPaths, confOverrides)

	lints, err := confReader.Read(&conf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
		os.Exit(1)
	}
	if strict && len(lints) > 0 {
		for _, lint := range lints {
			fmt.Fprintln(os.Stderr, lint)
		}
		fmt.Println("Shutting down due to linter errors, to prevent shutdown run Benthos with --chilled")
		return 1
	}

	if len(overrideLogLevel) > 0 {
		conf.Logger.LogLevel = strings.ToUpper(overrideLogLevel)
	}

	// Logging and stats aggregation.
	var logger log.Modular

	// Note: Only log to Stderr if our output is stdout, brokers aren't counted
	// here as this is only a special circumstance for very basic use cases.
	if !streamsMode && conf.Output.Type == "stdout" {
		logger, err = log.NewV2(os.Stderr, conf.Logger)
	} else {
		logger, err = log.NewV2(os.Stdout, conf.Logger)
	}
	if err != nil {
		fmt.Printf("Failed to create logger: %v\n", err)
		return 1
	}

	lintlog := logger.NewModule(".linter")
	for _, lint := range lints {
		lintlog.Infoln(lint)
	}

	// Create our metrics type.
	var stats metrics.Type
	stats, err = metrics.New(conf.Metrics, metrics.OptSetLogger(logger))
	for err != nil {
		logger.Errorf("Failed to connect to metrics aggregator: %v\n", err)
		<-time.After(time.Second)
		stats, err = metrics.New(conf.Metrics, metrics.OptSetLogger(logger))
	}
	defer func() {
		if sCloseErr := stats.Close(); sCloseErr != nil {
			logger.Errorf("Failed to cleanly close metrics aggregator: %v\n", sCloseErr)
		}
	}()

	// Create our tracer type.
	var trac tracer.Type
	if trac, err = tracer.New(conf.Tracer); err != nil {
		logger.Errorf("Failed to initialise tracer: %v\n", err)
		return 1
	}
	defer trac.Close()

	// Create HTTP API with a sanitised service config.
	var sanitNode yaml.Node
	err = sanitNode.Encode(conf)
	if err == nil {
		err = config.Spec().SanitiseYAML(&sanitNode, docs.SanitiseConfig{
			RemoveTypeField: true,
		})
	}
	if err != nil {
		logger.Warnf("Failed to generate sanitised config: %v\n", err)
	}
	var httpServer *api.Type
	if httpServer, err = api.New(Version, DateBuilt, conf.HTTP, sanitNode, logger, stats, apiOpts...); err != nil {
		logger.Errorf("Failed to initialise API: %v\n", err)
		return 1
	}

	// Create resource manager.
	manager, err := manager.NewV2(conf.ResourceConfig, httpServer, logger, stats)
	if err != nil {
		logger.Errorf("Failed to create resource: %v\n", err)
		return 1
	}
	if err = onManagerInit(manager, logger, stats); err != nil {
		logger.Errorf("Failed to initialise manager: %v\n", err)
		return 1
	}

	var stoppableStream stoppable
	var dataStreamClosedChan chan struct{}

	strmAPITimeout := 5 * time.Second
	if cTout := conf.HTTP.ReadTimeout; cTout != "" {
		if tmpTout, _ := time.ParseDuration(cTout); tmpTout > 0 {
			strmAPITimeout = tmpTout
		}
	}

	// Create data streams.
	if streamsMode {
		stoppableStream = initStreamsMode(strict, watching, enableStreamsAPI, confReader, strmAPITimeout, manager, logger, stats)
	} else {
		stoppableStream, dataStreamClosedChan = initNormalMode(strict, watching, confReader, manager, logger, stats)
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
		go func() {
			httpServer.Shutdown(context.Background())
			select {
			case <-httpServerClosedChan:
			case <-time.After(exitTimeout / 2):
				logger.Warnln("Service failed to close HTTP server gracefully in time.")
			}
		}()

		go func() {
			<-time.After(exitTimeout + time.Second)
			logger.Warnln(
				"Service failed to close cleanly within allocated time." +
					" Exiting forcefully and dumping stack trace to stderr.",
			)
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			os.Exit(1)
		}()

		if err := confReader.Close(context.Background()); err != nil {
			logger.Warnf("Failed to cleanly shut down file watcher: %v", err)
			os.Exit(1)
		}

		timesOut := time.Now().Add(exitTimeout)
		if err := stoppableStream.Stop(exitTimeout); err != nil {
			os.Exit(1)
		}
		manager.CloseAsync()
		if err := manager.WaitForClose(time.Until(timesOut)); err != nil {
			logger.Warnf(
				"Service failed to close cleanly within allocated time: %v."+
					" Exiting forcefully and dumping stack trace to stderr.\n", err,
			)
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			os.Exit(1)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for termination signal
	select {
	case <-sigChan:
		logger.Infoln("Received SIGTERM, the service is closing.")
	case <-dataStreamClosedChan:
		logger.Infoln("Pipeline has terminated. Shutting down the service.")
	case <-httpServerClosedChan:
		logger.Infoln("HTTP Server has terminated. Shutting down the service.")
	case <-optContext.Done():
		logger.Infoln("Run context was cancelled. Shutting down the service.")
	}
	return 0
}

//------------------------------------------------------------------------------
