package service

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/Jeffail/benthos/v3/lib/api"
	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/stream"
	strmmgr "github.com/Jeffail/benthos/v3/lib/stream/manager"
	"github.com/Jeffail/benthos/v3/lib/tracer"
	"github.com/Jeffail/benthos/v3/lib/types"
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

type stoppableStreams interface {
	Stop(timeout time.Duration) error
}

// ManagerInitFunc is a function to be called once the Benthos service manager,
// which manages resources shared across all components, is initialised. This is
// a useful time to add additional resources that might be required for custom
// plugins. If a non-nil error is returned the service will terminate.
type ManagerInitFunc func(manager types.Manager, logger log.Modular, stats metrics.Type) error

var onManagerInit ManagerInitFunc = func(manager types.Manager, logger log.Modular, stats metrics.Type) error {
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

func readConfig(path string, resourcesPaths []string) (lints []string) {
	// A list of default config paths to check for if not explicitly defined
	defaultPaths := []string{
		"/benthos.yaml",
		"/etc/benthos/config.yaml",
		"/etc/benthos.yaml",
	}

	if len(path) > 0 {
		var err error
		if lints, err = config.Read(path, true, &conf); err != nil {
			fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Iterate default config paths
		for _, path := range defaultPaths {
			if _, err := os.Stat(path); err == nil {
				fmt.Fprintf(os.Stderr, "Config file not specified, reading from %v\n", path)

				if lints, err = config.Read(path, true, &conf); err != nil {
					fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
					os.Exit(1)
				}
				break
			}
		}
	}

	for _, rPath := range resourcesPaths {
		resourceBytes, err := config.ReadWithJSONPointers(rPath, true)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Resource configuration file read error: %v\n", err)
			os.Exit(1)
		}
		extraMgrWrapper := struct {
			Manager manager.Config `yaml:"resources"`
		}{
			Manager: manager.NewConfig(),
		}
		if err = yaml.Unmarshal(resourceBytes, &extraMgrWrapper); err != nil {
			fmt.Fprintf(os.Stderr, "Resource configuration file read error: %v\n", err)
			os.Exit(1)
		}
		if err = conf.Manager.AddFrom(&extraMgrWrapper.Manager); err != nil {
			fmt.Fprintf(os.Stderr, "Resource configuration file read error: %v\n", err)
			os.Exit(1)
		}
	}

	return
}

//------------------------------------------------------------------------------

func cmdService(
	confPath string,
	resourcesPaths []string,
	overrideLogLevel string,
	strict bool,
	streamsMode bool,
	streamsConfigs []string,
) int {
	lints := readConfig(confPath, resourcesPaths)
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
		logger = log.New(os.Stderr, conf.Logger)
	} else {
		logger = log.New(os.Stdout, conf.Logger)
	}

	if len(lints) > 0 {
		lintlog := logger.NewModule(".linter")
		for _, lint := range lints {
			lintlog.Infoln(lint)
		}
	}

	// Create our metrics type.
	var err error
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
	sanConf, err := conf.Sanitised()
	if err != nil {
		logger.Warnf("Failed to generate sanitised config: %v\n", err)
	}
	var httpServer *api.Type
	if httpServer, err = api.New(Version, DateBuilt, conf.HTTP, sanConf, logger, stats); err != nil {
		logger.Errorf("Failed to initialise API: %v\n", err)
		return 1
	}

	// Create resource manager.
	manager, err := manager.New(conf.Manager, httpServer, logger, stats)
	if err != nil {
		logger.Errorf("Failed to create resource: %v\n", err)
		return 1
	}
	if err = onManagerInit(manager, logger, stats); err != nil {
		logger.Errorf("Failed to initialise manager: %v\n", err)
		return 1
	}

	var dataStream stoppableStreams
	dataStreamClosedChan := make(chan struct{})

	// Create data streams.
	if streamsMode {
		streamMgr := strmmgr.New(
			strmmgr.OptSetAPITimeout(time.Second*5),
			strmmgr.OptSetLogger(logger),
			strmmgr.OptSetManager(manager),
			strmmgr.OptSetStats(stats),
		)
		streamConfs := map[string]stream.Config{}
		var streamLints []string
		for _, path := range streamsConfigs {
			lints, err := strmmgr.LoadStreamConfigsFromPath(path, testSuffix, streamConfs)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Failed to load stream configs: %v\n", err)
				return 1
			}
			streamLints = append(streamLints, lints...)
		}

		if strict && len(streamLints) > 0 {
			for _, lint := range streamLints {
				fmt.Fprintln(os.Stderr, lint)
			}
			fmt.Println("Shutting down due to linter errors, to prevent shutdown run Benthos with --chilled")
			return 1
		} else if len(streamLints) > 0 {
			lintlog := logger.NewModule(".linter")
			for _, lint := range streamLints {
				lintlog.Infoln(lint)
			}
		}

		dataStream = streamMgr
		for id, conf := range streamConfs {
			if err = streamMgr.Create(id, conf); err != nil {
				logger.Errorf("Failed to create stream (%v): %v\n", id, err)
				return 1
			}
		}
		logger.Infoln("Launching benthos in streams mode, use CTRL+C to close.")
	} else {
		if dataStream, err = stream.New(
			conf.Config,
			stream.OptSetLogger(logger),
			stream.OptSetStats(stats),
			stream.OptSetManager(manager),
			stream.OptOnClose(func() {
				close(dataStreamClosedChan)
			}),
		); err != nil {
			logger.Errorf("Service closing due to: %v\n", err)
			return 1
		}
		logger.Infoln("Launching a benthos instance, use CTRL+C to close.")
	}

	// Start HTTP server.
	httpServerClosedChan := make(chan struct{})
	go func() {
		logger.Infof(
			"Listening for HTTP requests at: %v\n",
			"http://"+conf.HTTP.Address,
		)
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

		timesOut := time.Now().Add(exitTimeout)
		if err := dataStream.Stop(exitTimeout); err != nil {
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
	}
	return 0
}

//------------------------------------------------------------------------------
