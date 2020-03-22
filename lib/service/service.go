package service

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
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
)

//------------------------------------------------------------------------------

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

func cmdService(
	config *config.Type,
	lints []string,
	streamsMode bool,
	streamsDir string,
) {
	if len(lints) > 0 {
		for _, lint := range lints {
			fmt.Fprintln(os.Stderr, lint)
		}
		fmt.Println("Shutting down due to linter errors, to prevent shutdown run Benthos with --chilled")
		os.Exit(1)
	}
	cmdServiceChilled(config, nil, streamsMode, streamsDir)
}

func cmdServiceChilled(
	config *config.Type,
	lints []string,
	streamsMode bool,
	streamsDir string,
) {
	// Logging and stats aggregation.
	var logger log.Modular
	// Note: Only log to Stderr if one of our outputs is stdout.
	if config.Output.Type == "stdout" {
		logger = log.New(os.Stderr, config.Logger)
	} else {
		logger = log.New(os.Stdout, config.Logger)
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
	stats, err = metrics.New(config.Metrics, metrics.OptSetLogger(logger))
	for err != nil {
		logger.Errorf("Failed to connect to metrics aggregator: %v\n", err)
		<-time.After(time.Second)
		stats, err = metrics.New(config.Metrics, metrics.OptSetLogger(logger))
	}
	defer func() {
		if sCloseErr := stats.Close(); sCloseErr != nil {
			logger.Errorf("Failed to cleanly close metrics aggregator: %v\n", sCloseErr)
		}
	}()

	// Create our tracer type.
	var trac tracer.Type
	if trac, err = tracer.New(config.Tracer); err != nil {
		logger.Errorf("Failed to initialise tracer: %v\n", err)
		os.Exit(1)
	}
	defer trac.Close()

	// Create HTTP API with a sanitised service config.
	sanConf, err := config.Sanitised()
	if err != nil {
		logger.Warnf("Failed to generate sanitised config: %v\n", err)
	}
	var httpServer *api.Type
	if httpServer, err = api.New(Version, DateBuilt, config.HTTP, sanConf, logger, stats); err != nil {
		logger.Errorf("Failed to initialise API: %v\n", err)
		os.Exit(1)
	}

	// Create resource manager.
	manager, err := manager.New(config.Manager, httpServer, logger, stats)
	if err != nil {
		logger.Errorf("Failed to create resource: %v\n", err)
		os.Exit(1)
	}
	if err = onManagerInit(manager, logger, stats); err != nil {
		logger.Errorf("Failed to initialise manager: %v\n", err)
		os.Exit(1)
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
		var streamConfs map[string]stream.Config
		if len(streamsDir) > 0 {
			if streamConfs, err = strmmgr.LoadStreamConfigsFromDirectory(true, streamsDir); err != nil {
				logger.Errorf("Failed to load stream configs: %v\n", err)
				os.Exit(1)
			}
		}
		dataStream = streamMgr
		for id, conf := range streamConfs {
			if err = streamMgr.Create(id, conf); err != nil {
				logger.Errorf("Failed to create stream (%v): %v\n", id, err)
				os.Exit(1)
			}
		}
		logger.Infoln("Launching benthos in streams mode, use CTRL+C to close.")
		if lStreams := len(streamConfs); lStreams > 0 {
			logger.Infof("Created %v streams from directory: %v\n", lStreams, streamsDir)
		}
	} else {
		if dataStream, err = stream.New(
			config.Config,
			stream.OptSetLogger(logger),
			stream.OptSetStats(stats),
			stream.OptSetManager(manager),
			stream.OptOnClose(func() {
				close(dataStreamClosedChan)
			}),
		); err != nil {
			logger.Errorf("Service closing due to: %v\n", err)
			os.Exit(1)
		}
		logger.Infoln("Launching a benthos instance, use CTRL+C to close.")
	}

	// Start HTTP server.
	httpServerClosedChan := make(chan struct{})
	go func() {
		logger.Infof(
			"Listening for HTTP requests at: %v\n",
			"http://"+config.HTTP.Address,
		)
		httpErr := httpServer.ListenAndServe()
		if httpErr != nil && httpErr != http.ErrServerClosed {
			logger.Errorf("HTTP Server error: %v\n", httpErr)
		}
		close(httpServerClosedChan)
	}()

	var exitTimeout time.Duration
	if tout := config.SystemCloseTimeout; len(tout) > 0 {
		var err error
		if exitTimeout, err = time.ParseDuration(tout); err != nil {
			logger.Errorf("Failed to parse shutdown timeout period string: %v\n", err)
			os.Exit(1)
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
	os.Exit(0)
}

//------------------------------------------------------------------------------
