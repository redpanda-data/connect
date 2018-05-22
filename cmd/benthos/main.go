// Copyright (c) 2014 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package main

import (
	"context"
	"fmt"
	"runtime/pprof"

	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Jeffail/benthos/lib/api"
	"github.com/Jeffail/benthos/lib/buffer"
	"github.com/Jeffail/benthos/lib/cache"
	"github.com/Jeffail/benthos/lib/input"
	"github.com/Jeffail/benthos/lib/manager"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/output"
	"github.com/Jeffail/benthos/lib/pipeline"
	"github.com/Jeffail/benthos/lib/processor"
	"github.com/Jeffail/benthos/lib/processor/condition"
	"github.com/Jeffail/benthos/lib/stream"
	strmmgr "github.com/Jeffail/benthos/lib/stream/manager"
	"github.com/Jeffail/benthos/lib/util/service"
	"github.com/Jeffail/benthos/lib/util/service/log"
)

//------------------------------------------------------------------------------

// Config is the benthos configuration struct.
type Config struct {
	HTTP                 api.Config `json:"http" yaml:"http"`
	stream.Config        `json:",inline" yaml:",inline"`
	Manager              manager.Config   `json:"resources" yaml:"resources"`
	Logger               log.LoggerConfig `json:"logger" yaml:"logger"`
	Metrics              metrics.Config   `json:"metrics" yaml:"metrics"`
	SystemCloseTimeoutMS int              `json:"sys_exit_timeout_ms" yaml:"sys_exit_timeout_ms"`
}

// NewConfig returns a new configuration with default values.
func NewConfig() Config {
	metricsConf := metrics.NewConfig()
	metricsConf.Prefix = "benthos"

	return Config{
		HTTP:                 api.NewConfig(),
		Config:               stream.NewConfig(),
		Manager:              manager.NewConfig(),
		Logger:               log.NewLoggerConfig(),
		Metrics:              metricsConf,
		SystemCloseTimeoutMS: 20000,
	}
}

// Sanitised returns a sanitised copy of the Benthos configuration, meaning
// fields of no consequence (unused inputs, outputs, processors etc) are
// excluded.
func (c Config) Sanitised() (interface{}, error) {
	inConf, err := input.SanitiseConfig(c.Input)
	if err != nil {
		return nil, err
	}

	var pipeConf interface{}
	pipeConf, err = pipeline.SanitiseConfig(c.Pipeline)
	if err != nil {
		return nil, err
	}

	var outConf interface{}
	outConf, err = output.SanitiseConfig(c.Output)
	if err != nil {
		return nil, err
	}

	var bufConf interface{}
	bufConf, err = buffer.SanitiseConfig(c.Buffer)
	if err != nil {
		return nil, err
	}

	var metConf interface{}
	metConf, err = metrics.SanitiseConfig(c.Metrics)
	if err != nil {
		return nil, err
	}

	return struct {
		HTTP                 interface{} `json:"http" yaml:"http"`
		Input                interface{} `json:"input" yaml:"input"`
		Buffer               interface{} `json:"buffer" yaml:"buffer"`
		Pipeline             interface{} `json:"pipeline" yaml:"pipeline"`
		Output               interface{} `json:"output" yaml:"output"`
		Manager              interface{} `json:"resources" yaml:"resources"`
		Logger               interface{} `json:"logger" yaml:"logger"`
		Metrics              interface{} `json:"metrics" yaml:"metrics"`
		SystemCloseTimeoutMS interface{} `json:"sys_exit_timeout_ms" yaml:"sys_exit_timeout_ms"`
	}{
		HTTP:                 c.HTTP,
		Input:                inConf,
		Buffer:               bufConf,
		Pipeline:             pipeConf,
		Output:               outConf,
		Manager:              c.Manager,
		Logger:               c.Logger,
		Metrics:              metConf,
		SystemCloseTimeoutMS: c.SystemCloseTimeoutMS,
	}, nil
}

//------------------------------------------------------------------------------

// Extra flags
var (
	printInputs = flag.Bool(
		"list-inputs", false,
		"Print a list of available input options, then exit",
	)
	printOutputs = flag.Bool(
		"list-outputs", false,
		"Print a list of available output options, then exit",
	)
	printBuffers = flag.Bool(
		"list-buffers", false,
		"Print a list of available buffer options, then exit",
	)
	printProcessors = flag.Bool(
		"list-processors", false,
		"Print a list of available processor options, then exit",
	)
	printConditions = flag.Bool(
		"list-conditions", false,
		"Print a list of available processor condition options, then exit",
	)
	printCaches = flag.Bool(
		"list-caches", false,
		"Print a list of available cache options, then exit",
	)
	streamsMode = flag.Bool(
		"streams", false,
		"Run Benthos in streams mode, where streams can be created, updated"+
			" and removed via REST HTTP endpoints. In streams mode the stream"+
			" fields of a config file (input, buffer, pipeline, output) will"+
			" be ignored",
	)
)

//------------------------------------------------------------------------------

// bootstrap reads cmd args and either parses and config file or prints helper
// text and exits.
func bootstrap() Config {
	config := NewConfig()

	// A list of default config paths to check for if not explicitly defined
	defaultPaths := []string{
		"/benthos.yaml",
		"/etc/benthos/config.yaml",
		"/etc/benthos.yaml",
	}

	// Override default help printing
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Usage: benthos [flags...]")
		fmt.Fprintln(os.Stderr, "Flags:")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr,
			"\nFor example configs use --print-yaml or --print-json\n"+
				"For a list of available inputs or outputs use --list-inputs or --list-outputs\n"+
				"For a list of available buffer options use --list-buffers\n")
	}

	// Load configuration etc
	if !service.Bootstrap(&config, defaultPaths...) {
		os.Exit(0)
	}

	// If we only want to print our inputs or outputs we should exit afterwards
	if *printInputs || *printOutputs || *printBuffers || *printProcessors || *printConditions || *printCaches {
		if *printInputs {
			fmt.Println(input.Descriptions())
		}
		if *printProcessors {
			fmt.Println(processor.Descriptions())
		}
		if *printConditions {
			fmt.Println(condition.Descriptions())
		}
		if *printBuffers {
			fmt.Println(buffer.Descriptions())
		}
		if *printOutputs {
			fmt.Println(output.Descriptions())
		}
		if *printCaches {
			fmt.Println(cache.Descriptions())
		}
		os.Exit(1)
	}

	return config
}

type stoppableStreams interface {
	Stop(timeout time.Duration) error
}

func main() {
	// Bootstrap by reading cmd flags and configuration file.
	config := bootstrap()

	// Logging and stats aggregation.
	var logger log.Modular

	// Note: Only log to Stderr if one of our outputs is stdout.
	if config.Output.Type == "stdout" {
		logger = log.NewLogger(os.Stderr, config.Logger)
	} else {
		logger = log.NewLogger(os.Stdout, config.Logger)
	}

	logger.Infoln("Launching a benthos instance, use CTRL+C to close.")

	// Create our metrics type.
	stats, err := metrics.New(config.Metrics)
	if err != nil {
		logger.Errorf("Metrics error: %v\n", err)
		os.Exit(1)
	}
	defer stats.Close()

	// Create HTTP API with a sanitised service config.
	sanConf, err := config.Sanitised()
	if err != nil {
		logger.Warnf("Failed to generate sanitised config: %v\n", err)
	}
	httpServer := api.New(service.Version, service.DateBuilt, config.HTTP, sanConf, logger, stats)

	// Create resource manager.
	manager, err := manager.New(config.Manager, httpServer, logger, stats)
	if err != nil {
		logger.Errorf("Failed to create resource: %v\n", err)
		os.Exit(1)
	}

	var dataStream stoppableStreams
	dataStreamClosedChan := make(chan struct{})

	// Create data streams.
	if *streamsMode {
		dataStream = strmmgr.New(
			strmmgr.OptSetAPITimeout(time.Duration(config.HTTP.ReadTimeoutMS)*time.Millisecond),
			strmmgr.OptSetLogger(logger),
			strmmgr.OptSetManager(manager),
			strmmgr.OptSetStats(stats),
		)
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

	// Defer clean up.
	defer func() {
		tout := time.Millisecond * time.Duration(config.SystemCloseTimeoutMS)

		go func() {
			httpServer.Shutdown(context.Background())
			select {
			case <-httpServerClosedChan:
			case <-time.After(tout / 2):
				logger.Warnln("Service failed to close HTTP server gracefully in time.")
			}
		}()

		go func() {
			<-time.After(tout + time.Second)
			logger.Warnln(
				"Service failed to close cleanly within allocated time." +
					" Exiting forcefully and dumping stack trace to stderr.",
			)
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			os.Exit(1)
		}()

		if err := dataStream.Stop(tout); err != nil {
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
}

//------------------------------------------------------------------------------
