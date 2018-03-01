// Copyright (c) 2018 Ashley Jeffs
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
	_ "net/http/pprof"
	"runtime/pprof"

	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Jeffail/benthos/lib/api"
	"github.com/Jeffail/benthos/lib/input"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util"
	"github.com/Jeffail/benthos/lib/util/service"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	"github.com/Jeffail/benthos/lib/util/test"
)

//------------------------------------------------------------------------------

// Config is the benthos configuration struct.
type Config struct {
	ReportPeriodMS       int              `json:"report_period_ms" yaml:"report_period_ms"`
	HTTP                 api.Config       `json:"http" yaml:"http"`
	Input                input.Config     `json:"input" yaml:"input"`
	Logger               log.LoggerConfig `json:"logger" yaml:"logger"`
	Metrics              metrics.Config   `json:"metrics" yaml:"metrics"`
	SystemCloseTimeoutMS int              `json:"sys_exit_timeout_ms" yaml:"sys_exit_timeout_ms"`
}

// NewConfig returns a new configuration with default values.
func NewConfig() Config {
	metricsConf := metrics.NewConfig()
	metricsConf.Prefix = "benthos"

	return Config{
		ReportPeriodMS:       60000,
		HTTP:                 api.NewConfig(),
		Input:                input.NewConfig(),
		Logger:               log.NewLoggerConfig(),
		Metrics:              metricsConf,
		SystemCloseTimeoutMS: 20000,
	}
}

//------------------------------------------------------------------------------

// bootstrap reads cmd args and either parses and config file or prints helper
// text and exits.
func bootstrap() Config {
	config := NewConfig()

	// Load configuration etc
	if !service.Bootstrap(&config) {
		os.Exit(0)
	}

	return config
}

// createPipeline creates a pipeline based on the supplied configuration file,
// and return a closable pool of pipeline objects, a channel indicating that all
// inputs and outputs have seized, or an error.
func createPipeline(
	config Config, mgr types.Manager, logger log.Modular, stats metrics.Type,
) (*util.ClosablePool, error) {
	pool := util.NewClosablePool()

	// Create our input pipe
	inputPipe, err := input.New(config.Input, mgr, logger, stats)
	if err != nil {
		logger.Errorf("Input error (%s): %v\n", config.Input.Type, err)
		return nil, err
	}
	pool.Add(1, inputPipe)

	// Create our benchmarking output pipe
	outputPipe := test.NewBenchOutput(
		time.Duration(config.ReportPeriodMS)*time.Millisecond, logger, stats,
	)
	pool.Add(10, outputPipe)

	outputPipe.StartReceiving(inputPipe.TransactionChan())
	return pool, nil
}

func main() {
	// Bootstrap by reading cmd flags and configuration file
	config := bootstrap()

	// Logging and stats aggregation
	logger := log.NewLogger(os.Stderr, config.Logger)

	logger.Infoln("Launching a benthos_bench instance, use CTRL+C to close.")

	// Create our metrics type
	stats, err := metrics.New(config.Metrics)
	if err != nil {
		logger.Errorf("Metrics error: %v\n", err)
		return
	}
	defer stats.Close()

	httpServer := api.New(service.Version, service.DateBuilt, config.HTTP, config, logger, stats)

	pool, err := createPipeline(config, httpServer, logger, stats)
	if err != nil {
		logger.Errorf("Service closing due to: %v\n", err)
		return
	}

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

	// Defer ordered pool clean up.
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

		if err := pool.Close(tout); err != nil {
			logger.Warnln(
				"Service failed to close using ordered tiers, you may receive a duplicate " +
					"message on the next service start.",
			)
			if config.Logger.LogLevel == "DEBUG" {
				pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			}
			os.Exit(1)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for termination signal
	select {
	case <-sigChan:
		logger.Infoln("Received SIGTERM, the service is closing.")
	}
}

//------------------------------------------------------------------------------
