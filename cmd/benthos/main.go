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
	_ "net/http/pprof"
	"runtime/pprof"

	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Jeffail/benthos/lib/api"
	"github.com/Jeffail/benthos/lib/buffer"
	"github.com/Jeffail/benthos/lib/input"
	"github.com/Jeffail/benthos/lib/output"
	"github.com/Jeffail/benthos/lib/processor"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util"
	"github.com/Jeffail/benthos/lib/util/service"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

// Config is the benthos configuration struct.
type Config struct {
	HTTP                 api.Config       `json:"http" yaml:"http"`
	Input                input.Config     `json:"input" yaml:"input"`
	Output               output.Config    `json:"output" yaml:"output"`
	Buffer               buffer.Config    `json:"buffer" yaml:"buffer"`
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
		Input:                input.NewConfig(),
		Output:               output.NewConfig(),
		Buffer:               buffer.NewConfig(),
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
		Output               interface{} `json:"output" yaml:"output"`
		Buffer               interface{} `json:"buffer" yaml:"buffer"`
		Logger               interface{} `json:"logger" yaml:"logger"`
		Metrics              interface{} `json:"metrics" yaml:"metrics"`
		SystemCloseTimeoutMS interface{} `json:"sys_exit_timeout_ms" yaml:"sys_exit_timeout_ms"`
	}{
		HTTP:                 c.HTTP,
		Input:                inConf,
		Output:               outConf,
		Buffer:               bufConf,
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
	if *printInputs || *printOutputs || *printBuffers || *printProcessors {
		if *printInputs {
			fmt.Println(input.Descriptions())
		}
		if *printProcessors {
			fmt.Println(processor.Descriptions())
		}
		if *printBuffers {
			fmt.Println(buffer.Descriptions())
		}
		if *printOutputs {
			fmt.Println(output.Descriptions())
		}
		os.Exit(1)
	}

	return config
}

// createPipeline creates a pipeline based on the supplied configuration file,
// and return a closable pool of pipeline objects, a channel indicating that all
// inputs and outputs have seized, or an error.
func createPipeline(
	config Config, mgr types.Manager, logger log.Modular, stats metrics.Type,
) (*util.ClosablePool, *util.ClosablePool, chan struct{}, error) {
	// Create two pools, this helps manage ordered closure of all pipeline
	// components. We have a tiered (t1) and an non-tiered (t2) pool. If the
	// tiered pool cannot close within our allotted time period then we try
	// closing the second non-tiered pool. If the second pool also fails then we
	// exit the service ungracefully.
	poolt1, poolt2 := util.NewClosablePool(), util.NewClosablePool()

	// Create our input pipe
	inputPipe, err := input.New(config.Input, mgr, logger, stats)
	if err != nil {
		logger.Errorf("Input error (%s): %v\n", config.Input.Type, err)
		return nil, nil, nil, err
	}
	poolt1.Add(1, inputPipe)
	poolt2.Add(0, inputPipe)

	// Create a buffer
	buf, err := buffer.New(config.Buffer, logger, stats)
	if err != nil {
		logger.Errorf("Buffer error (%s): %v\n", config.Buffer.Type, err)
		return nil, nil, nil, err
	}
	poolt1.Add(3, buf)
	poolt2.Add(0, buf)

	// Create our output pipe
	outputPipe, err := output.New(config.Output, mgr, logger, stats)
	if err != nil {
		logger.Errorf("Output error (%s): %v\n", config.Output.Type, err)
		return nil, nil, nil, err
	}
	poolt1.Add(10, outputPipe)
	poolt2.Add(0, outputPipe)

	util.Couple(buf, outputPipe)
	util.Couple(inputPipe, buf)
	closeChan := make(chan struct{})

	// If our outputs close down then we should shut down the service
	go func() {
		for {
			if err := outputPipe.WaitForClose(time.Second * 60); err == nil {
				closeChan <- struct{}{}
				return
			}
		}
	}()

	return poolt1, poolt2, closeChan, nil
}

func main() {
	// Bootstrap by reading cmd flags and configuration file
	config := bootstrap()

	// Logging and stats aggregation
	var logger log.Modular

	// Note: Only log to Stderr if one of our outputs is stdout
	if config.Output.Type == "stdout" {
		logger = log.NewLogger(os.Stderr, config.Logger)
	} else {
		logger = log.NewLogger(os.Stdout, config.Logger)
	}

	logger.Infoln("Launching a benthos instance, use CTRL+C to close.")

	// Create our metrics type
	stats, err := metrics.New(config.Metrics)
	if err != nil {
		logger.Errorf("Metrics error: %v\n", err)
		return
	}
	defer stats.Close()

	sanConf, err := config.Sanitised()
	if err != nil {
		logger.Warnf("Failed to generate sanitised config: %v\n", err)
	}
	httpServer := api.New(service.Version, service.DateBuilt, config.HTTP, sanConf, logger, stats)

	poolTiered, poolNonTiered, outputsClosedChan, err := createPipeline(config, httpServer, logger, stats)
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

		if config.Logger.LogLevel == "DEBUG" {
			go func() {
				<-time.After(tout)
				pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
				os.Exit(1)
			}()
		}

		if err := poolTiered.Close(tout / 2); err != nil {
			logger.Warnln(
				"Service failed to close using ordered tiers, you may receive a duplicate " +
					"message on the next service start.",
			)
			if err = poolNonTiered.Close(tout / 2); err != nil {
				logger.Warnln(
					"Service failed to close cleanly within allocated time. Exiting forcefully.",
				)
				os.Exit(1)
			}
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for termination signal
	select {
	case <-sigChan:
		logger.Infoln("Received SIGTERM, the service is closing.")
	case <-outputsClosedChan:
		logger.Infoln("Pipeline has terminated. Shutting down the service.")
	case <-httpServerClosedChan:
		logger.Infoln("HTTP Server has terminated. Shutting down the service.")
	}
}

//------------------------------------------------------------------------------
