/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package main

import (
	_ "net/http/pprof"
	"runtime/pprof"

	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jeffail/benthos/lib/buffer"
	"github.com/jeffail/benthos/lib/input"
	"github.com/jeffail/benthos/lib/output"
	butil "github.com/jeffail/benthos/lib/util"
	"github.com/jeffail/util"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

// HTTPMetConfig - HTTP endpoint config values for metrics exposure.
type HTTPMetConfig struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Address string `json:"address" yaml:"address"`
	Path    string `json:"path" yaml:"path"`
}

// MetConfig - Adds some custom fields to our metrics config.
type MetConfig struct {
	Config metrics.Config `json:"config" yaml:"config"`
	HTTP   HTTPMetConfig  `json:"http" yaml:"http"`
}

// Config - The benthos configuration struct.
type Config struct {
	Input                input.Config     `json:"input" yaml:"input"`
	Output               output.Config    `json:"output" yaml:"output"`
	Buffer               buffer.Config    `json:"buffer" yaml:"buffer"`
	Logger               log.LoggerConfig `json:"logger" yaml:"logger"`
	Metrics              MetConfig        `json:"metrics" yaml:"metrics"`
	SystemCloseTimeoutMS int              `json:"sys_exit_timeout_ms" yaml:"sys_exit_timeout_ms"`
}

// NewConfig - Returns a new configuration with default values.
func NewConfig() Config {
	return Config{
		Input:  input.NewConfig(),
		Output: output.NewConfig(),
		Buffer: buffer.NewConfig(),
		Logger: log.NewLoggerConfig(),
		Metrics: MetConfig{
			Config: metrics.NewConfig(),
			HTTP: HTTPMetConfig{
				Enabled: true,
				Address: "localhost:8040",
				Path:    "/stats",
			},
		},
		SystemCloseTimeoutMS: 20000,
	}
}

//--------------------------------------------------------------------------------------------------

var profileAddr = flag.String(
	"profile", "",
	"Provide an HTTP address to host CPU and MEM profiling.",
)

//--------------------------------------------------------------------------------------------------

func main() {
	config := NewConfig()

	// A list of default config paths to check for if not explicitly defined
	defaultPaths := []string{}

	// Load configuration etc
	if !util.Bootstrap(&config, defaultPaths...) {
		return
	}

	// Logging and stats aggregation
	var logger log.Modular

	// Note: Only log to Stderr if one of our outputs is stdout
	if config.Output.Type == "stdout" {
		logger = log.NewLogger(os.Stderr, config.Logger)
	} else {
		logger = log.NewLogger(os.Stdout, config.Logger)
	}

	logger.Infoln("Launching a benthos instance, use CTRL+C to close.")

	// If profiling is enabled.
	if *profileAddr != "" {
		go func() {
			exampleAddr := *profileAddr
			if (*profileAddr)[0] == ':' {
				exampleAddr = "localhost" + exampleAddr
			}
			logger.Infof("Serving profiling at: %s\n", *profileAddr)
			logger.Infof("To use the profiling tool: "+
				"`go tool pprof http://%s/debug/pprof/(heap|profile|block|etc)`\n", exampleAddr)
			if err := http.ListenAndServe(*profileAddr, http.DefaultServeMux); err != nil {
				logger.Errorf("Failed to spawn HTTP server for profiling: %v\n", err)
			}
		}()
	}

	// Create our metrics type
	stats, err := metrics.New(config.Metrics.Config)
	if err != nil {
		logger.Errorf("Metrics error: %v\n", err)
		return
	}
	defer stats.Close()

	// Create a pool, this helps manage ordered closure of all pipeline components.
	pool := butil.NewClosablePool()

	// Create a buffer
	buf, err := buffer.Construct(config.Buffer, logger, stats)
	if err != nil {
		logger.Errorf("Buffer error: %v\n", err)
		return
	}
	pool.Add(3, buf)

	// Create our output pipe
	outputPipe, err := output.Construct(config.Output, logger, stats)
	if err != nil {
		logger.Errorf("Output error (%s): %v\n", config.Output.Type, err)
		return
	}
	butil.Couple(buf, outputPipe)
	pool.Add(10, outputPipe)

	// Create our input pipe
	inputPipe, err := input.Construct(config.Input, logger, stats)
	if err != nil {
		logger.Errorf("Input error (%s): %v\n", config.Input.Type, err)
		return
	}
	butil.Couple(inputPipe, buf)
	pool.Add(1, inputPipe)

	// Defer ordered pool clean up.
	defer func() {
		tout := time.Millisecond * time.Duration(config.SystemCloseTimeoutMS)
		if err := pool.Close(tout); err != nil {
			pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			os.Exit(1)
		}
	}()

	if config.Metrics.HTTP.Enabled {
		go func() {
			mux := http.NewServeMux()
			mux.HandleFunc(config.Metrics.HTTP.Path, stats.JSONHandler())

			logger.Infof(
				"Serving HTTP metrics at: %s\n",
				config.Metrics.HTTP.Address+config.Metrics.HTTP.Path,
			)
			if err := http.ListenAndServe(config.Metrics.HTTP.Address, mux); err != nil {
				logger.Errorf("Metrics HTTP server failed: %v\n", err)
			}
		}()
	}

	sigChan, closeChan := make(chan os.Signal, 1), make(chan struct{})
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// If our input closes down then we should shut down the service
	go func() {
		for {
			if err := inputPipe.WaitForClose(time.Second * 60); err == nil {
				closeChan <- struct{}{}
				return
			}
		}
	}()

	// Wait for termination signal
	select {
	case <-sigChan:
	case <-closeChan:
		logger.Infoln("All inputs have shut down, the service is closing.")
	}
}

//--------------------------------------------------------------------------------------------------
