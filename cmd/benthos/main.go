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

	"github.com/jeffail/benthos/broker"
	"github.com/jeffail/benthos/buffer"
	"github.com/jeffail/benthos/input"
	"github.com/jeffail/benthos/output"
	"github.com/jeffail/benthos/types"
	butil "github.com/jeffail/benthos/util"
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
	Inputs               []input.Config   `json:"inputs" yaml:"inputs"`
	Outputs              []output.Config  `json:"outputs" yaml:"outputs"`
	Buffer               buffer.Config    `json:"buffer" yaml:"buffer"`
	Logger               log.LoggerConfig `json:"logger" yaml:"logger"`
	Metrics              MetConfig        `json:"metrics" yaml:"metrics"`
	SystemCloseTimeoutMS int              `json:"sys_exit_timeout_ms" yaml:"sys_exit_timeout_ms"`
}

// NewConfig - Returns a new configuration with default values.
func NewConfig() Config {
	return Config{
		Inputs:  []input.Config{input.NewConfig()},
		Outputs: []output.Config{output.NewConfig()},
		Buffer:  buffer.NewConfig(),
		Logger:  log.NewLoggerConfig(),
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
	haveStdout := false
	for _, outConf := range config.Outputs {
		if outConf.Type == "stdout" {
			haveStdout = true
		}
	}
	if haveStdout {
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

	// Create pipeline
	inputs := []types.Input{}
	outputs := []types.Output{}

	// Create a buffer
	buf, err := buffer.Construct(config.Buffer, logger, stats)
	if err != nil {
		logger.Errorf("Buffer error: %v\n", err)
		return
	}
	pool.Add(3, buf)

	// For each configured output
	for _, outConf := range config.Outputs {
		if out, err := output.Construct(outConf, logger, stats); err == nil {
			outputs = append(outputs, out)
			pool.Add(10, out)
		} else {
			logger.Errorf("Output error (%s): %v\n", outConf.Type, err)
			return
		}
	}

	// For each configured input
	for _, inConf := range config.Inputs {
		if in, err := input.Construct(inConf, logger, stats); err == nil {
			inputs = append(inputs, in)
			pool.Add(1, in)
		} else {
			logger.Errorf("Input error (%s): %v\n", inConf.Type, err)
			return
		}
	}

	// Create fan-out broker for outputs if there is more than one.
	if len(outputs) != 1 {
		msgBroker, err := broker.NewFanOut(outputs, stats)
		if err != nil {
			logger.Errorf("Output error: %v\n", err)
			return
		}
		butil.Couple(buf, msgBroker)
		pool.Add(5, msgBroker)
	} else {
		butil.Couple(buf, outputs[0])
	}

	// Create fan-in broker for inputs if there is more than one.
	if len(inputs) != 1 {
		msgBroker, err := broker.NewFanIn(inputs, stats)
		if err != nil {
			logger.Errorf("Broker error: %v\n", err)
			return
		}
		butil.Couple(msgBroker, buf)
		pool.Add(2, msgBroker)
	} else {
		butil.Couple(inputs[0], buf)
	}

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

			logger.Infof("Serving HTTP metrics at: %s\n", config.Metrics.HTTP.Address+config.Metrics.HTTP.Path)
			if err := http.ListenAndServe(config.Metrics.HTTP.Address, mux); err != nil {
				logger.Errorf("Metrics HTTP server failed: %v\n", err)
			}
		}()
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for termination signal
	select {
	case <-sigChan:
	}
}

//--------------------------------------------------------------------------------------------------
