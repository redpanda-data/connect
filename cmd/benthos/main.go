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
	"fmt"
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

// Extra flags
var (
	profileAddr = flag.String(
		"profile", "",
		"Provide an HTTP address to host CPU and MEM profiling.",
	)
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
)

//--------------------------------------------------------------------------------------------------

// bootstrap - Reads cmd args and either parses and config file or prints helper text and exits.
func bootstrap() Config {
	config := NewConfig()

	// A list of default config paths to check for if not explicitly defined
	defaultPaths := []string{}

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
	if !util.Bootstrap(&config, defaultPaths...) {
		os.Exit(0)
	}

	// If we only want to print our inputs or outputs we should exit afterwards
	if *printInputs || *printOutputs || *printBuffers {
		if *printInputs {
			fmt.Println(input.Descriptions())
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

/*
createPipeline - Based on the supplied configuration file, create a pipeline (input, buffer, output)
and return a closable pool of pipeline objects, a channel indicating that all inputs have seized, or
an error.
*/
func createPipeline(
	config Config, logger log.Modular, stats metrics.Aggregator,
) (*butil.ClosablePool, chan struct{}, error) {
	// Create a pool, this helps manage ordered closure of all pipeline components.
	pool := butil.NewClosablePool()

	// Create a buffer
	buf, err := buffer.Construct(config.Buffer, logger, stats)
	if err != nil {
		logger.Errorf("Buffer error (%s): %v\n", config.Buffer.Type, err)
		return nil, nil, err
	}
	pool.Add(3, buf)

	// Create our output pipe
	outputPipe, err := output.Construct(config.Output, logger, stats)
	if err != nil {
		logger.Errorf("Output error (%s): %v\n", config.Output.Type, err)
		return nil, nil, err
	}
	butil.Couple(buf, outputPipe)
	pool.Add(10, outputPipe)

	// Create our input pipe
	inputPipe, err := input.Construct(config.Input, logger, stats)
	if err != nil {
		logger.Errorf("Input error (%s): %v\n", config.Input.Type, err)
		return nil, nil, err
	}
	butil.Couple(inputPipe, buf)
	pool.Add(1, inputPipe)

	closeChan := make(chan struct{})

	// If our input closes down then we should shut down the service
	go func() {
		for {
			if err := inputPipe.WaitForClose(time.Second * 60); err == nil {
				closeChan <- struct{}{}
				return
			}
		}
	}()

	return pool, closeChan, nil
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

	pool, _, err := createPipeline(config, logger, stats)
	if err != nil {
		logger.Errorf("Service closing due to: %v\n", err)
		return
	}

	// Defer ordered pool clean up.
	defer func() {
		tout := time.Millisecond * time.Duration(config.SystemCloseTimeoutMS)
		if err := pool.Close(tout); err != nil {
			logger.Warnln(
				"Service failed to shut down cleanly within allocated time. Exiting forcefully.",
			)
			if config.Logger.LogLevel == "DEBUG" {
				pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			}
			os.Exit(1)
		}
	}()

	// We can host our own metrics HTTP endpoint, returns a json blob of latest metrics snapshot.
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

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for termination signal
	select {
	case <-sigChan:
		logger.Infoln("Received SIGTERM, the service is closing.")
	}
}

//--------------------------------------------------------------------------------------------------
