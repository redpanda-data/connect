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
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
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
	"github.com/jeffail/util/path"
)

//--------------------------------------------------------------------------------------------------

// HTTPMetConf - HTTP endpoint config values for metrics exposure.
type HTTPMetConf struct {
	Enabled bool   `json:"enabled" yaml:"enabled"`
	Address string `json:"address" yaml:"address"`
	Path    string `json:"path" yaml:"path"`
}

// MetConf - Adds some custom fields to our metrics config.
type MetConf struct {
	Config metrics.Config `json:"config" yaml:"config"`
	HTTP   HTTPMetConf    `json:"http" yaml:"http"`
}

// Config - The benthos configuration struct.
type Config struct {
	Input   input.Config     `json:"input" yaml:"input"`
	Outputs []output.Config  `json:"outputs" yaml:"outputs"`
	Logger  log.LoggerConfig `json:"logger" yaml:"logger"`
	Metrics MetConf          `json:"metrics" yaml:"metrics"`
	Buffer  buffer.Config    `json:"buffer" yaml:"buffer"`
}

// NewConfig - Returns a new configuration with default values.
func NewConfig() Config {
	return Config{
		Input:   input.NewConfig(),
		Outputs: []output.Config{output.NewConfig()},
		Logger:  log.DefaultLoggerConfig(),
		Metrics: MetConf{
			Config: metrics.NewConfig(),
			HTTP: HTTPMetConf{
				Enabled: true,
				Address: "localhost:8040",
				Path:    "/stats",
			},
		},
		Buffer: buffer.NewConfig(),
	}
}

//--------------------------------------------------------------------------------------------------

var cpuProfile = flag.String("cpuprofile", "", "Write cpu profile to file")
var memProfile = flag.String("memprofile", "", "Write memory profile to file")

//--------------------------------------------------------------------------------------------------

func main() {
	config := NewConfig()

	// A list of default config paths to check for if not explicitly defined
	defaultPaths := []string{}

	/* If we manage to get the path of our executable then we want to try and find config files
	 * relative to that path, we always check from the parent folder since we assume benthos is
	 * stored within the bin folder.
	 */
	if executablePath, err := path.BinaryPath(); err == nil {
		defaultPaths = append(defaultPaths, filepath.Join(executablePath, "..", "config.yaml"))
		defaultPaths = append(defaultPaths, filepath.Join(executablePath, "..", "config", "benthos.yaml"))
		defaultPaths = append(defaultPaths, filepath.Join(executablePath, "..", "config.json"))
		defaultPaths = append(defaultPaths, filepath.Join(executablePath, "..", "config", "benthos.json"))
	}

	defaultPaths = append(defaultPaths, []string{
		filepath.Join(".", "benthos.yaml"),
		filepath.Join(".", "benthos.json"),
		"/etc/benthos.yaml",
		"/etc/benthos.json",
		"/etc/benthos/config.yaml",
		"/etc/benthos/config.json",
	}...)

	// Load configuration etc
	if !util.Bootstrap(&config, defaultPaths...) {
		return
	}

	// Logging and stats aggregation
	// Note: Only log to Stderr if one of our outputs is stdout
	var logger *log.Logger
	for _, outConf := range config.Outputs {
		if outConf.Type == "stdout" {
			logger = log.NewLogger(os.Stderr, config.Logger)
		} else {
			logger = log.NewLogger(os.Stdout, config.Logger)
		}
	}

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			logger.Errorf("Failed to create CPU profile file: %v\n", err)
			return
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			logger.Errorf("Failed to create MEM profile file: %v\n", err)
			return
		}
		go func() {
			<-time.After(60 * time.Second)
			pprof.WriteHeapProfile(f)
			f.Close()
		}()
	}

	// Create our metrics type
	stats, err := metrics.New(config.Metrics.Config)
	if err != nil {
		logger.Errorf("Metrics error: %v\n", err)
		return
	}
	defer stats.Close()

	pool := butil.NewClosablePool()

	// Create outputs
	outputs := []types.Output{}

	// For each configured output
	for _, outConf := range config.Outputs {
		if out, err := output.Construct(outConf); err == nil {
			outputs = append(outputs, out)
			pool.Add(10, out)
		} else {
			logger.Errorf("Output error: %v\n", err)
			return
		}
	}

	// Create input and input channel
	in, err := input.Construct(config.Input)
	if err != nil {
		logger.Errorf("Input error: %v\n", err)
		return
	}
	pool.Add(1, in)

	// Create a memory buffer
	buffer, err := buffer.Construct(config.Buffer, logger, stats)
	if err != nil {
		logger.Errorf("Buffer error: %v\n", err)
		return
	}
	butil.Couple(in, buffer)
	pool.Add(2, buffer)

	// Create broker - no need if there is only one output.
	if len(outputs) != 1 {
		msgBroker, err := broker.NewFanOut(outputs, stats)
		if err != nil {
			logger.Errorf("Output error: %v\n", err)
			return
		}
		butil.Couple(buffer, msgBroker)
		pool.Add(5, msgBroker)
	} else {
		butil.Couple(buffer, outputs[0])
	}

	// Defer ordered pool clean up.
	defer func() {
		if err := pool.Close(time.Second * 20); err != nil {
			panic(err)
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

	fmt.Fprintf(os.Stderr, "Launching a benthos instance, use CTRL+C to close.\n\n")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for termination signal
	select {
	case <-sigChan:
	}
}

//--------------------------------------------------------------------------------------------------
