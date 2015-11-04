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
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/jeffail/benthos/agent"
	"github.com/jeffail/benthos/broker"
	"github.com/jeffail/benthos/input"
	"github.com/jeffail/benthos/output"
	"github.com/jeffail/util"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
	"github.com/jeffail/util/path"
)

//--------------------------------------------------------------------------------------------------

// Config - The benthos configuration struct.
type Config struct {
	Input   input.Config     `json:"input" yaml:"input"`
	Outputs []output.Config  `json:"outputs" yaml:"outputs"`
	Logger  log.LoggerConfig `json:"logger" yaml:"logger"`
	Metrics metrics.Config   `json:"metrics" yaml:"metrics"`
}

// NewConfig - Returns a new configuration with default values.
func NewConfig() Config {
	return Config{
		Input:   input.NewConfig(),
		Outputs: []output.Config{output.NewConfig()},
		Logger:  log.DefaultLoggerConfig(),
		Metrics: metrics.NewConfig(),
	}
}

//--------------------------------------------------------------------------------------------------

func main() {
	var (
		err       error
		closeChan = make(chan struct{})
	)

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

	// Create our metrics type.
	stats, err := metrics.New(config.Metrics)
	if err != nil {
		logger.Errorf("Metrics error: %v\n", err)
		return
	}
	defer stats.Close()

	// Create output agents.
	agents := []agent.Type{}

	// For each configured output
	for _, outConf := range config.Outputs {
		if out, err := output.Construct(outConf); err == nil {
			agents = append(agents, agent.NewUnbuffered(out))
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

	// Error propagator
	errProp := broker.NewErrPropagator(agents)

	// Create broker
	msgBroker := broker.NewOneToMany(agents)
	msgBroker.SetMessageChan(in.MessageChan())

	in.SetResponseChan(msgBroker.ResponseChan())

	// Error reader
	go func() {
		for errs := range errProp.OutputChan() {
			logger.Errorf("Agent errors: %v\n", errs)
		}
	}()

	// Defer clean broker, input and output closure
	defer func() {
		in.CloseAsync()
		if err := in.WaitForClose(time.Second * 5); err != nil {
			panic(err)
		}

		msgBroker.CloseAsync()
		if err := msgBroker.WaitForClose(time.Second * 5); err != nil {
			panic(err)
		}

		errProp.CloseAsync()
		errProp.WaitForClose(time.Second)

		for _, a := range agents {
			a.CloseAsync()
		}
		for _, a := range agents {
			if err := a.WaitForClose(time.Second * 5); err != nil {
				panic(err)
			}
		}
	}()

	fmt.Fprintf(os.Stderr, "Launching a benthos instance, use CTRL+C to close.\n\n")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for termination signal
	select {
	case <-sigChan:
	case <-closeChan:
	}
}

//--------------------------------------------------------------------------------------------------
