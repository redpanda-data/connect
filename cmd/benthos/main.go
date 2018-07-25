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
	"encoding/json"
	"fmt"
	"runtime/pprof"
	"strings"

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
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/manager"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/output"
	"github.com/Jeffail/benthos/lib/pipeline"
	"github.com/Jeffail/benthos/lib/processor"
	"github.com/Jeffail/benthos/lib/processor/condition"
	"github.com/Jeffail/benthos/lib/stream"
	strmmgr "github.com/Jeffail/benthos/lib/stream/manager"
	"github.com/Jeffail/benthos/lib/util/config"
	yaml "gopkg.in/yaml.v2"
)

//------------------------------------------------------------------------------

// Build stamps.
var (
	Version   string
	DateBuilt string
)

//------------------------------------------------------------------------------

// Config is the benthos configuration struct.
type Config struct {
	HTTP                 api.Config `json:"http" yaml:"http"`
	stream.Config        `json:",inline" yaml:",inline"`
	Manager              manager.Config `json:"resources" yaml:"resources"`
	Logger               log.Config     `json:"logger" yaml:"logger"`
	Metrics              metrics.Config `json:"metrics" yaml:"metrics"`
	SystemCloseTimeoutMS int            `json:"sys_exit_timeout_ms" yaml:"sys_exit_timeout_ms"`
}

// NewConfig returns a new configuration with default values.
func NewConfig() Config {
	metricsConf := metrics.NewConfig()
	metricsConf.Prefix = "benthos"

	return Config{
		HTTP:                 api.NewConfig(),
		Config:               stream.NewConfig(),
		Manager:              manager.NewConfig(),
		Logger:               log.NewConfig(),
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
	showVersion = flag.Bool(
		"version", false, "Display version info, then exit",
	)
	showConfigJSON = flag.Bool(
		"print-json", false, "Print loaded configuration as JSON, then exit",
	)
	showConfigYAML = flag.Bool(
		"print-yaml", false, "Print loaded configuration as YAML, then exit",
	)
	showAll = flag.Bool(
		"all", false,
		"Set whether _all_ fields should be shown when printing configuration"+
			" via --print-yaml or --print-json, otherwise only used values"+
			" will be printed.",
	)
	configPath = flag.String(
		"c", "", "Path to a configuration file",
	)
	swapEnvs = flag.Bool(
		"swap-envs", true,
		"Swap ${FOO} patterns in config file with environment variables",
	)
	examples = flag.String(
		"examples", "",
		"Add specific examples when printing a configuration file with"+
			" `--print-yaml` or `--print-json` by listing comma separated"+
			" types. Types can be any input, buffer, processor or output. For"+
			" example: `benthos --print-yaml --examples websocket,jmespath`"+
			" would print a config with a websocket input and output and a"+
			" jmespath processor.",
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
			" be ignored. Instead, any .yaml or .json files inside the"+
			" --streams-dir directory will be parsed as stream configs.",
	)
	streamsDir = flag.String(
		"streams-dir", "/benthos/streams",
		"When running Benthos in streams mode any files in this directory with"+
			" a .json or .yaml extension will be parsed as a stream"+
			" configuration (input, buffer, pipeline, output), where the"+
			" filename less the extension will be the id of the stream.",
	)
)

//------------------------------------------------------------------------------

func addExamples(examples string, conf *Config) {
	var inputType, bufferType, conditionType, outputType string
	var processorTypes []string
	for _, e := range strings.Split(examples, ",") {
		if _, exists := input.Constructors[e]; exists && len(inputType) == 0 {
			inputType = e
		}
		if _, exists := buffer.Constructors[e]; exists {
			bufferType = e
		}
		if _, exists := processor.Constructors[e]; exists {
			processorTypes = append(processorTypes, e)
		}
		if _, exists := condition.Constructors[e]; exists {
			conditionType = e
		}
		if _, exists := output.Constructors[e]; exists {
			outputType = e
		}
	}
	if len(inputType) > 0 {
		conf.Input.Type = inputType
	}
	if len(bufferType) > 0 {
		conf.Buffer.Type = bufferType
	}
	if len(processorTypes) > 0 {
		for _, procType := range processorTypes {
			procConf := processor.NewConfig()
			procConf.Type = procType
			conf.Pipeline.Processors = append(conf.Pipeline.Processors, procConf)
		}
	}
	if len(conditionType) > 0 {
		condConf := condition.NewConfig()
		condConf.Type = conditionType
		procConf := processor.NewConfig()
		procConf.Type = "filter"
		procConf.Filter.Config = condConf
		conf.Pipeline.Processors = append(conf.Pipeline.Processors, procConf)
	}
	if len(outputType) > 0 {
		conf.Output.Type = outputType
	}
}

// bootstrap reads cmd args and either parses and config file or prints helper
// text and exits.
func bootstrap() Config {
	conf := NewConfig()

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

	flag.Parse()

	// If the user wants the version we print it.
	if *showVersion {
		fmt.Printf("Version: %v\nDate: %v\n", Version, DateBuilt)
		os.Exit(0)
	}

	if len(*configPath) > 0 {
		if err := config.Read(*configPath, *swapEnvs, &conf); err != nil {
			fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
			os.Exit(1)
		}
	} else {
		// Iterate default config paths
		for _, path := range defaultPaths {
			if _, err := os.Stat(path); err == nil {
				fmt.Fprintf(os.Stderr, "Config file not specified, reading from %v\n", path)

				if err = config.Read(path, *swapEnvs, &conf); err != nil {
					fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
					os.Exit(1)
				}
				break
			}
		}
	}

	// If the user wants the configuration to be printed we do so and then exit.
	if *showConfigJSON || *showConfigYAML {
		var outConf interface{}
		var err error

		if len(*examples) > 0 {
			addExamples(*examples, &conf)
		}

		if !*showAll {
			if outConf, err = conf.Sanitised(); err != nil {
				fmt.Fprintln(os.Stderr, fmt.Sprintf("Configuration sanitise error: %v", err))
				os.Exit(1)
			}
		} else {
			if len(conf.Input.Processors) == 0 &&
				len(conf.Pipeline.Processors) == 0 &&
				len(conf.Output.Processors) == 0 {
				conf.Pipeline.Processors = append(conf.Pipeline.Processors, processor.NewConfig())
			}
			manager.AddExamples(&conf.Manager)
			outConf = conf
		}

		if *showConfigJSON {
			if configJSON, err := json.Marshal(outConf); err == nil {
				fmt.Println(string(configJSON))
			} else {
				fmt.Fprintln(os.Stderr, fmt.Sprintf("Configuration marshal error: %v", err))
			}
			os.Exit(0)
		} else {
			if configYAML, err := yaml.Marshal(outConf); err == nil {
				fmt.Println(string(configYAML))
			} else {
				fmt.Fprintln(os.Stderr, fmt.Sprintf("Configuration marshal error: %v", err))
			}
			os.Exit(0)
		}
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
		os.Exit(0)
	}

	return conf
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
		logger = log.New(os.Stderr, config.Logger)
	} else {
		logger = log.New(os.Stdout, config.Logger)
	}

	// Create our metrics type.
	var stats metrics.Type
	var err error
	stats, err = metrics.New(config.Metrics, metrics.OptSetLogger(logger))
	for err != nil {
		logger.Errorf("Failed to connect to metrics aggregator: %v\n", err)
		<-time.After(time.Second)
		stats, err = metrics.New(config.Metrics, metrics.OptSetLogger(logger))
	}
	defer stats.Close()

	// Create HTTP API with a sanitised service config.
	sanConf, err := config.Sanitised()
	if err != nil {
		logger.Warnf("Failed to generate sanitised config: %v\n", err)
	}
	httpServer := api.New(Version, DateBuilt, config.HTTP, sanConf, logger, stats)

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
		streamMgr := strmmgr.New(
			strmmgr.OptSetAPITimeout(time.Duration(config.HTTP.ReadTimeoutMS)*time.Millisecond),
			strmmgr.OptSetLogger(logger),
			strmmgr.OptSetManager(manager),
			strmmgr.OptSetStats(stats),
		)
		var streamConfs map[string]stream.Config
		if streamConfs, err = strmmgr.LoadStreamConfigsFromDirectory(true, *streamsDir); err != nil {
			logger.Errorf("Failed to load stream configs: %v\n", err)
			os.Exit(1)
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
			logger.Infof("Created %v streams from directory: %v\n", lStreams, *streamsDir)
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
