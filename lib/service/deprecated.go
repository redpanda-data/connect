package service

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/service/test"
	"github.com/Jeffail/benthos/v3/lib/tracer"
	uconfig "github.com/Jeffail/benthos/v3/lib/util/config"
)

//------------------------------------------------------------------------------

func cmdDeprecatedUnitTest(path, suffix string, doLint bool) {
	if test.Run(path, suffix, doLint) {
		os.Exit(0)
	}
	os.Exit(1)
}

func cmdDeprecatedUnitTestGenerate(path, suffix string) {
	if err := test.Generate(path, suffix); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to generate config tests: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func cmdDeprecatedLintConfig(lints []string) {
	if len(lints) > 0 {
		for _, l := range lints {
			fmt.Fprintln(os.Stderr, l)
		}
		os.Exit(1)
	}
	os.Exit(0)
}

func cmdDeprecatedPrintConfig(conf *config.Type, examples string, showAll bool, jsonFormat bool) {
	var outConf interface{}
	var err error

	if len(examples) > 0 {
		config.AddExamples(conf, strings.Split(examples, ",")...)
	}

	if !showAll {
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

	if jsonFormat {
		if configJSON, err := json.Marshal(outConf); err == nil {
			fmt.Println(string(configJSON))
		} else {
			fmt.Fprintln(os.Stderr, fmt.Sprintf("Configuration marshal error: %v", err))
		}
	} else {
		if configYAML, err := uconfig.MarshalYAML(outConf); err == nil {
			fmt.Println(string(configYAML))
		} else {
			fmt.Fprintln(os.Stderr, fmt.Sprintf("Configuration marshal error: %v", err))
		}
	}

	os.Exit(0)
}

func cmdDeprecatedPrintDocs(
	printInputs bool,
	printProcessors bool,
	printConditions bool,
	printRateLimits bool,
	printBuffers bool,
	printOutputs bool,
	printCaches bool,
	printMetrics bool,
	printTracers bool,
	printInputPlugins bool,
	printProcessorPlugins bool,
	printConditionPlugins bool,
	printRateLimitPlugins bool,
	printOutputPlugins bool,
	printCachePlugins bool,
) {
	if printInputs {
		fmt.Println(input.Descriptions())
	}
	if printProcessors {
		fmt.Println(processor.Descriptions())
	}
	if printConditions {
		fmt.Println(condition.Descriptions())
	}
	if printRateLimits {
		fmt.Println(ratelimit.Descriptions())
	}
	if printBuffers {
		fmt.Println(buffer.Descriptions())
	}
	if printOutputs {
		fmt.Println(output.Descriptions())
	}
	if printCaches {
		fmt.Println(cache.Descriptions())
	}
	if printMetrics {
		fmt.Println(metrics.Descriptions())
	}
	if printTracers {
		fmt.Println(tracer.Descriptions())
	}
	if printInputPlugins {
		fmt.Println(input.PluginDescriptions())
	}
	if printProcessorPlugins {
		fmt.Println(processor.PluginDescriptions())
	}
	if printConditionPlugins {
		fmt.Println(condition.PluginDescriptions())
	}
	if printRateLimitPlugins {
		fmt.Println(ratelimit.PluginDescriptions())
	}
	if printOutputPlugins {
		fmt.Println(output.PluginDescriptions())
	}
	if printCachePlugins {
		fmt.Println(cache.PluginDescriptions())
	}
	os.Exit(0)
}

//------------------------------------------------------------------------------

var depFlags struct {
	showConfigJSON  bool
	showConfigYAML  bool
	showAll         bool
	lintConfig      bool
	runTests        string
	generateTests   string
	strictConfig    bool
	examples        string
	printInputs     bool
	printOutputs    bool
	printBuffers    bool
	printProcessors bool
	printConditions bool
	printCaches     bool
	printRateLimits bool
	printMetrics    bool
	printTracers    bool
	streamsMode     bool
	streamsDir      string

	// Plugin Flags
	printInputPlugins     bool
	printOutputPlugins    bool
	printProcessorPlugins bool
	printConditionPlugins bool
	printCachePlugins     bool
	printRateLimitPlugins bool
}

func checkDeprecatedFlags(args []string) (*flag.FlagSet, bool) {
	flagSet := flag.NewFlagSet("benthos", flag.ExitOnError)

	flagSet.BoolVar(
		&depFlags.streamsMode, "streams", false,
		`
Run Benthos in streams mode, where streams can be created, updated and removed
via REST HTTP endpoints. In streams mode the stream fields of a config file
(input, buffer, pipeline, output) will be ignored. Instead, any .yaml or .json
files inside the --streams-dir directory will be parsed as stream configs.`[1:],
	)
	flagSet.StringVar(
		&depFlags.streamsDir, "streams-dir", "",
		`
When running Benthos in streams mode any files in this directory with a .json or
.yaml extension will be parsed as a stream configuration (input, buffer,
pipeline, output), where the filename less the extension will be the id of the
stream.`[1:],
	)
	flagSet.BoolVar(
		&depFlags.showConfigJSON, "print-json", false, "Print loaded configuration as JSON, then exit",
	)
	flagSet.BoolVar(
		&depFlags.showConfigYAML, "print-yaml", false, "Print loaded configuration as YAML, then exit",
	)
	flagSet.BoolVar(
		&depFlags.showAll, "all", false,
		`
Set whether all fields should be shown when printing configuration via
--print-yaml or --print-json, otherwise only used values will be printed.`[1:],
	)
	flagSet.BoolVar(
		&depFlags.lintConfig, "lint", false, "Lint the target configuration file, then exit",
	)
	flagSet.StringVar(
		&depFlags.runTests, "test", "",
		`
EXPERIMENTAL: This flag is subject to change outside of major version releases.

Execute unit tests, then exit. The argument may point to a config file, test
definition or directory, and supports '...' wildcards, e.g. './foo/...' would
execute all tests found under the directory 'foo'. When combining this flag with
-lint each tested config will also be linted.`[1:],
	)
	flagSet.StringVar(
		&depFlags.generateTests, "gen-test", "",
		`
EXPERIMENTAL: This flag is subject to change outside of major version releases.

Generate unit test definition files for Benthos configs, then exit. The argument
may point to a config file or directory and supports '...' wildcards, e.g.
'./foo/...' would generate tests for all Benthos configs found under the
directory 'foo'.`[1:],
	)
	flagSet.BoolVar(
		&depFlags.strictConfig, "strict", false,
		`
Parse config files in strict mode, where any linting errors will cause Benthos
to fail`[1:],
	)
	flagSet.StringVar(
		&depFlags.examples, "example", "",
		`
Add specific examples when printing a configuration file with --print-yaml or
--print-json by listing comma separated types. Types can be any input, buffer,
processor or output.

For example: 'benthos --print-yaml --example websocket,jmespath' would print a
config with a websocket input and output and a jmespath processor.`[1:],
	)
	flagSet.BoolVar(
		&depFlags.printInputs, "list-inputs", false,
		"Print a list of available input options, then exit",
	)
	flagSet.BoolVar(
		&depFlags.printOutputs, "list-outputs", false,
		"Print a list of available output options, then exit",
	)
	flagSet.BoolVar(
		&depFlags.printBuffers, "list-buffers", false,
		"Print a list of available buffer options, then exit",
	)
	flagSet.BoolVar(
		&depFlags.printProcessors, "list-processors", false,
		"Print a list of available processor options, then exit",
	)
	flagSet.BoolVar(
		&depFlags.printConditions, "list-conditions", false,
		"Print a list of available processor condition options, then exit",
	)
	flagSet.BoolVar(
		&depFlags.printCaches, "list-caches", false,
		"Print a list of available cache options, then exit",
	)
	flagSet.BoolVar(
		&depFlags.printRateLimits, "list-rate-limits", false,
		"Print a list of available rate_limit options, then exit",
	)
	flagSet.BoolVar(
		&depFlags.printMetrics, "list-metrics", false,
		"Print a list of available metrics options, then exit",
	)
	flagSet.BoolVar(
		&depFlags.printTracers, "list-tracers", false,
		"Print a list of available tracer options, then exit",
	)

	if input.PluginCount() > 0 {
		flagSet.BoolVar(
			&depFlags.printInputPlugins, "list-input-plugins", false,
			"Print a list of available input plugins, then exit",
		)
	}
	if output.PluginCount() > 0 {
		flagSet.BoolVar(
			&depFlags.printOutputPlugins, "list-output-plugins", false,
			"Print a list of available output plugins, then exit",
		)
	}
	if processor.PluginCount() > 0 {
		flagSet.BoolVar(
			&depFlags.printProcessorPlugins, "list-processor-plugins", false,
			"Print a list of available processor plugins, then exit",
		)
	}
	if condition.PluginCount() > 0 {
		flagSet.BoolVar(
			&depFlags.printConditionPlugins, "list-condition-plugins", false,
			"Print a list of available condition plugins, then exit",
		)
	}
	if cache.PluginCount() > 0 {
		flagSet.BoolVar(
			&depFlags.printCachePlugins, "list-cache-plugins", false,
			"Print a list of available cache plugins, then exit",
		)
	}
	if ratelimit.PluginCount() > 0 {
		flagSet.BoolVar(
			&depFlags.printRateLimitPlugins, "list-rate-limit-plugins", false,
			"Print a list of available ratelimit plugins, then exit",
		)
	}

	foundOldFlag := false
	flagSet.VisitAll(func(f *flag.Flag) {
		if foundOldFlag {
			return
		}
		for _, arg := range args {
			if !strings.HasPrefix(arg, "-") {
				continue
			}
			trimmed := strings.TrimPrefix(strings.TrimPrefix(arg, "--"), "-")
			if f.Name == trimmed {
				foundOldFlag = true
				break
			}
		}
	})

	if foundOldFlag {
		return flagSet, true
	}
	return nil, false
}

func deprecatedExecute(configPath string, testSuffix string) {
	fmt.Fprintln(os.Stderr, "Running with deprecated CLI flags, use --help to see an up to date summary.")

	if len(depFlags.runTests) > 0 {
		cmdDeprecatedUnitTest(depFlags.runTests, testSuffix, depFlags.lintConfig)
	}

	if len(depFlags.generateTests) > 0 {
		cmdDeprecatedUnitTestGenerate(depFlags.generateTests, testSuffix)
	}

	if depFlags.lintConfig {
		lints := readConfig(configPath, nil)
		cmdDeprecatedLintConfig(lints)
	}

	// If the user wants the configuration to be printed we do so and then exit.
	if depFlags.showConfigJSON || depFlags.showConfigYAML {
		readConfig(configPath, nil)
		cmdDeprecatedPrintConfig(&conf, depFlags.examples, depFlags.showAll, depFlags.showConfigJSON)
	}

	// If we only want to print our inputs or outputs we should exit afterwards
	if depFlags.printInputs || depFlags.printOutputs || depFlags.printBuffers || depFlags.printProcessors ||
		depFlags.printConditions || depFlags.printCaches || depFlags.printRateLimits ||
		depFlags.printMetrics || depFlags.printTracers || depFlags.printInputPlugins ||
		depFlags.printOutputPlugins || depFlags.printProcessorPlugins || depFlags.printConditionPlugins ||
		depFlags.printCachePlugins || depFlags.printRateLimitPlugins {
		cmdDeprecatedPrintDocs(
			depFlags.printInputs,
			depFlags.printProcessors,
			depFlags.printConditions,
			depFlags.printRateLimits,
			depFlags.printBuffers,
			depFlags.printOutputs,
			depFlags.printCaches,
			depFlags.printMetrics,
			depFlags.printTracers,
			depFlags.printInputPlugins,
			depFlags.printProcessorPlugins,
			depFlags.printConditionPlugins,
			depFlags.printRateLimitPlugins,
			depFlags.printOutputPlugins,
			depFlags.printCachePlugins,
		)
	}

	if depFlags.streamsMode {
		dirs := []string{}
		if len(depFlags.streamsDir) > 0 {
			dirs = append(dirs, depFlags.streamsDir)
		}
		os.Exit(cmdService(configPath, nil, "", depFlags.strictConfig, depFlags.streamsMode, dirs))
	}
}
