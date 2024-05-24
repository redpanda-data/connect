package io

import (
	"os"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func init() {
	if err := bloblang.RegisterFunctionV2("hostname",
		bloblang.NewPluginSpec().
			Impure().
			Category(query.FunctionCategoryEnvironment).
			Description(`Returns a string matching the hostname of the machine running Benthos.`).
			Example("", `root.thing.host = hostname()`),
		func(_ *bloblang.ParsedParams) (bloblang.Function, error) {
			return func() (any, error) {
				hn, err := os.Hostname()
				if err != nil {
					return nil, err
				}
				return hn, err
			}, nil
		},
	); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterFunctionV2("env",
		bloblang.NewPluginSpec().
			Impure().
			StaticWithFunc(func(args *bloblang.ParsedParams) bool {
				noCache, _ := args.GetBool("no_cache")
				return !noCache
			}).
			Category(query.FunctionCategoryEnvironment).
			Description("Returns the value of an environment variable, or `null` if the environment variable does not exist.").
			Param(bloblang.NewStringParam("name").
				Description("The name of an environment variable.")).
			Param(bloblang.NewBoolParam("no_cache").
				Description("Force the variable lookup to occur for each mapping invocation.").
				Default(false)).
			Example("", `root.thing.key = env("key").or("default value")`).
			Example("", `root.thing.key = env(this.thing.key_name)`).
			Example(
				"When the name parameter is static this function will only resolve once and yield the same result for each invocation as an optimization, this means that updates to env vars during runtime will not be reflected. You can disable this cache with the optional parameter `no_cache`, which when set to `true` will cause the variable lookup to be performed for each execution of the mapping.",
				`root.thing.key = env(name: "key", no_cache: true)`,
			),
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {
			name, err := args.GetString("name")
			if err != nil {
				return nil, err
			}

			noCache, err := args.GetBool("no_cache")
			if err != nil {
				return nil, err
			}

			var cachedValue any
			if !noCache {
				if valueStr, exists := os.LookupEnv(name); exists {
					cachedValue = valueStr
				}
			}

			return func() (any, error) {
				if noCache {
					if valueStr, exists := os.LookupEnv(name); exists {
						return valueStr, nil
					}
					return nil, nil
				}
				return cachedValue, nil
			}, nil
		},
	); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterFunctionV2("file",
		bloblang.NewPluginSpec().
			Impure().
			StaticWithFunc(func(args *bloblang.ParsedParams) bool {
				noCache, _ := args.GetBool("no_cache")
				return !noCache
			}).
			Category(query.FunctionCategoryEnvironment).
			Description("Reads a file and returns its contents. Relative paths are resolved from the directory of the process executing the mapping. In order to read files relative to the mapping file use the newer <<file_rel, `file_rel` function>>").
			Param(bloblang.NewStringParam("path").
				Description("The path of the target file.")).
			Param(bloblang.NewBoolParam("no_cache").
				Description("Force the file to be read for each mapping invocation.").
				Default(false)).
			Example("", `root.doc = file(env("BENTHOS_TEST_BLOBLANG_FILE")).parse_json()`, [2]string{
				`{}`,
				`{"doc":{"foo":"bar"}}`,
			}).
			Example(
				"When the path parameter is static this function will only read the specified file once and yield the same result for each invocation as an optimization, this means that updates to files during runtime will not be reflected. You can disable this cache with the optional parameter `no_cache`, which when set to `true` will cause the file to be read for each execution of the mapping.",
				`root.doc = file(path: env("BENTHOS_TEST_BLOBLANG_FILE"), no_cache: true).parse_json()`,
				[2]string{`{}`, `{"doc":{"foo":"bar"}}`},
			),
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {
			path, err := args.GetString("path")
			if err != nil {
				return nil, err
			}

			noCache, err := args.GetBool("no_cache")
			if err != nil {
				return nil, err
			}

			var cachedPathBytes []byte
			if !noCache {
				// TODO: Obtain FS from bloblang environment.
				if cachedPathBytes, err = os.ReadFile(path); err != nil {
					return nil, err
				}
			}

			return func() (any, error) {
				if noCache {
					return os.ReadFile(path)
				}
				return cachedPathBytes, nil
			}, nil
		},
	); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterFunctionV2("file_rel",
		bloblang.NewPluginSpec().
			Impure().
			StaticWithFunc(func(args *bloblang.ParsedParams) bool {
				noCache, _ := args.GetBool("no_cache")
				return !noCache
			}).
			Category(query.FunctionCategoryEnvironment).
			Description("Reads a file and returns its contents. Relative paths are resolved from the directory of the mapping.").
			Param(bloblang.NewStringParam("path").
				Description("The path of the target file.")).
			Param(bloblang.NewBoolParam("no_cache").
				Description("Force the file to be read for each mapping invocation.").
				Default(false)).
			Example("", `root.doc = file_rel(env("BENTHOS_TEST_BLOBLANG_FILE")).parse_json()`, [2]string{
				`{}`,
				`{"doc":{"foo":"bar"}}`,
			}).
			Example(
				"When the path parameter is static this function will only read the specified file once and yield the same result for each invocation as an optimization, this means that updates to files during runtime will not be reflected. You can disable this cache with the optional parameter `no_cache`, which when set to `true` will cause the file to be read for each execution of the mapping.",
				`root.doc = file_rel(path: env("BENTHOS_TEST_BLOBLANG_FILE"), no_cache: true).parse_json()`,
				[2]string{`{}`, `{"doc":{"foo":"bar"}}`},
			),
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {
			path, err := args.GetString("path")
			if err != nil {
				return nil, err
			}

			noCache, err := args.GetBool("no_cache")
			if err != nil {
				return nil, err
			}

			var cachedPathBytes []byte
			if !noCache {
				if cachedPathBytes, err = args.ImportFile(path); err != nil {
					return nil, err
				}
			}

			return func() (any, error) {
				if noCache {
					return args.ImportFile(path)
				}
				return cachedPathBytes, nil
			}, nil
		},
	); err != nil {
		panic(err)
	}
}
