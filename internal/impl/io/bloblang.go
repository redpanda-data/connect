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
			Static().
			Category(query.FunctionCategoryEnvironment).
			Description("Returns the value of an environment variable, or `null` if the environment variable does not exist.").
			Param(bloblang.NewStringParam("name").Description("The name of an environment variable.")).
			Example("", `root.thing.key = env("key").or("default value")`),
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {
			name, err := args.GetString("name")
			if err != nil {
				return nil, err
			}

			var value any
			if valueStr, exists := os.LookupEnv(name); exists {
				value = valueStr
			}

			return func() (any, error) {
				return value, nil
			}, nil
		},
	); err != nil {
		panic(err)
	}

	if err := bloblang.RegisterFunctionV2("file",
		bloblang.NewPluginSpec().
			Impure().
			Static().
			Category(query.FunctionCategoryEnvironment).
			Description("Reads a file and returns its contents. Relative paths are resolved from the directory of the process executing the mapping.").
			Param(bloblang.NewStringParam("path").Description("The path of the target file.")).
			Example("", `root.doc = file(env("BENTHOS_TEST_BLOBLANG_FILE")).parse_json()`, [2]string{
				`{}`,
				`{"doc":{"foo":"bar"}}`,
			}),
		func(args *bloblang.ParsedParams) (bloblang.Function, error) {
			path, err := args.GetString("path")
			if err != nil {
				return nil, err
			}

			// TODO: Obtain FS from bloblang environment.
			pathBytes, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}

			return func() (any, error) {
				return pathBytes, nil
			}, nil
		},
	); err != nil {
		panic(err)
	}
}
