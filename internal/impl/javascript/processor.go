// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package javascript

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/console"
	"github.com/dop251/goja_nodejs/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	codeField    = "code"
	fileField    = "file"
	includeField = "global_folders"
)

func javascriptProcessorConfig() *service.ConfigSpec {
	functionsSlice := make([]string, 0, len(vmRunnerFunctionCtors))
	for k := range vmRunnerFunctionCtors {
		functionsSlice = append(functionsSlice, k)
	}
	sort.Strings(functionsSlice)

	var description strings.Builder
	for _, name := range functionsSlice {
		_, _ = description.WriteString("\n")
		_, _ = description.WriteString(vmRunnerFunctionCtors[name].String())
	}

	return service.NewConfigSpec().
		Categories("Mapping").
		Version("4.14.0").
		Summary("Executes a provided JavaScript code block or file for each message.").
		Description(`
The https://github.com/dop251/goja[execution engine^] behind this processor provides full ECMAScript 5.1 support (including regex and strict mode). Most of the ECMAScript 6 spec is implemented but this is a work in progress.

Imports via `+"`require`"+` should work similarly to NodeJS, and access to the console is supported which will print via the Redpanda Connect logger. More caveats can be found on https://github.com/dop251/goja#known-incompatibilities-and-caveats[GitHub^].

This processor is implemented using the https://github.com/dop251/goja[github.com/dop251/goja^] library.`).
		Footnotes(`
== Runtime

In order to optimize code execution JS runtimes are created on demand (in order to support parallel execution) and are reused across invocations. Therefore, it is important to understand that global state created by your programs will outlive individual invocations. In order for your programs to avoid failing after the first invocation ensure that you do not define variables at the global scope.

Although technically possible, it is recommended that you do not rely on the global state for maintaining state across invocations as the pooling nature of the runtimes will prevent deterministic behavior. We aim to support deterministic strategies for mutating global state in the future.

== Functions
`+description.String()+`
`).
		Field(service.NewStringField(codeField).
			Description("An inline JavaScript program to run. One of `"+codeField+"` or `"+fileField+"` must be defined.").
			Optional()).
		Field(service.NewStringField(fileField).
			Description("A file containing a JavaScript program to run. One of `"+codeField+"` or `"+fileField+"` must be defined.").
			Optional()).
		Field(service.NewStringListField(includeField).
			Description("List of folders that will be used to load modules from if the requested JS module is not found elsewhere.").
			Default([]string{})).
		LintRule(fmt.Sprintf(`
let codeLen = (this.%v | "").length()
let fileLen = (this.%v | "").length()
root = if $codeLen == 0 && $fileLen == 0 {
  "either the code or file field must be specified"
} else if $codeLen > 0 && $fileLen > 0 {
  "cannot specify both the code and file fields"
}`, codeField, fileField)).
		Example(
			`Simple mutation`,
			`In this example we define a simple function that performs a basic mutation against messages, treating their contents as raw strings.`,
			`
pipeline:
  processors:
    - javascript:
        code: 'benthos.v0_msg_set_string(benthos.v0_msg_as_string() + "hello world");'
`,
		).
		Example(
			`Structured mutation`,
			`In this example we define a function that performs basic mutations against a structured message. Note that we encapsulate the logic within an anonymous function that is called for each invocation, this is required in order to avoid duplicate variable declarations in the global state.`,
			`
pipeline:
  processors:
    - javascript:
        code: |
          (() => {
            let thing = benthos.v0_msg_as_structured();
            thing.num_keys = Object.keys(thing).length;
            delete thing["b"];
            benthos.v0_msg_set_structured(thing);
          })();
`,
		)
}

func init() {
	service.MustRegisterBatchProcessor(
		"javascript", javascriptProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newJavascriptProcessorFromConfig(conf, mgr)
		})
}

//------------------------------------------------------------------------------

type javascriptProcessor struct {
	program         *goja.Program
	requireRegistry *require.Registry
	logger          *service.Logger
	vmPool          sync.Pool
}

func sourceLoader(serviceFS *service.FS) require.SourceLoader {
	// Copy of `require.DefaultSourceLoader`: https://github.com/dop251/goja_nodejs/blob/e84d9a924c5ca9e541575e643b7efbca5705862f/require/module.go#L116-L141
	// with some slight adjustments because we need to use the Benthos manager filesystem for opening and reading files.
	return func(filename string) ([]byte, error) {
		fp := filepath.FromSlash(filename)
		f, err := serviceFS.Open(fp)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				err = require.ModuleFileDoesNotExistError
			} else if runtime.GOOS == "windows" {
				if errors.Is(err, syscall.Errno(0x7b)) { // ERROR_INVALID_NAME, The filename, directory name, or volume label syntax is incorrect.
					err = require.ModuleFileDoesNotExistError
				}
			}
			return nil, err
		}

		defer f.Close()
		// On some systems (e.g. plan9 and FreeBSD) it is possible to use the standard read() call on directories
		// which means we cannot rely on read() returning an error, we have to do stat() instead.
		if fi, err := f.Stat(); err == nil {
			if fi.IsDir() {
				return nil, require.ModuleFileDoesNotExistError
			}
		} else {
			return nil, err
		}

		return io.ReadAll(f)
	}
}

func newJavascriptProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*javascriptProcessor, error) {
	code, _ := conf.FieldString(codeField)
	file, _ := conf.FieldString(fileField)
	if file == "" && code == "" {
		return nil, fmt.Errorf("either a `%s` or `%s` must be specified", codeField, fileField)
	}

	filename := "main.js"
	if file != "" {
		// Open file and read code
		codeBytes, err := service.ReadFile(mgr.FS(), file)
		if err != nil {
			return nil, fmt.Errorf("failed to open target file: %s", err)
		}
		filename = file
		code = string(codeBytes)
	}

	program, err := goja.Compile(filename, code, false)
	if err != nil {
		return nil, fmt.Errorf("failed to compile javascript code: %s", err)
	}

	logger := mgr.Logger()
	registryGlobalFolders, err := conf.FieldStringList(includeField)
	if err != nil {
		return nil, err
	}
	requireRegistry := require.NewRegistry(
		require.WithGlobalFolders(registryGlobalFolders...),
		require.WithLoader(sourceLoader(mgr.FS())),
	)
	requireRegistry.RegisterNativeModule("console", console.RequireWithPrinter(&Logger{logger}))

	return &javascriptProcessor{
		program:         program,
		requireRegistry: requireRegistry,
		logger:          logger,
		vmPool:          sync.Pool{},
	}, nil
}

func (j *javascriptProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	var vr *vmRunner
	var err error
	if vmRunnerPtr := j.vmPool.Get(); vmRunnerPtr != nil {
		vr = vmRunnerPtr.(*vmRunner)
	} else {
		if vr, err = j.newVM(); err != nil {
			return nil, err
		}
	}
	defer func() {
		// TODO: Decide whether to reset the program
		j.vmPool.Put(vr)
	}()

	b, err := vr.Run(ctx, batch)
	if err != nil {
		return nil, err
	}
	return []service.MessageBatch{b}, nil
}

func (j *javascriptProcessor) Close(ctx context.Context) error {
	for {
		mr := j.vmPool.Get()
		if mr == nil {
			return nil
		}
		if err := mr.(*vmRunner).Close(ctx); err != nil {
			return err
		}
	}
}
