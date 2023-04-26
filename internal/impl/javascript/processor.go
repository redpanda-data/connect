package javascript

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/console"
	"github.com/dop251/goja_nodejs/require"

	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/public/service"
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
The [execution engine](https://github.com/dop251/goja) behind this processor provides full ECMAScript 5.1 support (including regex and strict mode). Most of the ECMAScript 6 spec is implemented but this is a work in progress.

Imports via ` + "`require`" + ` should work similarly to NodeJS, and access to the console is supported which will print via the Benthos logger. More caveats can be [found here](https://github.com/dop251/goja#known-incompatibilities-and-caveats).

## Functions
` + description.String() + `

This processor is implemented using the [github.com/dop251/goja](https://github.com/dop251/goja) library.
`).
		Field(service.NewInterpolatedStringField(codeField).
			Description("An inline JavaScript program to run. One of `" + codeField + "` or `" + fileField + "` must be defined.").
			Optional()).
		Field(service.NewInterpolatedStringField(fileField).
			Description("A file containing a JavaScript program to run. One of `" + codeField + "` or `" + fileField + "` must be defined.").
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
}`, codeField, fileField))
}

func init() {
	err := service.RegisterBatchProcessor(
		"javascript", javascriptProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newJavascriptProcessorFromConfig(conf, mgr)
		})

	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type javascriptProcessor struct {
	program         *goja.Program
	requireRegistry *require.Registry
	logger          *service.Logger
	vmPool          sync.Pool
}

func newJavascriptProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*javascriptProcessor, error) {
	code, _ := conf.FieldString(codeField)
	file, _ := conf.FieldString(fileField)
	if file == "" && code == "" {
		return nil, fmt.Errorf("either a `%v` or `%v` must be specified", codeField, fileField)
	}

	filename := "main.js"
	if file != "" {
		// Open file and read code
		codeBytes, err := ifs.ReadFile(mgr.FS(), file)
		if err != nil {
			return nil, fmt.Errorf("failed to open target file: %w", err)
		}
		filename = file
		code = string(codeBytes)
	}

	program, err := goja.Compile(filename, code, false)
	if err != nil {
		return nil, fmt.Errorf("failed to compile javascript code: %v", err)
	}

	logger := mgr.Logger()
	registryGlobalFolders, err := conf.FieldStringList(includeField)
	if err != nil {
		return nil, err
	}
	requireRegistry := require.NewRegistry(
		require.WithGlobalFolders(registryGlobalFolders...),
		require.WithLoader(func(path string) ([]byte, error) {
			return ifs.ReadFile(mgr.FS(), path)
		}))
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
