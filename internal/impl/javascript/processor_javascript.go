package javascript

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/console"
	"github.com/dop251/goja_nodejs/require"
)

func javascriptProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Mapping").
		Summary("Executes the provided JavaScript code using the github.com/dop251/goja library. The `console` and `require` packages from https://github.com/dop251/goja_nodejs are also implementd.").
		Field(service.NewInterpolatedStringField("code").
			Description("The javascript code to use.").
			Default("")).
		Field(service.NewInterpolatedStringField("file").
			Description("The javascript file to use.").
			Default(""))
}

func init() {
	err := service.RegisterProcessor(
		"javascript", javascriptProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
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
}

func newJavascriptProcessorFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*javascriptProcessor, error) {
	code, err := conf.FieldString("code")
	if err != nil {
		return nil, err
	}
	file, err := conf.FieldString("file")
	if err != nil {
		return nil, err
	}

	if code != "" && file != "" {
		return nil, errors.New("both 'code' and 'file' fields are specified but only one is allowed")
	}
	if code == "" && file == "" {
		return nil, errors.New("neither 'code' nor 'file' fields are specified but one of them is required")
	}

	filename := "main.js"
	var program *goja.Program
	if file != "" {
		// Open file and read code
		codeBytes, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("failed to open the file specifed in 'file' field: %v", err)
		}
		filename = file
		code = string(codeBytes)
	}

	program, err = goja.Compile(filename, code, false)
	if err != nil {
		return nil, fmt.Errorf("failed to compile javascript code: %v", err)
	}

	logger := mgr.Logger()

	// Initialize global registry. This is where modules (JS files) live. This enables easy code re-use.
	// TODO: We need to set a registry root folder. Can we make this configurable somehow? Setting it to the working directory for now.
	// TODO: Implement registry live reloading in dev mode? E. g. if a JS file is modified.
	requireRegistry := require.NewRegistry(require.WithGlobalFolders(""))
	requireRegistry.RegisterNativeModule("console", console.RequireWithPrinter(&Logger{logger}))

	return &javascriptProcessor{program: program, requireRegistry: requireRegistry, logger: logger}, nil
}

func (j *javascriptProcessor) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
	// Create new JS VM
	vm := getVM(m, j.requireRegistry, j.logger)

	// Run JS file
	_, err := vm.RunProgram(j.program)
	if err != nil {
		return nil, err
	}
	return []*service.Message{m}, nil
}

func (j *javascriptProcessor) Close(ctx context.Context) error {
	return nil
}
