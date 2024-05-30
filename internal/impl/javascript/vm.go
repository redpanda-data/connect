package javascript

import (
	"context"
	"fmt"

	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/console"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type vmRunner struct {
	vm *goja.Runtime
	p  *goja.Program

	logger *service.Logger

	runBatch      service.MessageBatch
	targetMessage *service.Message
	targetIndex   int
}

func (j *javascriptProcessor) newVM() (*vmRunner, error) {
	vm := goja.New()

	j.requireRegistry.Enable(vm)
	console.Enable(vm)

	vr := &vmRunner{
		vm:     vm,
		logger: j.logger,
		p:      j.program,
	}

	for name, fc := range vmRunnerFunctionCtors {
		if err := setFunction(vr, name, fc.ctor(vr)); err != nil {
			return nil, err
		}
	}
	return vr, nil
}

// The namespace within all our function definitions
const fnCtxName = "benthos"

func setFunction(vr *vmRunner, name string, function jsFunction) error {
	var targetObj *goja.Object
	if targetObjValue := vr.vm.GlobalObject().Get(fnCtxName); targetObjValue != nil {
		targetObj = targetObjValue.ToObject(vr.vm)
	}
	if targetObj == nil {
		if err := vr.vm.GlobalObject().Set(fnCtxName, map[string]any{}); err != nil {
			return fmt.Errorf("failed to set global benthos object: %w", err)
		}
		targetObj = vr.vm.GlobalObject().Get(fnCtxName).ToObject(vr.vm)
	}

	if err := targetObj.Set(name, func(call goja.FunctionCall, rt *goja.Runtime) goja.Value {
		l := vr.logger.With("function", name)
		result, err := function(call, rt, l)
		if err != nil {
			panic(rt.ToValue(err.Error()))
		}
		return rt.ToValue(result)
	}); err != nil {
		return fmt.Errorf("failed to set global function %v: %w", name, err)
	}
	return nil
}

func parseArgs(call goja.FunctionCall, ptrs ...any) error {
	if len(ptrs) < len(call.Arguments) {
		return fmt.Errorf("have %d arguments, but only %d pointers to parse into", len(call.Arguments), len(ptrs))
	}

	for i := 0; i < len(call.Arguments); i++ {
		arg, ptr := call.Argument(i), ptrs[i]

		if goja.IsUndefined(arg) {
			return fmt.Errorf("argument at position %d is undefined", i)
		}

		var err error
		switch p := ptr.(type) {
		case *string:
			*p = arg.String()
		case *int:
			*p = int(arg.ToInteger())
		case *int64:
			*p = arg.ToInteger()
		case *float64:
			*p = arg.ToFloat()
		case *map[string]any:
			*p, err = getMapFromValue(arg)
		case *bool:
			*p = arg.ToBoolean()
		case *[]any:
			*p, err = getSliceFromValue(arg)
		case *[]map[string]any:
			*p, err = getMapSliceFromValue(arg)
		case *goja.Value:
			*p = arg
		case *any:
			*p = arg.Export()
		default:
			return fmt.Errorf("encountered unhandled type %T while trying to parse %v into %v", arg.ExportType().String(), arg, p)
		}
		if err != nil {
			return fmt.Errorf("could not parse %v (%s) into %v (%T): %v", arg, arg.ExportType().String(), ptr, ptr, err)
		}
	}

	return nil
}

func (r *vmRunner) reset() {
	r.runBatch = nil
	r.targetMessage = nil
	r.targetIndex = 0
}

func (r *vmRunner) Run(ctx context.Context, batch service.MessageBatch) (service.MessageBatch, error) {
	defer r.reset()

	var newBatch service.MessageBatch
	for i := range batch {
		r.reset()
		r.runBatch = batch
		r.targetIndex = i
		r.targetMessage = batch[i]

		_, err := r.vm.RunProgram(r.p)
		if err != nil {
			// TODO: Make this more granular, error could be message specific
			return nil, err
		}
		if newMsg := r.targetMessage; newMsg != nil {
			newBatch = append(newBatch, newMsg)
		}
	}
	return newBatch, nil
}

func (r *vmRunner) Close(ctx context.Context) error {
	return nil
}
