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

package starlark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/redpanda-data/benthos/v4/public/service"
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

// MCPProcessorTool represents a processor tool defined in a Starlark file.
type MCPProcessorTool struct {
	Label            string
	Description      string
	Name             string
	SerializedConfig json.RawMessage
}

// EvalResult represents the evaluated contents of a starlark file.
type EvalResult struct {
	Processors []MCPProcessorTool
}

// Eval attempts to parse a Starlark file.
func Eval(
	ctx context.Context,
	env *service.Environment,
	logger *slog.Logger,
	path string,
	contents []byte,
	envVarLookupFunc func(context.Context, string) (string, bool),
) (*EvalResult, error) {
	opts := &syntax.FileOptions{
		Set:               true,
		While:             true,
		TopLevelControl:   true,
		GlobalReassign:    true,
		LoadBindsGlobally: false,
		Recursion:         true,
	}
	thread := &starlark.Thread{
		Name: "main",
		Print: func(_ *starlark.Thread, msg string) {
			logger.Debug(msg)
		},
		Load: func(_ *starlark.Thread, module string) (starlark.StringDict, error) {
			return nil, errors.New("load disallowed")
		},
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-ctx.Done()
		thread.Cancel("context cancelled")
	}()
	result := &EvalResult{}
	mcpToolFn := func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if len(args) != 0 {
			return nil, errors.New("unexpected positional arguments")
		}
		var (
			label       string
			description string
			processor   *starlarkComponent
		)
		err := starlark.UnpackArgs(
			b.Name(),
			args,
			kwargs,
			"label",
			&label,
			"description?",
			&description,
			"processor",
			&processor,
		)
		if err != nil {
			return nil, err
		}
		if processor == nil {
			return nil, errors.New("processor is required")
		}
		// TODO: Check for duplicate labels
		result.Processors = append(result.Processors, MCPProcessorTool{
			Label:            label,
			Description:      description,
			Name:             processor.Name,
			SerializedConfig: slices.Clone(processor.SerializedConfig),
		})
		return starlark.None, nil
	}
	secretFn := func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		var (
			name string
		)
		err := starlark.UnpackArgs(
			b.Name(),
			args,
			kwargs,
			"name",
			&name,
		)
		if err != nil {
			return nil, err
		}
		if name == "" {
			return nil, errors.New("name is required")
		}
		value, ok := envVarLookupFunc(ctx, name)
		if !ok {
			return starlark.None, nil
		}
		return starlark.String(value), nil
	}
	predeclared := starlark.StringDict{
		"mcp_tool": starlark.NewBuiltin("mcp_tool", mcpToolFn),
		"secret":   starlark.NewBuiltin("secret", secretFn),
	}
	var walkErr error
	env.WalkProcessors(func(name string, conf *service.ConfigView) {
		_, err := opts.ParseExpr(path, name+"()", 0)
		methodName := name
		if err != nil {
			newName, ok := identifierReplacements[name]
			if !ok {
				logger.Warn("Skipping processor %v due to invalid identifier: %v", name, err)
				return
			}
			methodName = newName
		}
		spec, err := extractFieldSpec(conf)
		if err != nil {
			walkErr = fmt.Errorf("error extracting field spec for %s: %v", name, err)
			return
		}
		builtin, err := toBuiltinMethod(methodName, name, spec)
		if err != nil {
			walkErr = fmt.Errorf("error building constructor for %s: %v", name, err)
			return
		}
		predeclared[methodName] = builtin
	})
	if walkErr != nil {
		return nil, walkErr
	}
	_, err := starlark.ExecFileOptions(opts, thread, path, contents, predeclared)
	if err != nil {
		return nil, fmt.Errorf("error loading %s: %v", path, err)
	}
	return result, nil
}
