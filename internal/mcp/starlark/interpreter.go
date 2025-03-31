/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package starlark

import (
	"errors"
	"fmt"
	"os"

	"github.com/redpanda-data/benthos/v4/public/service"
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

// EvalResult represents the evaluated contents of a starlark file.
type EvalResult struct {
	Processors map[string]starlarkComponent
}

// Eval attempts to parse a Starlark file.
func Eval(env *service.Environment, logger *service.Logger, path string, contents []byte) (*EvalResult, error) {
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
	result := &EvalResult{
		Processors: make(map[string]starlarkComponent),
	}
	mcpToolFn := func(thread *starlark.Thread, b *starlark.Builtin, args starlark.Tuple, kwargs []starlark.Tuple) (starlark.Value, error) {
		if len(args) != 0 {
			return nil, errors.New("unexpected positional arguments")
		}
		var (
			label     string
			processor *starlarkComponent
		)
		err := starlark.UnpackArgs(
			b.Name(),
			args,
			kwargs,
			"label",
			&label,
			"processor",
			&processor,
		)
		if err != nil {
			return nil, err
		}
		if processor == nil {
			return nil, errors.New("processor is required")
		}
		name := processor.Name
		if name == "attempt" {
			name = "try"
		}
		// TODO: Check for duplicate labels
		result.Processors[label] = starlarkComponent{
			Name:             name,
			SerializedConfig: processor.SerializedConfig,
		}
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
		// TODO: Use the correct secret lookup function
		value := os.Getenv(name)
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
				logger.Warnf("Skipping processor %v due to invalid identifier: %v", name, err)
				return
			}
			methodName = newName
		}
		spec, err := extractFieldSpec(conf)
		if err != nil {
			walkErr = fmt.Errorf("error extracting field spec for %s: %v", name, err)
			return
		}
		builtin, err := toBuiltinMethod(methodName, spec)
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
