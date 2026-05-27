// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpcn

import (
	"errors"
	"fmt"
)

// Environment is the plugin author's registry of components. Mirrors
// the registration surface of *service.Environment for the subset
// that's plugin-relevant.
//
// A plugin's main() builds an Environment, registers one or more
// components, and calls Serve(env) which runs the gRPC protocol the
// host drives.
type Environment struct {
	procs   []registeredProcessor
	inputs  []registeredInput
	outputs []registeredOutput
}

type registeredProcessor struct {
	name string
	spec *ConfigSpec
	ctor BatchProcessorCtor
}

type registeredInput struct {
	name string
	spec *ConfigSpec
	ctor BatchInputCtor
}

type registeredOutput struct {
	name string
	spec *ConfigSpec
	ctor BatchOutputCtor
}

// NewEnvironment creates an empty Environment.
func NewEnvironment() *Environment {
	return &Environment{}
}

// RegisterBatchProcessor registers a new processor component. Mirrors
// (*service.Environment).RegisterBatchProcessor.
func (e *Environment) RegisterBatchProcessor(name string, spec *ConfigSpec, ctor BatchProcessorCtor) error {
	if err := validateRegistration(name, spec, ctor); err != nil {
		return err
	}
	if e.hasComponent(name) {
		return fmt.Errorf("component %q is already registered", name)
	}
	e.procs = append(e.procs, registeredProcessor{name: name, spec: spec, ctor: ctor})
	return nil
}

// RegisterBatchInput registers a new input component. Mirrors
// (*service.Environment).RegisterBatchInput.
//
// PoC note: the host side ignores this registration today; inputs land
// in a follow-up.
func (e *Environment) RegisterBatchInput(name string, spec *ConfigSpec, ctor BatchInputCtor) error {
	if err := validateRegistration(name, spec, ctor); err != nil {
		return err
	}
	if e.hasComponent(name) {
		return fmt.Errorf("component %q is already registered", name)
	}
	e.inputs = append(e.inputs, registeredInput{name: name, spec: spec, ctor: ctor})
	return nil
}

// RegisterBatchOutput registers a new output component. Mirrors
// (*service.Environment).RegisterBatchOutput.
//
// PoC note: the host side ignores this registration today; outputs land
// in a follow-up.
func (e *Environment) RegisterBatchOutput(name string, spec *ConfigSpec, ctor BatchOutputCtor) error {
	if err := validateRegistration[BatchOutputCtor](name, spec, ctor); err != nil {
		return err
	}
	if e.hasComponent(name) {
		return fmt.Errorf("component %q is already registered", name)
	}
	e.outputs = append(e.outputs, registeredOutput{name: name, spec: spec, ctor: ctor})
	return nil
}

func (e *Environment) hasComponent(name string) bool {
	for _, p := range e.procs {
		if p.name == name {
			return true
		}
	}
	for _, in := range e.inputs {
		if in.name == name {
			return true
		}
	}
	for _, out := range e.outputs {
		if out.name == name {
			return true
		}
	}
	return false
}

// validateRegistration enforces the minimum invariants every component
// registration must satisfy.
func validateRegistration[T any](name string, spec *ConfigSpec, ctor T) error {
	if name == "" {
		return errors.New("component name is required")
	}
	if spec == nil {
		return errors.New("component spec is required")
	}
	// ctor is an interface-typed function under the hood; nil-check the
	// zero value to catch authors who pass an uninitialised variable.
	if isNil(ctor) {
		return errors.New("component constructor is required")
	}
	return nil
}

// isNil safely checks whether the supplied generic constructor value is
// nil. Go's `== nil` doesn't work on a generic type parameter directly,
// so we route through any.
func isNil(v any) bool {
	return v == nil
}
