package service

import (
	"github.com/benthosdev/benthos/v4/internal/config/schema"
	"github.com/benthosdev/benthos/v4/internal/cuegen"
)

// EnvironmentSchema represents a schema definition for all components
// registered within the environment.
type EnvironmentSchema struct {
	s schema.Full
}

func (e *Environment) GenerateSchema(version, dateBuilt string) *EnvironmentSchema {
	schema := schema.New(version, dateBuilt)
	return &EnvironmentSchema{s: schema}
}

// ReduceToStatus removes all components that aren't of the given stability
// status.
func (e *EnvironmentSchema) ReduceToStatus(status string) *EnvironmentSchema {
	e.s.ReduceToStatus(status)
	return e
}

// Minimise removes all documentation from the schema definition.
func (e *EnvironmentSchema) Minimise() *EnvironmentSchema {
	e.s.Scrub()
	return e
}

// ToCUE attempts to generate a CUE schema.
func (e *EnvironmentSchema) ToCUE() ([]byte, error) {
	return cuegen.GenerateSchema(e.s)
}

// XFlattened returns a generic structure of the schema as a map of component
// types to component names.
//
// Experimental: This method is experimental and therefore could be changed
// outside of major version releases.
func (e *EnvironmentSchema) XFlattened() map[string][]string {
	return e.s.Flattened()
}
