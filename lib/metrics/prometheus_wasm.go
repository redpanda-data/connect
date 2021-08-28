//go:build wasm
// +build wasm

package metrics

import "errors"

//------------------------------------------------------------------------------

// NewPrometheus creates and returns a new Prometheus object.
func NewPrometheus(config Config, opts ...func(Type)) (Type, error) {
	return nil, errors.New("Prometheus metrics are disabled in WASM builds")
}

//------------------------------------------------------------------------------
