// Package legacy imports old legacy component definitions (and plugins), and
// also walks them during init in order to register their docs and constructors
// using the new APIs.
package legacy

import (
	"github.com/benthosdev/benthos/v4/internal/bundle"
	iinput "github.com/benthosdev/benthos/v4/internal/component/input"
	ioutput "github.com/benthosdev/benthos/v4/internal/component/output"
	iprocessor "github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/old/input"
	"github.com/benthosdev/benthos/v4/internal/old/output"
	"github.com/benthosdev/benthos/v4/internal/old/processor"
)

func init() {
	input.WalkConstructors(func(ctor input.ConstructorFunc, spec docs.ComponentSpec) {
		if err := bundle.AllInputs.Add(func(
			conf input.Config,
			mgr bundle.NewManagement,
			pipes ...iprocessor.PipelineConstructorFunc,
		) (iinput.Streamed, error) {
			return ctor(conf, mgr, mgr.Logger(), mgr.Metrics(), pipes...)
		}, spec); err != nil {
			panic(err)
		}
	})
	output.WalkConstructors(func(ctor output.ConstructorFunc, spec docs.ComponentSpec) {
		if err := bundle.AllOutputs.Add(func(
			conf output.Config,
			mgr bundle.NewManagement,
			pipes ...iprocessor.PipelineConstructorFunc,
		) (ioutput.Streamed, error) {
			return ctor(conf, mgr, mgr.Logger(), mgr.Metrics(), pipes...)
		}, spec); err != nil {
			panic(err)
		}
	})
	processor.WalkConstructors(func(ctor processor.ConstructorFunc, spec docs.ComponentSpec) {
		if err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (iprocessor.V1, error) {
			return ctor(conf, mgr, mgr.Logger(), mgr.Metrics())
		}, spec); err != nil {
			panic(err)
		}
	})
}
