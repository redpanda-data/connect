// Package legacy imports old legacy component definitions (and plugins), and
// also walks them during init in order to register their docs and constructors
// using the new APIs.
package legacy

import (
	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/tracer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func init() {
	input.WalkConstructors(func(ctor input.ConstructorFunc, spec docs.ComponentSpec) {
		if err := bundle.AllInputs.Add(func(
			conf input.Config,
			mgr bundle.NewManagement,
			pipes ...types.PipelineConstructorFunc,
		) (input.Type, error) {
			return ctor(conf, mgr, mgr.Logger(), mgr.Metrics(), pipes...)
		}, spec); err != nil {
			panic(err)
		}
	})
	metrics.WalkConstructors(func(ctor metrics.ConstructorFunc, spec docs.ComponentSpec) {
		if err := bundle.AllMetrics.Add(bundle.MetricConstructor(ctor), spec); err != nil {
			panic(err)
		}
	})
	output.WalkConstructors(func(ctor output.ConstructorFunc, spec docs.ComponentSpec) {
		if err := bundle.AllOutputs.Add(func(
			conf output.Config,
			mgr bundle.NewManagement,
			pipes ...types.PipelineConstructorFunc,
		) (output.Type, error) {
			return ctor(conf, mgr, mgr.Logger(), mgr.Metrics(), pipes...)
		}, spec); err != nil {
			panic(err)
		}
	})
	processor.WalkConstructors(func(ctor processor.ConstructorFunc, spec docs.ComponentSpec) {
		if err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.Type, error) {
			return ctor(conf, mgr, mgr.Logger(), mgr.Metrics())
		}, spec); err != nil {
			panic(err)
		}
	})
	tracer.WalkConstructors(func(ctor tracer.ConstructorFunc, spec docs.ComponentSpec) {
		if err := bundle.AllTracers.Add(bundle.TracerConstructor(ctor), spec); err != nil {
			panic(err)
		}
	})
}
