package schema

import (
	"sort"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/config"
)

// Full represents the entirety of the Benthos instances configuration spec and
// all plugins.
type Full struct {
	Version           string               `json:"version"`
	Date              string               `json:"date"`
	Config            docs.FieldSpecs      `json:"config,omitempty"`
	Buffers           []docs.ComponentSpec `json:"buffers,omitempty"`
	Caches            []docs.ComponentSpec `json:"caches,omitempty"`
	Inputs            []docs.ComponentSpec `json:"inputs,omitempty"`
	Outputs           []docs.ComponentSpec `json:"outputs,omitempty"`
	Processors        []docs.ComponentSpec `json:"processors,omitempty"`
	RateLimits        []docs.ComponentSpec `json:"rate-limits,omitempty"`
	Metrics           []docs.ComponentSpec `json:"metrics,omitempty"`
	Tracers           []docs.ComponentSpec `json:"tracers,omitempty"`
	conditions        []string
	BloblangFunctions []query.FunctionSpec `json:"bloblang-functions,omitempty"`
	BloblangMethods   []query.MethodSpec   `json:"bloblang-methods,omitempty"`
}

// New walks all registered Benthos components and creates a full schema
// definition of it.
func New(version, date string) Full {
	s := Full{
		Version:           version,
		Date:              date,
		Config:            config.Spec(),
		Buffers:           bundle.AllBuffers.Docs(),
		Caches:            bundle.AllCaches.Docs(),
		Inputs:            bundle.AllInputs.Docs(),
		Outputs:           bundle.AllOutputs.Docs(),
		Processors:        bundle.AllProcessors.Docs(),
		RateLimits:        bundle.AllRateLimits.Docs(),
		Metrics:           bundle.AllMetrics.Docs(),
		Tracers:           bundle.AllTracers.Docs(),
		BloblangFunctions: query.FunctionDocs(),
		BloblangMethods:   query.MethodDocs(),
	}
	sort.Strings(s.conditions)
	return s
}

// Flattened returns a flattened representation of all registered plugin types
// and names.
func (f *Full) Flattened() map[string][]string {
	justNames := func(components []docs.ComponentSpec) []string {
		names := []string{}
		for _, c := range components {
			if c.Status != docs.StatusDeprecated {
				names = append(names, c.Name)
			}
		}
		return names
	}
	justNamesBloblFuncs := func(fns []query.FunctionSpec) []string {
		names := []string{}
		for _, c := range fns {
			if c.Status != query.StatusDeprecated {
				names = append(names, c.Name)
			}
		}
		return names
	}
	justNamesBloblMethods := func(fns []query.MethodSpec) []string {
		names := []string{}
		for _, c := range fns {
			if c.Status != query.StatusDeprecated {
				names = append(names, c.Name)
			}
		}
		return names
	}
	return map[string][]string{
		"buffers":            justNames(f.Buffers),
		"caches":             justNames(f.Caches),
		"inputs":             justNames(f.Inputs),
		"outputs":            justNames(f.Outputs),
		"processors":         justNames(f.Processors),
		"rate-limits":        justNames(f.RateLimits),
		"metrics":            justNames(f.Metrics),
		"tracers":            justNames(f.Tracers),
		"conditions":         f.conditions,
		"bloblang-functions": justNamesBloblFuncs(f.BloblangFunctions),
		"bloblang-methods":   justNamesBloblMethods(f.BloblangMethods),
	}
}

// Scrub walks the schema and removes all descriptions and other long-form
// documentation, reducing the overall size.
func (f *Full) Scrub() {
	scrubFieldSpecs(f.Config)
	scrubComponentSpecs(f.Buffers)
	scrubComponentSpecs(f.Caches)
	scrubComponentSpecs(f.Inputs)
	scrubComponentSpecs(f.Outputs)
	scrubComponentSpecs(f.Processors)
	scrubComponentSpecs(f.RateLimits)
	scrubComponentSpecs(f.Metrics)
	scrubComponentSpecs(f.Tracers)

	for i := range f.BloblangFunctions {
		f.BloblangFunctions[i].Description = ""
		f.BloblangFunctions[i].Examples = nil
		scrubParams(f.BloblangFunctions[i].Params.Definitions)
	}
	for i := range f.BloblangMethods {
		f.BloblangMethods[i].Description = ""
		f.BloblangMethods[i].Examples = nil
		f.BloblangMethods[i].Categories = nil
		scrubParams(f.BloblangMethods[i].Params.Definitions)
	}
}

func scrubParams(p []query.ParamDefinition) {
	for i := range p {
		p[i].Description = ""
	}
}

func scrubFieldSpecs(fs []docs.FieldSpec) {
	for i := range fs {
		fs[i].Description = ""
		fs[i].Examples = nil
		for j := range fs[i].AnnotatedOptions {
			fs[i].AnnotatedOptions[j][1] = ""
		}
		scrubFieldSpecs(fs[i].Children)
	}
}

func scrubFieldSpec(fs *docs.FieldSpec) {
	fs.Description = ""
	scrubFieldSpecs(fs.Children)
}

func scrubComponentSpecs(cs []docs.ComponentSpec) {
	for i := range cs {
		cs[i].Description = ""
		cs[i].Summary = ""
		cs[i].Footnotes = ""
		cs[i].Examples = nil
		scrubFieldSpec(&cs[i].Config)
	}
}
