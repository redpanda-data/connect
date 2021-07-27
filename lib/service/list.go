package service

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/urfave/cli/v2"
)

type fullSchema struct {
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
	BloblangFunctions []string `json:"bloblang-functions,omitempty"`
	BloblangMethods   []string `json:"bloblang-methods,omitempty"`
}

func (f *fullSchema) flattened() map[string][]string {
	justNames := func(components []docs.ComponentSpec) []string {
		names := []string{}
		for _, c := range components {
			if c.Status != docs.StatusDeprecated {
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
		"bloblang-functions": f.BloblangFunctions,
		"bloblang-methods":   f.BloblangMethods,
	}
}

func listComponents(c *cli.Context) {
	ofTypes := map[string]struct{}{}
	for _, k := range c.Args().Slice() {
		ofTypes[k] = struct{}{}
	}

	schema := fullSchema{
		Config:            config.Spec(),
		Buffers:           bundle.AllBuffers.Docs(),
		Caches:            bundle.AllCaches.Docs(),
		Inputs:            bundle.AllInputs.Docs(),
		Outputs:           bundle.AllOutputs.Docs(),
		Processors:        bundle.AllProcessors.Docs(),
		RateLimits:        bundle.AllRateLimits.Docs(),
		Metrics:           bundle.AllMetrics.Docs(),
		Tracers:           bundle.AllTracers.Docs(),
		BloblangFunctions: query.ListFunctions(),
		BloblangMethods:   query.ListMethods(),
	}
	for t := range condition.Constructors {
		schema.conditions = append(schema.conditions, t)
	}
	sort.Strings(schema.conditions)

	switch c.String("format") {
	case "text":
		flat := schema.flattened()
		i := 0
		for _, k := range []string{
			"inputs",
			"processors",
			"conditions",
			"outputs",
			"caches",
			"rate-limits",
			"buffers",
			"metrics",
			"tracers",
			"bloblang-functions",
			"bloblang-methods",
		} {
			if _, exists := ofTypes[k]; len(ofTypes) > 0 && !exists {
				continue
			}
			if i > 0 {
				fmt.Println("")
			}
			i++
			title := strings.Title(strings.ReplaceAll(k, "-", " "))
			fmt.Printf("%v:\n", title)
			for _, t := range flat[k] {
				fmt.Printf("  - %v\n", t)
			}
		}
	case "json":
		flat := schema.flattened()
		if len(ofTypes) > 0 {
			for k := range flat {
				if _, exists := ofTypes[k]; !exists {
					delete(flat, k)
				}
			}
		}
		jsonBytes, err := json.Marshal(flat)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(jsonBytes))
	case "json-full":
		jsonBytes, err := json.Marshal(schema)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(jsonBytes))
	}
}
