package cuegen

import (
	"cuelang.org/go/cue/ast"
	"cuelang.org/go/cue/format"

	// Populating default environment in order to walk it and generate Cue types.
	"github.com/benthosdev/benthos/v4/internal/config/schema"
)

// GenerateSchema generates a Cue schema which includes definitions for the
// configuration file structure and component configs.
func GenerateSchema(sch schema.Full) ([]byte, error) {
	root := &ast.File{
		Decls: []ast.Decl{
			&ast.Package{
				Name: ast.NewIdent("benthos"),
			},
		},
	}

	configDecls, err := doConfig(sch)
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, configDecls...)

	inputDecls, err := doComponents(
		sch.Inputs,
		&componentOptions{
			collectionIdent:  identInputCollection,
			disjunctionIdent: identInputDisjunction,
			canLabel:         true,
			canPreProcess:    true,
		},
	)
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, inputDecls...)

	outputDecls, err := doComponents(
		sch.Outputs,
		&componentOptions{
			collectionIdent:  identOutputCollection,
			disjunctionIdent: identOutputDisjunction,
			canLabel:         true,
			canPreProcess:    true,
		},
	)
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, outputDecls...)

	processorDecls, err := doComponents(
		sch.Processors,
		&componentOptions{
			collectionIdent:  identProcessorCollection,
			disjunctionIdent: identProcessorDisjunction,
			canLabel:         true,
		},
	)
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, processorDecls...)

	cacheDecls, err := doComponents(
		sch.Caches,
		&componentOptions{
			collectionIdent:  identCacheCollection,
			disjunctionIdent: identCacheDisjunction,
		},
	)
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, cacheDecls...)

	rateLimitDecls, err := doComponents(
		sch.RateLimits,
		&componentOptions{
			collectionIdent:  identRateLimitCollection,
			disjunctionIdent: identRateLimitDisjunction,
		},
	)
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, rateLimitDecls...)

	bufferDecls, err := doComponents(
		sch.Buffers,
		&componentOptions{
			collectionIdent:  identBufferCollection,
			disjunctionIdent: identBufferDisjunction,
		},
	)
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, bufferDecls...)

	metricDecls, err := doComponents(
		sch.Metrics,
		&componentOptions{
			collectionIdent:  identMetricCollection,
			disjunctionIdent: identMetricDisjunction,
		},
	)
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, metricDecls...)

	tracerDecls, err := doComponents(
		sch.Tracers,
		&componentOptions{
			collectionIdent:  identTracerCollection,
			disjunctionIdent: identTracerDisjunction,
		},
	)
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, tracerDecls...)

	return format.Node(root)
}
