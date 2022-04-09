package cuegen

import (
	"cuelang.org/go/cue/ast"

	// Populating default environment in order to walk it and generate Cue types
	"github.com/benthosdev/benthos/v4/internal/config/schema"
	_ "github.com/benthosdev/benthos/v4/public/components/all"
)

// GenerateSchemaAST generates a Cue AST which includes Cue definitions that
// represent the configuration file format and component configs.
func GenerateSchemaAST(sch schema.Full) (ast.Node, error) {
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

	httpDecls, err := doHTTP()
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, httpDecls...)

	loggerDecls, err := doLoggers()
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, loggerDecls...)

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

	testDecls, err := doTests()
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, testDecls...)

	return root, nil
}
