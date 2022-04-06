package cuegen

import (
	"cuelang.org/go/cue/ast"

	// Populating default environment in order to walk it and generate Cue types
	_ "github.com/benthosdev/benthos/v4/public/components/all"
)

// GenerateSchemaAST generates a Cue AST which includes Cue definitions that
// represent the configuration file format and component configs.
func GenerateSchemaAST() (ast.Node, error) {
	root := &ast.File{
		Decls: []ast.Decl{
			&ast.Package{
				Name: ast.NewIdent("benthos"),
			},
		},
	}

	configDecls, err := doConfig()
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, configDecls...)

	resourcesDecls, err := doResources()
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, resourcesDecls...)

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

	inputDecls, err := doInputs()
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, inputDecls...)

	outputDecls, err := doOutputs()
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, outputDecls...)

	processorDecls, err := doProcessors()
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, processorDecls...)

	bufferDecls, err := doBuffers()
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, bufferDecls...)

	metricDecls, err := doMetrics()
	if err != nil {
		return nil, err
	}
	root.Decls = append(root.Decls, metricDecls...)

	tracerDecls, err := doTracers()
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
