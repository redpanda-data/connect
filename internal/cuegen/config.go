package cuegen

import (
	"cuelang.org/go/cue/ast"
	"cuelang.org/go/cue/token"
)

func doConfig() ([]ast.Decl, error) {
	var members []interface{}
	members = append(members, resourcesFields...)

	members = append(members, []interface{}{
		// TODO: replace this with a proper generator from the shutdown_timeout
		// field spec
		identShutdownTimeout,
		token.OPTION,
		ast.NewIdent("string"),

		identHTTP,
		token.OPTION,
		identHTTPConf,

		identLogger,
		token.OPTION,
		identLoggerConf,

		identBuffer,
		token.OPTION,
		identBufferDisjunction,

		identMetric,
		token.OPTION,
		identMetricDisjunction,

		identTracer,
		token.OPTION,
		identTracerDisjunction,

		identInput,
		token.OPTION,
		identInputDisjunction,

		identPipeline,
		token.OPTION,
		ast.NewStruct(
			ast.NewIdent("threads"),
			token.OPTION,
			ast.NewIdent("int"),
			&ast.Field{
				Label: ast.NewIdent("processors"),
				Value: ast.NewList(&ast.Ellipsis{Type: identProcessorDisjunction}),
			},
		),

		identOutput,
		token.OPTION,
		identOutputDisjunction,

		ast.Embed(identTestSuite),
	}...)

	return []ast.Decl{
		&ast.Field{
			Label: identConfig,
			Value: ast.NewStruct(members...),
		},
	}, nil
}
