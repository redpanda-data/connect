package cuegen

import (
	"fmt"

	"cuelang.org/go/cue/ast"
	"github.com/benthosdev/benthos/v4/internal/bundle"
)

func doMetrics() ([]ast.Decl, error) {
	specs := bundle.AllMetrics.Docs()

	fields := make([]interface{}, 0, len(specs))

	for _, v := range specs {
		field, err := doComponent(v)
		if err != nil {
			return nil, fmt.Errorf("failed to generate cue type for component: %s: %w", v.Name, err)
		}
		fields = append(fields, field)
	}

	decls := []ast.Decl{
		&ast.Field{
			Label: identMetricDisjunction,
			Value: ast.NewCall(ast.NewIdent("or"), ast.NewList(&ast.Comprehension{
				Clauses: []ast.Clause{
					&ast.ForClause{
						Key:    ast.NewIdent("name"),
						Value:  ast.NewIdent("config"),
						Source: identMetricCollection,
					},
				},
				Value: ast.NewStruct(&ast.Field{
					Label: interpolateIdent(ast.NewIdent("name")),
					Value: ast.NewIdent("config"),
				}),
			})),
		},
		&ast.Field{
			Label: identMetricCollection,
			Value: ast.NewStruct(fields...),
		},
	}

	return decls, nil
}
