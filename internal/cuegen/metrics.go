package cuegen

import (
	"fmt"

	"cuelang.org/go/cue/ast"
	"github.com/benthosdev/benthos/v4/internal/config/schema"
)

func doMetrics(sch schema.Full) ([]ast.Decl, error) {
	fields := make([]interface{}, 0, len(sch.Metrics))

	for _, v := range sch.Metrics {
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
