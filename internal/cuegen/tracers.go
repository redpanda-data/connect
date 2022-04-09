package cuegen

import (
	"fmt"

	"cuelang.org/go/cue/ast"
	"github.com/benthosdev/benthos/v4/internal/config/schema"
)

func doTracers(sch schema.Full) ([]ast.Decl, error) {
	fields := make([]interface{}, 0, len(sch.Tracers))

	for _, v := range sch.Tracers {
		field, err := doComponent(v)
		if err != nil {
			return nil, fmt.Errorf("failed to generate cue type for component: %s: %w", v.Name, err)
		}
		fields = append(fields, field)
	}

	decls := []ast.Decl{
		&ast.Field{
			Label: identTracerDisjunction,
			Value: ast.NewCall(ast.NewIdent("or"), ast.NewList(&ast.Comprehension{
				Clauses: []ast.Clause{
					&ast.ForClause{
						Key:    ast.NewIdent("name"),
						Value:  ast.NewIdent("config"),
						Source: identTracerCollection,
					},
				},
				Value: ast.NewStruct(&ast.Field{
					Label: interpolateIdent(ast.NewIdent("name")),
					Value: ast.NewIdent("config"),
				}),
			})),
		},
		&ast.Field{
			Label: identTracerCollection,
			Value: ast.NewStruct(fields...),
		},
	}

	return decls, nil
}
