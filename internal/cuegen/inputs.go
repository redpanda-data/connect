package cuegen

import (
	"fmt"

	"cuelang.org/go/cue/ast"
	"cuelang.org/go/cue/token"
	"github.com/benthosdev/benthos/v4/internal/bundle"
)

func doInputs() ([]ast.Decl, error) {
	specs := bundle.AllInputs.Docs()

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
			Label: identInputDisjunction,
			Value: ast.NewCall(ast.NewIdent("or"), ast.NewList(&ast.Comprehension{
				Clauses: []ast.Clause{
					&ast.ForClause{
						Key:    ast.NewIdent("name"),
						Value:  ast.NewIdent("config"),
						Source: identInputCollection,
					},
				},
				Value: ast.NewStruct(&ast.Field{
					Label: interpolateIdent(ast.NewIdent("name")),
					Value: ast.NewIdent("config"),
				}),
			})),
		},
		&ast.Field{
			Label: identInputDisjunction,
			Value: ast.NewBinExpr(
				token.AND,
				identInputDisjunction,
				ast.NewStruct(
					ast.NewIdent("processors"),
					token.OPTION,
					ast.NewList(&ast.Ellipsis{Type: ast.NewIdent("#Processor")}),

					ast.NewIdent("label"),
					token.OPTION,
					ast.NewIdent("string"),
				),
			),
		},
		&ast.Field{
			Label: identInputCollection,
			Value: ast.NewStruct(fields...),
		},
	}

	return decls, nil
}
