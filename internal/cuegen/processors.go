package cuegen

import (
	"fmt"

	"cuelang.org/go/cue/ast"
	"cuelang.org/go/cue/token"
	"github.com/benthosdev/benthos/v4/internal/config/schema"
)

func doProcessors(sch schema.Full) ([]ast.Decl, error) {
	fields := make([]interface{}, 0, len(sch.Processors))

	for _, v := range sch.Processors {
		field, err := doComponent(v)
		if err != nil {
			return nil, fmt.Errorf("failed to generate cue type for component: %s: %w", v.Name, err)
		}
		fields = append(fields, field)
	}

	decls := []ast.Decl{
		&ast.Field{
			Label: identProcessorDisjunction,
			Value: ast.NewCall(ast.NewIdent("or"), ast.NewList(&ast.Comprehension{
				Clauses: []ast.Clause{
					&ast.ForClause{
						Key:    ast.NewIdent("name"),
						Value:  ast.NewIdent("config"),
						Source: identProcessorCollection,
					},
				},
				Value: ast.NewStruct(&ast.Field{
					Label: interpolateIdent(ast.NewIdent("name")),
					Value: ast.NewIdent("config"),
				}),
			})),
		},
		&ast.Field{
			Label: identProcessorDisjunction,
			Value: ast.NewBinExpr(
				token.AND,
				identProcessorDisjunction,
				ast.NewStruct(
					ast.NewIdent("label"),
					token.OPTION,
					ast.NewIdent("string"),
				),
			),
		},
		&ast.Field{
			Label: identProcessorCollection,
			Value: ast.NewStruct(fields...),
		},
	}

	return decls, nil
}
