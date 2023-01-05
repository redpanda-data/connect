package cuegen

import (
	"fmt"

	"cuelang.org/go/cue/ast"
	"cuelang.org/go/cue/token"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

type componentOptions struct {
	collectionIdent  *ast.Ident
	disjunctionIdent *ast.Ident

	canLabel      bool
	canPreProcess bool
}

func doComponents(specs []docs.ComponentSpec, opts *componentOptions) ([]ast.Decl, error) {
	fields := make([]any, 0, len(specs))

	for _, v := range specs {
		field, err := doComponentSpec(v)
		if err != nil {
			return nil, fmt.Errorf("failed to generate cue type for component: %s: %w", v.Name, err)
		}
		fields = append(fields, field)
	}

	var addons []any
	if opts.canLabel {
		addons = append(
			addons,
			ast.NewIdent("label"),
			token.OPTION,
			ast.NewIdent("string"),
		)
	}

	if opts.canPreProcess {
		addons = append(
			addons,
			ast.NewIdent("processors"),
			token.OPTION,
			ast.NewList(&ast.Ellipsis{Type: ast.NewIdent("#Processor")}),
		)
	}

	decls := []ast.Decl{
		&ast.Field{
			Label: opts.collectionIdent,
			Value: ast.NewStruct(fields...),
		},
		&ast.Field{
			Label: opts.disjunctionIdent,
			Value: ast.NewCall(ast.NewIdent("or"), ast.NewList(&ast.Comprehension{
				Clauses: []ast.Clause{
					&ast.ForClause{
						Key:    ast.NewIdent("name"),
						Value:  ast.NewIdent("config"),
						Source: opts.collectionIdent,
					},
				},
				Value: ast.NewStruct(&ast.Field{
					Label: interpolateIdent(ast.NewIdent("name")),
					Value: ast.NewIdent("config"),
				}),
			})),
		},
	}

	if len(addons) > 0 {
		decls = append(decls, &ast.Field{
			Label: opts.disjunctionIdent,
			Value: ast.NewBinExpr(
				token.AND,
				opts.disjunctionIdent,
				ast.NewStruct(
					ast.NewIdent("processors"),
					token.OPTION,
					ast.NewList(&ast.Ellipsis{Type: ast.NewIdent("#Processor")}),

					ast.NewIdent("label"),
					token.OPTION,
					ast.NewIdent("string"),
				),
			),
		})
	}

	return decls, nil
}
