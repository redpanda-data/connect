package cuegen

import (
	"cuelang.org/go/cue/ast"
	"cuelang.org/go/cue/token"
)

var labelled = ast.NewStruct(&ast.Field{
	Label: ast.NewIdent("label"),
	Value: ast.NewIdent("string"),
})

var resourcesFields = []interface{}{
	identInputResources,
	token.OPTION,
	ast.NewList(&ast.Ellipsis{Type: ast.NewBinExpr(token.AND, identInputDisjunction, labelled)}),

	identProcessorResources,
	token.OPTION,
	ast.NewList(&ast.Ellipsis{Type: ast.NewBinExpr(token.AND, identProcessorDisjunction, labelled)}),

	identOutputResources,
	token.OPTION,
	ast.NewList(&ast.Ellipsis{Type: ast.NewBinExpr(token.AND, identOutputDisjunction, labelled)}),

	identCacheResources,
	token.OPTION,
	ast.NewList(&ast.Ellipsis{Type: identCacheDisjunction}),

	identRateLimitResources,
	token.OPTION,
	ast.NewList(&ast.Ellipsis{Type: identRateLimitDisjunction}),
}

func doResources() ([]ast.Decl, error) {
	cacheDecls, err := doCaches()
	if err != nil {
		return nil, err
	}

	rateLimitDecls, err := doRateLimits()
	if err != nil {
		return nil, err
	}

	var out []ast.Decl

	out = append(out, &ast.Field{
		Label: identResources,
		Value: ast.NewStruct(resourcesFields...),
	})
	out = append(out, cacheDecls...)
	out = append(out, rateLimitDecls...)

	return out, nil
}
