package cuegen

import (
	"cuelang.org/go/cue/ast"
	"cuelang.org/go/cue/token"
	"github.com/benthosdev/benthos/v4/internal/api"
)

func doHTTP() ([]ast.Decl, error) {
	specs := api.Spec()

	var fields []interface{}
	for _, fieldSpec := range specs {
		field, err := doFieldSpec(fieldSpec)
		if err != nil {
			return nil, err
		}
		if checkRequired(fieldSpec) {
			fields = append(fields, field)
		} else {
			fields = append(fields, field.Label, token.OPTION, field.Value)
		}
	}

	return []ast.Decl{
		&ast.Field{
			Label: identHTTPConf,
			Value: ast.NewStruct(fields...),
		},
	}, nil
}
