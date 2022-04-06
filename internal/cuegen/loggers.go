package cuegen

import (
	"cuelang.org/go/cue/ast"
	"cuelang.org/go/cue/token"
	"github.com/benthosdev/benthos/v4/internal/log"
)

func doLoggers() ([]ast.Decl, error) {
	specs := log.Spec()

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
			Label: identLoggerConf,
			Value: ast.NewStruct(fields...),
		},
	}, nil
}
