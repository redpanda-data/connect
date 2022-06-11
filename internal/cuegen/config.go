package cuegen

import (
	"cuelang.org/go/cue/ast"

	"github.com/benthosdev/benthos/v4/internal/config/schema"
)

func doConfig(sch schema.Full) ([]ast.Decl, error) {
	members, err := doFieldSpecs(sch.Config)
	if err != nil {
		return nil, err
	}

	return []ast.Decl{
		&ast.Field{
			Label: identConfig,
			Value: ast.NewStruct(members...),
		},
	}, nil
}
