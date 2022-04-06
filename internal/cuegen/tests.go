package cuegen

import (
	"cuelang.org/go/cue/ast"
	"cuelang.org/go/cue/token"
	tdocs "github.com/benthosdev/benthos/v4/internal/cli/test/docs"
)

func doTests() ([]ast.Decl, error) {
	spec := tdocs.ConfigSpec()
	field, err := doFieldSpec(spec)
	if err != nil {
		return nil, err
	}

	return []ast.Decl{
		&ast.Field{
			Label: identTestSuite,
			Value: ast.NewStruct(field.Label, token.OPTION, field.Value),
		},
	}, nil
}
