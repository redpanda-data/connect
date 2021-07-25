package bloblang

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/Jeffail/benthos/v3/internal/bloblang/plugins"
)

func init() {
	if err := plugins.Register(); err != nil {
		panic(err)
	}
}

// NewField attempts to parse and create a dynamic field expression from a
// string. If the expression is invalid an error is returned.
//
// When a parsing error occurs the returned error will be a *parser.Error type,
// which allows you to gain positional and structured error messages.
func NewField(expr string) (*field.Expression, error) {
	return NewFieldWithContext(parser.GlobalContext(), expr)
}

// NewFieldWithContext attempts to parse and create a dynamic field expression
// from a string using a provided context to determine available functions,
// methods and features. If the expression is invalid an error is returned.
//
// When a parsing error occurs the returned error will be a *parser.Error type,
// which allows you to gain positional and structured error messages.
func NewFieldWithContext(ctx parser.Context, expr string) (*field.Expression, error) {
	e, err := parser.ParseField(ctx, expr)
	if err != nil {
		return e, err
	}
	return e, nil
}

// NewMapping attempts to parse and create a Bloblang mapping from a string. If
// the mapping was read from a file the path should be provided in order to
// resolve relative imports, otherwise the path can be left empty.
//
// When a parsing error occurs the returned error may be a *parser.Error type,
// which allows you to gain positional and structured error messages.
func NewMapping(path, expr string) (*mapping.Executor, error) {
	return NewMappingWithContext(parser.GlobalContext(), path, expr)
}

// NewMappingWithContext attempts to parse and create a Bloblang mapping from a
// string using a specific context for determining available functions, methods
// and features. If the mapping was read from a file the path should be provided
// in order to resolve relative imports, otherwise the path can be left empty.
//
// When a parsing error occurs the returned error may be a *parser.Error type,
// which allows you to gain positional and structured error messages.
func NewMappingWithContext(ctx parser.Context, path, expr string) (*mapping.Executor, error) {
	e, err := parser.ParseMapping(ctx, path, expr)
	if err != nil {
		return nil, err
	}
	return e, nil
}
