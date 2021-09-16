package bloblang

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
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
	return GlobalEnvironment().NewField(expr)
}

// NewMapping attempts to parse and create a Bloblang mapping from a string. If
// the mapping was read from a file the path should be provided in order to
// resolve relative imports, otherwise the path can be left empty.
//
// When a parsing error occurs the returned error may be a *parser.Error type,
// which allows you to gain positional and structured error messages.
func NewMapping(expr string) (*mapping.Executor, error) {
	return GlobalEnvironment().NewMapping(expr)
}
