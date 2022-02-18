package metrics

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
)

func init() {
	Constructors[TypeNone] = TypeSpec{
		constructor: func(Config, log.Modular) (Type, error) {
			return Noop(), nil
		},
		Summary: `Disable metrics entirely.`,
		config:  docs.FieldComponent().HasType(docs.FieldTypeObject),
	}
}
