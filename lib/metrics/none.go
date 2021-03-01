package metrics

import "github.com/Jeffail/benthos/v3/internal/docs"

func init() {
	Constructors[TypeNone] = TypeSpec{
		constructor: func(config Config, opts ...func(Type)) (Type, error) {
			return Noop(), nil
		},
		Summary: `Disable metrics entirely.`,
		config:  docs.FieldComponent().Map(),
	}
}
