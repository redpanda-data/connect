package metrics

func init() {
	Constructors[TypeNone] = TypeSpec{
		constructor: func(config Config, opts ...func(Type)) (Type, error) {
			return Noop(), nil
		},
		Summary: `Disable metrics entirely.`,
	}
}
