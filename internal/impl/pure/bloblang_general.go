package pure

import (
	"fmt"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/value"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func init() {
	maxUint := ^uint64(0)
	maxInt := maxUint >> 1

	if err := bloblang.RegisterAdvancedFunction("counter",
		bloblang.NewPluginSpec().
			Category(query.FunctionCategoryGeneral).
			Experimental().
			Description("Returns a non-negative integer that increments each time it is resolved, yielding the minimum (`1` by default) as the first value. Each instantiation of `counter` has its own independent count. Once the maximum integer (or `max` argument) is reached the counter resets back to the minimum.").
			Param(bloblang.NewQueryParam("min", true).
				Default(1).
				Description("The minimum value of the counter, this is the first value that will be yielded. If this parameter is dynamic it will be resolved only once during the lifetime of the mapping.")).
			Param(bloblang.NewQueryParam("max", true).
				Default(maxInt).
				Description("The maximum value of the counter, once this value is yielded the counter will reset back to the min. If this parameter is dynamic it will be resolved only once during the lifetime of the mapping.")).
			Param(bloblang.NewQueryParam("set", false).
				Optional().
				Description("An optional mapping that when specified will be executed each time the counter is resolved. When this mapping resolves to a non-negative integer value it will cause the counter to reset to this value and yield it. If this mapping is omitted or doesn't resolve to anything then the counter will increment and yield the value as normal. If this mapping resolves to `null` then the counter is not incremented and the current value is yielded. If this mapping resolves to a deletion then the counter is reset to the `min` value.")).
			Example("", `root.id = counter()`,
				[2]string{
					`{}`,
					`{"id":1}`,
				},
				[2]string{
					`{}`,
					`{"id":2}`,
				},
			).
			Example("It's possible to increment a counter multiple times within a single mapping invocation using a map.",
				`
map foos {
  root = counter()
}

root.meow_id = null.apply("foos")
root.woof_id = null.apply("foos")
`,
				[2]string{
					`{}`,
					`{"meow_id":1,"woof_id":2}`,
				},
				[2]string{
					`{}`,
					`{"meow_id":3,"woof_id":4}`,
				},
			).
			Example(
				"By specifying an optional `set` parameter it is possible to dynamically reset the counter based on input data.",
				`root.consecutive_doggos = counter(min: 1, set: if !this.sound.lowercase().contains("woof") { 0 })`,
				[2]string{
					`{"sound":"woof woof"}`,
					`{"consecutive_doggos":1}`,
				},
				[2]string{
					`{"sound":"woofer wooooo"}`,
					`{"consecutive_doggos":2}`,
				},
				[2]string{
					`{"sound":"meow"}`,
					`{"consecutive_doggos":0}`,
				},
				[2]string{
					`{"sound":"uuuuh uh uh woof uhhhhhh"}`,
					`{"consecutive_doggos":1}`,
				},
			).
			Example(
				"The `set` parameter can also be utilised to peek at the counter without mutating it by returning `null`.",
				`root.things = counter(set: if this.id == null { null })`,
				[2]string{`{"id":"a"}`, `{"things":1}`},
				[2]string{`{"id":"b"}`, `{"things":2}`},
				[2]string{`{"what":"just checking"}`, `{"things":2}`},
				[2]string{`{"id":"c"}`, `{"things":3}`},
			),
		func(args *bloblang.ParsedParams) (bloblang.AdvancedFunction, error) {
			minFunc, err := args.GetQuery("min")
			if err != nil {
				return nil, err
			}

			maxFunc, err := args.GetOptionalQuery("max")
			if err != nil {
				return nil, err
			}

			setFunc, err := args.GetOptionalQuery("set")
			if err != nil {
				return nil, err
			}

			var min, max int64
			var i *int64

			var mut sync.Mutex

			return func(ctx *bloblang.ExecContext) (any, error) {
				mut.Lock()
				defer mut.Unlock()

				if i == nil {
					var err error
					if min, err = ctx.ExecToInt64(minFunc); err != nil {
						return nil, fmt.Errorf("failed to resolve min argument: %w", err)
					}
					if min < 0 {
						return nil, fmt.Errorf("min argument must be >0, got %v", min)
					}
					if max, err = ctx.ExecToInt64(maxFunc); err != nil {
						return nil, fmt.Errorf("failed to resolve max argument: %w", err)
					}
					if max < 0 || max <= min {
						return nil, fmt.Errorf("max argument must be >0 and >min, got %v", max)
					}

					iV := min - 1
					i = &iV
				}

				if setFunc != nil {
					setV, err := ctx.Exec(setFunc)
					if err != nil {
						return nil, fmt.Errorf("failed to resolve set argument: %w", err)
					}
					if setV == nil {
						return *i, nil
					}
					switch setV.(type) {
					case bloblang.ExecResultDelete:
						*i = min - 1
					case bloblang.ExecResultNothing:
					default:
						iv, err := value.IGetInt(setV)
						if err != nil {
							return nil, fmt.Errorf("failed to resolve set argument: %w", err)
						}
						*i = iv
						return iv, nil
					}
				}

				*i++
				v := *i
				if v >= max {
					*i = min - 1
				}
				return v, nil
			}, nil
		}); err != nil {
		panic(err)
	}
}
