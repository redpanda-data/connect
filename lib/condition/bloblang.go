package condition

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeBloblang] = TypeSpec{
		constructor: NewBloblang,
		Summary: `
Executes a [Bloblang](/docs/guides/bloblang/about) query on messages, expecting
a boolean result. If the result of the query is true then the condition passes,
otherwise it does not.`,
		Footnotes: `
## Examples

With the following config:

` + "``` yaml" + `
bloblang: a == "foo"
` + "```" + `

A message ` + "`" + `{"a":"foo"}` + "`" + ` would pass, but
` + "`" + `{"a":"bar"}` + "`" + ` would not.`,
	}
}

//------------------------------------------------------------------------------

// BloblangConfig is a configuration struct containing fields for the bloblang
// condition.
type BloblangConfig string

// NewBloblangConfig returns a BloblangConfig with default values.
func NewBloblangConfig() BloblangConfig {
	return ""
}

//------------------------------------------------------------------------------

// Bloblang is a condition that checks message against a Bloblang query.
type Bloblang struct {
	fn *mapping.Executor

	log log.Modular

	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
	mErr   metrics.StatCounter
}

// NewBloblang returns a Bloblang condition.
func NewBloblang(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	fn, err := bloblang.NewMapping(string(conf.Bloblang))
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			return nil, fmt.Errorf("%v", perr.ErrorAtPosition([]rune(conf.Bloblang)))
		}
		return nil, err
	}

	return &Bloblang{
		fn:  fn,
		log: log,

		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
		mErr:   stats.GetCounter("error"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Bloblang) Check(msg types.Message) bool {
	c.mCount.Incr(1)

	var valuePtr *interface{}
	var parseErr error

	lazyValue := func() *interface{} {
		if valuePtr == nil && parseErr == nil {
			if jObj, err := msg.Get(0).JSON(); err == nil {
				valuePtr = &jObj
			} else {
				parseErr = err
			}
		}
		return valuePtr
	}

	result, err := c.fn.Exec(query.FunctionContext{
		Maps:     map[string]query.Function{},
		Vars:     map[string]interface{}{},
		MsgBatch: msg,
	}.WithValueFunc(lazyValue))
	if err != nil {
		c.log.Errorf("Failed to check query: %v\n", err)
		c.mErr.Incr(1)
		c.mFalse.Incr(1)
		return false
	}

	resultBool, _ := result.(bool)
	if resultBool {
		c.mTrue.Incr(1)
	} else {
		c.mFalse.Incr(1)
	}
	return resultBool
}

//------------------------------------------------------------------------------
