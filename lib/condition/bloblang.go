package condition

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/bloblang/x/query"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"golang.org/x/xerrors"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeBloblang] = TypeSpec{
		constructor: NewBloblang,
		Description: `
BETA: This a beta component and therefore subject to change outside of major
version releases. Consult the changelog for changes before upgrading.

Executes a [Bloblang](/docs/guides/bloblang/about) query on messages, expecting
a boolean result. If the result of the query is true then the condition passes,
otherwise it does not.`,
		Footnotes: `
## Examples

With the following config:

` + "``` yaml" + `
bloblang: 'a == "foo"'
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
	fn query.Function

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
	res := query.Parse([]rune(conf.Bloblang))
	if res.Err != nil {
		return nil, xerrors.Errorf("failed to parse query: %w", res.Err)
	}
	if len(res.Remaining) > 0 {
		return nil, fmt.Errorf("unexpected contents at end of query: %v", string(res.Remaining))
	}

	return &Bloblang{
		fn: res.Result.(query.Function),

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
	if jObj, err := msg.Get(0).JSON(); err == nil {
		valuePtr = &jObj
	}

	result, err := c.fn.Exec(query.FunctionContext{
		Value: valuePtr,
		Maps:  map[string]query.Function{},
		Vars:  map[string]interface{}{},
		Msg:   msg,
	})
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
