package condition

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeXor] = TypeSpec{
		constructor: NewXor,
		Description: `
Returns the logical XOR of its children conditions, meaning it only resolves to
true if _exactly_ one of its children conditions resolves to true.

` + "``` yaml" + `
# True if message contains 'foo' or 'bar', but not both
xor:
  - text:
      operator: contains
      arg: foo
  - text:
      operator: contains
      arg: bar
` + "```" + ``,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var err error
			condConfs := make([]interface{}, len(conf.Xor))
			for i, cConf := range conf.Xor {
				if condConfs[i], err = SanitiseConfig(cConf); err != nil {
					return nil, err
				}
			}
			return condConfs, nil
		},
	}
}

//------------------------------------------------------------------------------

// XorConfig is a configuration struct containing fields for the Xor condition.
type XorConfig []Config

// NewXorConfig returns a XorConfig with default values.
func NewXorConfig() XorConfig {
	return XorConfig{}
}

//------------------------------------------------------------------------------

// Xor is a condition that returns the logical xor of all children.
type Xor struct {
	children []Type

	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewXor returns an Xor condition.
func NewXor(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	children := []Type{}
	for i, childConf := range conf.Xor {
		ns := fmt.Sprintf("%v", i)
		child, err := New(childConf, mgr, log.NewModule("."+ns), metrics.Namespaced(stats, ns))
		if err != nil {
			return nil, fmt.Errorf("failed to create child '%v': %v", childConf.Type, err)
		}
		children = append(children, child)
	}
	return &Xor{
		children: children,

		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Xor) Check(msg types.Message) bool {
	c.mCount.Incr(1)
	hadTrue := false
	for _, child := range c.children {
		if child.Check(msg) {
			if hadTrue {
				c.mFalse.Incr(1)
				return false
			}
			hadTrue = true
		}
	}
	if hadTrue {
		c.mTrue.Incr(1)
	} else {
		c.mFalse.Incr(1)
	}
	return hadTrue
}

//------------------------------------------------------------------------------
