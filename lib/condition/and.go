package condition

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAnd] = TypeSpec{
		constructor: NewAnd,
		Description: `
And is a condition that returns the logical AND of its children conditions:

` + "``` yaml" + `
# True if message contains both 'foo' and 'bar'
and:
  - text:
      operator: contains
      arg: foo
  - text:
      operator: contains
      arg: bar
` + "```" + ``,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var err error
			condConfs := make([]interface{}, len(conf.And))
			for i, cConf := range conf.And {
				if condConfs[i], err = SanitiseConfig(cConf); err != nil {
					return nil, err
				}
			}
			return condConfs, nil
		},
	}
}

//------------------------------------------------------------------------------

// AndConfig is a configuration struct containing fields for the And condition.
type AndConfig []Config

// NewAndConfig returns a AndConfig with default values.
func NewAndConfig() AndConfig {
	return AndConfig{}
}

//------------------------------------------------------------------------------

// And is a condition that returns the logical AND of all children.
type And struct {
	children []Type

	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewAnd returns an And condition.
func NewAnd(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	children := []Type{}
	for i, childConf := range conf.And {
		ns := fmt.Sprintf("%v", i)
		child, err := New(childConf, mgr, log.NewModule("."+ns), metrics.Namespaced(stats, ns))
		if err != nil {
			return nil, fmt.Errorf("failed to create child '%v': %v", childConf.Type, err)
		}
		children = append(children, child)
	}
	return &And{
		children: children,

		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *And) Check(msg types.Message) bool {
	c.mCount.Incr(1)
	for _, child := range c.children {
		if !child.Check(msg) {
			c.mFalse.Incr(1)
			return false
		}
	}
	c.mTrue.Incr(1)
	return true
}

//------------------------------------------------------------------------------
