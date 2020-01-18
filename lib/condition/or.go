package condition

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeOr] = TypeSpec{
		constructor: NewOr,
		Description: `
Or is a condition that returns the logical OR of its children conditions.

` + "``` yaml" + `
# True if message contains 'foo' or 'bar'
or:
  - text:
      operator: contains
      arg: foo
  - text:
      operator: contains
      arg: bar
` + "```" + ``,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var err error
			condConfs := make([]interface{}, len(conf.Or))
			for i, cConf := range conf.Or {
				if condConfs[i], err = SanitiseConfig(cConf); err != nil {
					return nil, err
				}
			}
			return condConfs, nil
		},
	}
}

//------------------------------------------------------------------------------

// OrConfig is a configuration struct containing fields for the Or condition.
type OrConfig []Config

// NewOrConfig returns a OrConfig with default values.
func NewOrConfig() OrConfig {
	return OrConfig{}
}

//------------------------------------------------------------------------------

// Or is a condition that returns the logical or of all children.
type Or struct {
	children []Type

	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewOr returns an Or condition.
func NewOr(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	children := []Type{}
	for i, childConf := range conf.Or {
		ns := fmt.Sprintf("%v", i)
		child, err := New(childConf, mgr, log.NewModule("."+ns), metrics.Namespaced(stats, ns))
		if err != nil {
			return nil, fmt.Errorf("failed to create child '%v': %v", childConf.Type, err)
		}
		children = append(children, child)
	}
	return &Or{
		children: children,

		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Or) Check(msg types.Message) bool {
	c.mCount.Incr(1)
	for _, child := range c.children {
		if child.Check(msg) {
			c.mTrue.Incr(1)
			return true
		}
	}
	c.mFalse.Incr(1)
	return false
}

//------------------------------------------------------------------------------
