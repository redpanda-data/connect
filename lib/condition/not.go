package condition

import (
	"encoding/json"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNot] = TypeSpec{
		constructor: NewNot,
		Description: `
Not is a condition that returns the opposite (NOT) of its child condition. The
body of a not object is the child condition, i.e. in order to express 'part 0
NOT equal to "foo"' you could have the following YAML config:

` + "``` yaml" + `
not:
  text:
    operator: equal
    part: 0
    arg: foo
` + "```" + `

Or, the same example as JSON:

` + "``` json" + `
{
	"type": "not",
	"not": {
		"type": "text",
		"text": {
			"operator": "equal",
			"part": 0,
			"arg": "foo"
		}
	}
}
` + "```",
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			if conf.Not.Config == nil {
				return struct{}{}, nil
			}
			return SanitiseConfig(*conf.Not.Config)
		},
	}
}

//------------------------------------------------------------------------------

// NotConfig is a configuration struct containing fields for the Not condition.
type NotConfig struct {
	*Config `yaml:",inline" json:",inline"`
}

// NewNotConfig returns a NotConfig with default values.
func NewNotConfig() NotConfig {
	return NotConfig{
		Config: nil,
	}
}

//------------------------------------------------------------------------------

// MarshalJSON prints an empty object instead of nil.
func (m NotConfig) MarshalJSON() ([]byte, error) {
	if m.Config != nil {
		return json.Marshal(m.Config)
	}
	return json.Marshal(struct{}{})
}

// MarshalYAML prints an empty object instead of nil.
func (m NotConfig) MarshalYAML() (interface{}, error) {
	if m.Config != nil {
		return *m.Config, nil
	}
	return struct{}{}, nil
}

//------------------------------------------------------------------------------

// UnmarshalJSON ensures that when parsing child config it is initialised.
func (m *NotConfig) UnmarshalJSON(bytes []byte) error {
	if m.Config == nil {
		nConf := NewConfig()
		m.Config = &nConf
	}

	return json.Unmarshal(bytes, m.Config)
}

// UnmarshalYAML ensures that when parsing child config it is initialised.
func (m *NotConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if m.Config == nil {
		nConf := NewConfig()
		m.Config = &nConf
	}

	return unmarshal(m.Config)
}

//------------------------------------------------------------------------------

// Not is a condition that returns the opposite of a child condition.
type Not struct {
	child Type

	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewNot returns a Not condition.
func NewNot(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	childConf := conf.Not.Config
	if childConf == nil {
		newConf := NewConfig()
		childConf = &newConf
	}
	child, err := New(*childConf, mgr, log.NewModule(".not"), metrics.Namespaced(stats, "not"))
	if err != nil {
		return nil, err
	}
	return &Not{
		child: child,

		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Not) Check(msg types.Message) bool {
	c.mCount.Incr(1)
	res := !c.child.Check(msg)
	if res {
		c.mTrue.Incr(1)
	} else {
		c.mFalse.Incr(1)
	}
	return res
}

//------------------------------------------------------------------------------
