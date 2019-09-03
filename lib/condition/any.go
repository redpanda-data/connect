// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package condition

import (
	"encoding/json"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAny] = TypeSpec{
		constructor: NewAny,
		description: `
Any is a condition that tests a child condition against each message of a batch
individually. If any message passes the child condition then this condition also
passes.

For example, if we wanted to check that at least one message of a batch contains
the word 'foo' we could use this config:

` + "``` yaml" + `
any:
  text:
    operator: contains
    arg: foo
` + "```" + ``,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			if conf.Any.Config == nil {
				return struct{}{}, nil
			}
			return SanitiseConfig(*conf.Any.Config)
		},
	}
}

//------------------------------------------------------------------------------

// AnyConfig is a configuration struct containing fields for the Any condition.
type AnyConfig struct {
	*Config `yaml:",inline" json:",inline"`
}

// NewAnyConfig returns a AnyConfig with default values.
func NewAnyConfig() AnyConfig {
	return AnyConfig{
		Config: nil,
	}
}

// MarshalJSON prints an empty object instead of nil.
func (m AnyConfig) MarshalJSON() ([]byte, error) {
	if m.Config != nil {
		return json.Marshal(m.Config)
	}
	return json.Marshal(struct{}{})
}

// MarshalYAML prints an empty object instead of nil.
func (m AnyConfig) MarshalYAML() (interface{}, error) {
	if m.Config != nil {
		return *m.Config, nil
	}
	return struct{}{}, nil
}

// UnmarshalJSON ensures that when parsing child config it is initialised.
func (m *AnyConfig) UnmarshalJSON(bytes []byte) error {
	if m.Config == nil {
		nConf := NewConfig()
		m.Config = &nConf
	}

	return json.Unmarshal(bytes, m.Config)
}

// UnmarshalYAML ensures that when parsing child config it is initialised.
func (m *AnyConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	if m.Config == nil {
		nConf := NewConfig()
		m.Config = &nConf
	}

	return unmarshal(m.Config)
}

//------------------------------------------------------------------------------

// Any is a condition that returns the logical or of all children.
type Any struct {
	child Type

	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewAny returns an Any condition.
func NewAny(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	childConf := conf.Any.Config
	if childConf == nil {
		newConf := NewConfig()
		childConf = &newConf
	}
	child, err := New(*childConf, mgr, log.NewModule(".any"), metrics.Namespaced(stats, "any"))
	if err != nil {
		return nil, err
	}
	return &Any{
		child: child,

		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Any) Check(msg types.Message) bool {
	c.mCount.Incr(1)
	for i := 0; i < msg.Len(); i++ {
		if c.child.Check(message.Lock(msg, i)) {
			c.mTrue.Incr(1)
			return true
		}
	}
	c.mFalse.Incr(1)
	return false
}

//------------------------------------------------------------------------------
