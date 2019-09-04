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
	"errors"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCheckField] = TypeSpec{
		constructor: NewCheckField,
		description: `
Extracts the value of a field identified via [dot path](../field_paths.md)
within messages (currently only JSON format is supported) and then tests the
extracted value against a child condition.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var condConf interface{} = struct{}{}
			if conf.CheckField.Condition != nil {
				var err error
				if condConf, err = SanitiseConfig(*conf.CheckField.Condition); err != nil {
					return nil, err
				}
			}
			return map[string]interface{}{
				"parts":     conf.CheckField.Parts,
				"path":      conf.CheckField.Path,
				"condition": condConf,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// CheckFieldConfig contains configuration fields for the CheckField condition.
type CheckFieldConfig struct {
	Parts     []int   `json:"parts" yaml:"parts"`
	Path      string  `json:"path" yaml:"path"`
	Condition *Config `json:"condition" yaml:"condition"`
}

// NewCheckFieldConfig returns a CheckFieldConfig with default values.
func NewCheckFieldConfig() CheckFieldConfig {
	return CheckFieldConfig{
		Parts:     []int{},
		Path:      "",
		Condition: nil,
	}
}

//------------------------------------------------------------------------------

type dummyCheckFieldConfig struct {
	Parts     []int       `json:"parts" yaml:"parts"`
	Path      string      `json:"path" yaml:"path"`
	Condition interface{} `json:"condition" yaml:"condition"`
}

// MarshalJSON prints an empty object instead of nil.
func (c CheckFieldConfig) MarshalJSON() ([]byte, error) {
	dummy := dummyCheckFieldConfig{
		Parts:     c.Parts,
		Path:      c.Path,
		Condition: c.Condition,
	}
	if c.Condition == nil {
		dummy.Condition = struct{}{}
	}
	return json.Marshal(dummy)
}

// MarshalYAML prints an empty object instead of nil.
func (c CheckFieldConfig) MarshalYAML() (interface{}, error) {
	dummy := dummyCheckFieldConfig{
		Parts:     c.Parts,
		Path:      c.Path,
		Condition: c.Condition,
	}
	if c.Condition == nil {
		dummy.Condition = struct{}{}
	}
	return dummy, nil
}

//------------------------------------------------------------------------------

// CheckField is a condition that extracts a field and checks the contents
// against a child condition.
type CheckField struct {
	conf  CheckFieldConfig
	log   log.Modular
	stats metrics.Type

	child Type
	path  []string

	mCount   metrics.StatCounter
	mTrue    metrics.StatCounter
	mFalse   metrics.StatCounter
	mErrJSON metrics.StatCounter
	mErr     metrics.StatCounter
}

// NewCheckField returns a CheckField condition.
func NewCheckField(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	if conf.CheckField.Condition == nil {
		return nil, errors.New("cannot create check_field condition without a child")
	}

	child, err := New(*conf.CheckField.Condition, mgr, log, stats)
	if err != nil {
		return nil, err
	}

	return &CheckField{
		conf:  conf.CheckField,
		log:   log,
		stats: stats,
		child: child,
		path:  strings.Split(conf.CheckField.Path, "."),

		mCount:   stats.GetCounter("count"),
		mTrue:    stats.GetCounter("true"),
		mFalse:   stats.GetCounter("false"),
		mErrJSON: stats.GetCounter("error_json_parse"),
		mErr:     stats.GetCounter("error"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition
func (c *CheckField) Check(msg types.Message) bool {
	c.mCount.Incr(1)

	payload := msg.Copy()

	proc := func(index int) {
		payload.Get(index).Set([]byte(""))

		jpart, err := msg.Get(index).JSON()
		if err != nil {
			c.log.Debugf("Failed to parse message as JSON: %v\n", err)
			c.mErrJSON.Incr(1)
			c.mErr.Incr(1)
			return
		}

		gpart := gabs.Wrap(jpart).S(c.path...)
		switch t := gpart.Data().(type) {
		case string:
			payload.Get(index).Set([]byte(t))
		default:
			payload.Get(index).SetJSON(gpart.Data())
		}
	}

	if len(c.conf.Parts) == 0 {
		for i := 0; i < payload.Len(); i++ {
			proc(i)
		}
	} else {
		for _, index := range c.conf.Parts {
			proc(index)
		}
	}

	res := c.child.Check(payload)
	if res {
		c.mTrue.Incr(1)
	} else {
		c.mFalse.Incr(1)
	}
	return res
}

//------------------------------------------------------------------------------
