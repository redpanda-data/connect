package condition

import (
	"encoding/json"
	"errors"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCheckInterpolation] = TypeSpec{
		constructor: NewCheckInterpolation,
		Summary: `
Resolves a string containing
[function interpolations](/docs/configuration/interpolation#functions) and then tests
the result against a child condition.`,
		Description: `
For example, you could use this to test against the size of a message batch:

` + "``` yaml" + `
check_interpolation:
  value: ${!batch_size}
  condition:
    number:
      operator: greater_than
      arg: 1
` + "```" + ``,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			var condConf interface{} = struct{}{}
			if conf.CheckInterpolation.Condition != nil {
				var err error
				if condConf, err = SanitiseConfig(*conf.CheckInterpolation.Condition); err != nil {
					return nil, err
				}
			}
			return map[string]interface{}{
				"value":     conf.CheckInterpolation.Value,
				"condition": condConf,
			}, nil
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"value", "The value to check against the child condition.",
				"${!json_field:doc.title}",
				"${!metadata:kafka_topic}",
				"${!json_field:doc.id}-${!metadata:kafka_key}",
			).SupportsInterpolation(true),
			docs.FieldCommon("condition", "A child condition to test the field contents against."),
		},
	}
}

//------------------------------------------------------------------------------

// CheckInterpolationConfig contains configuration fields for the CheckInterpolation condition.
type CheckInterpolationConfig struct {
	Value     string  `json:"value" yaml:"value"`
	Condition *Config `json:"condition" yaml:"condition"`
}

// NewCheckInterpolationConfig returns a CheckInterpolationConfig with default values.
func NewCheckInterpolationConfig() CheckInterpolationConfig {
	return CheckInterpolationConfig{
		Value:     "",
		Condition: nil,
	}
}

//------------------------------------------------------------------------------

type dummyCheckInterpolationConfig struct {
	Value     string      `json:"value" yaml:"value"`
	Condition interface{} `json:"condition" yaml:"condition"`
}

// MarshalJSON prints an empty object instead of nil.
func (c CheckInterpolationConfig) MarshalJSON() ([]byte, error) {
	dummy := dummyCheckInterpolationConfig{
		Value:     c.Value,
		Condition: c.Condition,
	}
	if c.Condition == nil {
		dummy.Condition = struct{}{}
	}
	return json.Marshal(dummy)
}

// MarshalYAML prints an empty object instead of nil.
func (c CheckInterpolationConfig) MarshalYAML() (interface{}, error) {
	dummy := dummyCheckInterpolationConfig{
		Value:     c.Value,
		Condition: c.Condition,
	}
	if c.Condition == nil {
		dummy.Condition = struct{}{}
	}
	return dummy, nil
}

//------------------------------------------------------------------------------

// CheckInterpolation is a condition that resolves an interpolated string field
// and checks the contents against a child condition.
type CheckInterpolation struct {
	conf  CheckInterpolationConfig
	log   log.Modular
	stats metrics.Type

	child Type
	value *text.InterpolatedBytes

	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewCheckInterpolation returns a CheckInterpolation condition.
func NewCheckInterpolation(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	if conf.CheckInterpolation.Condition == nil {
		return nil, errors.New("cannot create check_interpolation condition without a child")
	}

	child, err := New(*conf.CheckInterpolation.Condition, mgr, log, stats)
	if err != nil {
		return nil, err
	}

	return &CheckInterpolation{
		conf:  conf.CheckInterpolation,
		log:   log,
		stats: stats,
		child: child,
		value: text.NewInterpolatedBytes([]byte(conf.CheckInterpolation.Value)),

		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition
func (c *CheckInterpolation) Check(msg types.Message) bool {
	c.mCount.Incr(1)

	payload := message.New(nil)
	payload.Append(msg.Get(0).Copy().Set(c.value.Get(msg)))

	res := c.child.Check(payload)
	if res {
		c.mTrue.Incr(1)
	} else {
		c.mFalse.Incr(1)
	}
	return res
}

//------------------------------------------------------------------------------
