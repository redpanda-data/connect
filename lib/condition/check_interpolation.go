package condition

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCheckInterpolation] = TypeSpec{
		constructor: NewCheckInterpolation,
		Status:      docs.StatusDeprecated,
		Footnotes: `
## Alternatives

Consider using the [bloblang](/docs/components/conditions/bloblang) condition
instead.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"value", "The value to check against the child condition.",
				`${! json("doc.title") }`,
				`${! meta("kafka_topic") }`,
				`${! json("doc.id") }-${! meta("kafka_key") }`,
			).IsInterpolated(),
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
	value field.Expression

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

	value, err := bloblang.NewField(conf.CheckInterpolation.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse interpolation value: %v", err)
	}

	return &CheckInterpolation{
		conf:  conf.CheckInterpolation,
		log:   log,
		stats: stats,
		child: child,
		value: value,

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
	payload.Append(msg.Get(0).Copy().Set(c.value.Bytes(0, msg)))

	res := c.child.Check(payload)
	if res {
		c.mTrue.Incr(1)
	} else {
		c.mFalse.Incr(1)
	}
	return res
}

//------------------------------------------------------------------------------
