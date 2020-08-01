package condition

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeNumber] = TypeSpec{
		constructor: NewNumber,
		Deprecated:  true,
		Footnotes: `
## Alternatives

Consider using the [bloblang](/docs/components/conditions/bloblang) condition
instead as it offers a wide range of number processing options. For example, the
following condition:

` + "``` yaml" + `
check_interpolation:
  value: ${!batch_size()}
  condition:
    number:
      operator: greater_than
      arg: 1
` + "```" + `

Can instead be expressed with:

` + "``` yaml" + `
bloblang: 'batch_size() > 1'
` + "```" + ``,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("operator", "An [operator](#operators) to apply."),
			docs.FieldCommon("arg", "An argument to check against. For some operators this field not be required."),
			partFieldSpec,
		},
	}
}

//------------------------------------------------------------------------------

// Errors for the number condition.
var (
	ErrInvalidNumberOperator = errors.New("invalid number operator type")
)

// NumberConfig is a configuration struct containing fields for the number
// condition.
type NumberConfig struct {
	Operator string  `json:"operator" yaml:"operator"`
	Part     int     `json:"part" yaml:"part"`
	Arg      float64 `json:"arg" yaml:"arg"`
}

// NewNumberConfig returns a NumberConfig with default values.
func NewNumberConfig() NumberConfig {
	return NumberConfig{
		Operator: "equals",
		Part:     0,
		Arg:      0,
	}
}

//------------------------------------------------------------------------------

type numberOperator func(c float64) bool

func numberEqualsOperator(arg float64) numberOperator {
	return func(c float64) bool {
		return arg == c
	}
}

func numberGreaterThanOperator(arg float64) numberOperator {
	return func(c float64) bool {
		return c > arg
	}
}

func numberLessThanOperator(arg float64) numberOperator {
	return func(c float64) bool {
		return c < arg
	}
}

func strToNumberOperator(str string, arg float64) (numberOperator, error) {
	switch str {
	case "equals":
		return numberEqualsOperator(arg), nil
	case "greater_than":
		return numberGreaterThanOperator(arg), nil
	case "less_than":
		return numberLessThanOperator(arg), nil
	}
	return nil, ErrInvalidNumberOperator
}

//------------------------------------------------------------------------------

// Number is a condition that checks messages interpretted as numbers against
// logical operators.
type Number struct {
	stats    metrics.Type
	operator numberOperator
	part     int

	log    log.Modular
	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewNumber returns a number condition.
func NewNumber(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	op, err := strToNumberOperator(conf.Number.Operator, conf.Number.Arg)
	if err != nil {
		return nil, fmt.Errorf("operator '%v': %v", conf.Number.Operator, err)
	}
	return &Number{
		stats:    stats,
		operator: op,
		part:     conf.Number.Part,

		log:    log,
		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Number) Check(msg types.Message) bool {
	c.mCount.Incr(1)
	index := c.part
	lParts := msg.Len()
	if lParts == 0 {
		c.mFalse.Incr(1)
		return false
	}

	msgPart := msg.Get(index).Get()
	if msgPart == nil {
		c.mFalse.Incr(1)
		return false
	}

	floatVal, err := strconv.ParseFloat(string(msgPart), 64)
	if err != nil {
		c.log.Debugf("Failed to parse message as number: %v\n", err)
		c.mFalse.Incr(1)
		return false
	}

	res := c.operator(floatVal)
	if res {
		c.mTrue.Incr(1)
	} else {
		c.mFalse.Incr(1)
	}
	return res
}

//------------------------------------------------------------------------------
