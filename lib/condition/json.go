package condition

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeJSON] = TypeSpec{
		constructor: NewJSON,
		Status:      docs.StatusDeprecated,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("operator", "A logical [operator](#operators) to check with.").HasOptions(
				"exists", "equals", "contains",
			),
			docs.FieldCommon("path", "The [path](/docs/configuration/field_paths) of a specific field within JSON documents to check."),
			docs.FieldCommon("arg", "An argument to check against. May not be applicable for all operators."),
			partFieldSpec,
		},
		Footnotes: `
## Alternatives

Consider using the [bloblang](/docs/components/conditions/bloblang) condition
instead as it offers a wide range of json processing options. For example, the
following condition:

` + "``` yaml" + `
json:
  operator: equals
  path: foo
  arg: bar
` + "```" + `

Can instead be expressed with:

` + "``` yaml" + `
bloblang: 'this.foo == "bar"'
` + "```" + ``,
	}
}

// JSONConfig is a configuration struct containing fields for the JSON
// condition.
type JSONConfig struct {
	Operator string      `json:"operator" yaml:"operator"`
	Part     int         `json:"part" yaml:"part"`
	Path     string      `json:"path" yaml:"path"`
	Arg      interface{} `json:"arg" yaml:"arg"`
}

// NewJSONConfig returns a JSONConfig with default values.
func NewJSONConfig() JSONConfig {
	return JSONConfig{
		Operator: "exists",
		Part:     0,
		Path:     "",
		Arg:      "",
	}
}

type jsonOperator func(c *gabs.Container) bool

func jsonExistOperator(path string) jsonOperator {
	return func(c *gabs.Container) bool {
		if path == "." || path == "" {
			return true
		}
		return c.ExistsP(path)
	}
}

func toFloat64(v interface{}) (float64, bool) {
	var argF float64
	switch t := v.(type) {
	case int:
		argF = float64(t)
	case int64:
		argF = float64(t)
	case float64:
		argF = float64(t)
	case json.Number:
		var err error
		if argF, err = t.Float64(); err != nil {
			argI, _ := t.Int64()
			argF = float64(argI)
		}
	default:
		return 0, false
	}
	return argF, true
}

func jsonContainsOperator(path string, arg interface{}) jsonOperator {
	argF, isNum := toFloat64(arg)
	if !isNum {
		return func(c *gabs.Container) bool {
			for _, child := range c.Path(path).Children() {
				if reflect.DeepEqual(child.Data(), arg) {
					return true
				}
			}
			return false
		}
	}
	return func(c *gabs.Container) bool {
		for _, child := range c.Path(path).Children() {
			if cF, isNum := toFloat64(child.Data()); isNum {
				if cF == argF {
					return true
				}
			}
		}
		return false
	}
}

func jsonEqualsOperator(path string, arg interface{}) jsonOperator {
	argF, isNum := toFloat64(arg)
	if !isNum {
		return func(c *gabs.Container) bool {
			return reflect.DeepEqual(c.Path(path).Data(), arg)
		}
	}
	return func(c *gabs.Container) bool {
		if cF, isNum := toFloat64(c.Path(path).Data()); isNum {
			return cF == argF
		}
		return false
	}
}

func strToJSONOperator(op, path string, arg interface{}) (jsonOperator, error) {
	switch op {
	case "exists":
		return jsonExistOperator(path), nil
	case "equals":
		return jsonEqualsOperator(path, arg), nil
	case "contains":
		return jsonContainsOperator(path, arg), nil
	}
	return nil, fmt.Errorf("unrecognised json operator: %v", op)
}

// JSON is a condition that checks JSON against a simple logic.
type JSON struct {
	stats    metrics.Type
	operator jsonOperator
	part     int

	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewJSON returns a JSON condition.
func NewJSON(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {

	op, err := strToJSONOperator(conf.JSON.Operator, conf.JSON.Path, conf.JSON.Arg)
	if err != nil {
		return nil, fmt.Errorf("operator '%v': %v", conf.JSON.Operator, err)
	}
	return &JSON{
		stats:    stats,
		operator: op,
		part:     conf.JSON.Part,

		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

// Check attempts to check a message part against a configured condition.
func (c *JSON) Check(msg types.Message) bool {
	c.mCount.Incr(1)
	index := c.part
	lParts := msg.Len()
	if lParts == 0 {
		c.mFalse.Incr(1)
		return false
	}

	msgPart, err := msg.Get(index).JSON()
	if err != nil {
		c.mFalse.Incr(1)
		return false
	}

	res := c.operator(gabs.Wrap(msgPart))
	if res {
		c.mTrue.Incr(1)
	} else {
		c.mFalse.Incr(1)
	}
	return res
}
