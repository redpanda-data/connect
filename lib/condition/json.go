package condition

import (
	"errors"
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeJSON] = TypeSpec{
		constructor: NewJSON,
		Summary: `
Checks JSON against simple logic.`,
		Description: `
		TODO
			`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("operator", "The action to perform."),
			docs.FieldCommon("path", "The JSON path to perform action on."),
			docs.FieldCommon("arg", "A value that operator will be checking a path against."),
		},
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

type jsonOperator func(c []byte) bool

func jsonExistOperator(path string) jsonOperator {
	return func(c []byte) bool {
		jsonParsed, err := gabs.ParseJSON(c)
		if err != nil {
			return false
		}
		return jsonParsed.ExistsP(path)
	}
}

func jsonContainsOperator(path string, arg interface{}) jsonOperator {
	return func(c []byte) bool {
		jsonParsed, err := gabs.ParseJSON(c)
		if err != nil {
			return false
		}
		switch arg.(type) {
		case string:
			for _, child := range jsonParsed.Path(path).Children() {
				if _, ok := child.Data().(string); ok && child.Data().(string) == arg.(string) {
					return true
				}
			}
		case bool:
			for _, child := range jsonParsed.Path(path).Children() {
				if _, ok := child.Data().(bool); ok && child.Data().(bool) == arg.(bool) {
					return true
				}
			}
		case int:
			for _, child := range jsonParsed.Path(path).Children() {
				if _, ok := child.Data().(float64); ok && child.Data().(float64) == float64(arg.(int)) {
					return true
				}
			}
		case float64:
			for _, child := range jsonParsed.Path(path).Children() {
				if _, ok := child.Data().(float64); ok && child.Data().(float64) == arg.(float64) {
					return true
				}
			}
		}
		return false
	}
}

func strToJSONOperator(op, path string, arg interface{}) (jsonOperator, error) {

	switch op {
	case "exists":
		return jsonExistOperator(path), nil
	case "contains":
		return jsonContainsOperator(path, arg), nil
	}
	return nil, errors.New("invalid json operator type")
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

	msgPart := msg.Get(index).Get()
	if msgPart == nil {
		c.mFalse.Incr(1)
		return false
	}

	res := c.operator(msgPart)
	if res {
		c.mTrue.Incr(1)
	} else {
		c.mFalse.Incr(1)
	}
	return res
}
