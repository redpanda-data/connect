package condition

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	radix "github.com/armon/go-radix"
	"github.com/spf13/cast"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMetadata] = TypeSpec{
		constructor: NewMetadata,
		Description: `
Metadata is a condition that checks metadata keys of a message part against an
operator from the following list:

### ` + "`enum`" + `

Checks whether the contents of a metadata key matches one of the defined enum
values.

` + "```yaml" + `
metadata:
  operator: enum
  part: 0
  key: foo
  arg:
    - bar
    - baz
    - qux
    - quux
` + "```" + `

### ` + "`equals`" + `

Checks whether the contents of a metadata key matches an argument. This operator
is case insensitive.

` + "```yaml" + `
metadata:
  operator: equals
  part: 0
  key: foo
  arg: bar
` + "```" + `

### ` + "`equals_cs`" + `

Checks whether the contents of a metadata key matches an argument. This operator
is case sensitive.

` + "```yaml" + `
metadata:
  operator: equals_cs
  part: 0
  key: foo
  arg: BAR
` + "```" + `

### ` + "`exists`" + `

Checks whether a metadata key exists.

` + "```yaml" + `
metadata:
  operator: exists
  part: 0
  key: foo
` + "```" + `

### ` + "`greater_than`" + `

Checks whether the contents of a metadata key, parsed as a floating point
number, is greater than an argument. Returns false if the metadata value cannot
be parsed into a number.

` + "```yaml" + `
metadata:
  operator: greater_than
  part: 0
  key: foo
  arg: 3
` + "```" + `

### ` + "`has_prefix`" + `

Checks whether the contents of a metadata key match one of the provided prefixes.
The arg field can either be a singular prefix string or a list of prefixes.

` + "```yaml" + `
metadata:
  operator: has_prefix
  part: 0
  key: foo
  arg:
    - foo
    - bar
    - baz
` + "```" + `

### ` + "`less_than`" + `

Checks whether the contents of a metadata key, parsed as a floating point
number, is less than an argument. Returns false if the metadata value cannot be
parsed into a number.

` + "```yaml" + `
metadata:
  operator: less_than
  part: 0
  key: foo
  arg: 3
` + "```" + `

### ` + "`regexp_partial`" + `

Checks whether any section of the contents of a metadata key matches a regular
expression (RE2 syntax).

` + "```yaml" + `
metadata:
  operator: regexp_partial
  part: 0
  key: foo
  arg: "1[a-z]2"
` + "```" + `

### ` + "`regexp_exact`" + `

Checks whether the contents of a metadata key exactly matches a regular expression 
(RE2 syntax).

` + "```yaml" + `
metadata:
  operator: regexp_partial
  part: 0
  key: foo
  arg: "1[a-z]2"
` + "```" + `
`,
	}
}

//------------------------------------------------------------------------------

// Errors for the metadata condition.
var (
	ErrInvalidMetadataOperator = errors.New("invalid metadata operator type")
)

// MetadataConfig is a configuration struct containing fields for the metadata
// condition.
type MetadataConfig struct {
	Operator string      `json:"operator" yaml:"operator"`
	Part     int         `json:"part" yaml:"part"`
	Key      string      `json:"key" yaml:"key"`
	Arg      interface{} `json:"arg" yaml:"arg"`
}

// NewMetadataConfig returns a MetadataConfig with default values.
func NewMetadataConfig() MetadataConfig {
	return MetadataConfig{
		Operator: "equals_cs",
		Part:     0,
		Key:      "",
		Arg:      "",
	}
}

//------------------------------------------------------------------------------

type metadataOperator func(md types.Metadata) bool

func metadataEnumOperator(key string, arg interface{}) (metadataOperator, error) {
	entries, err := cast.ToStringSliceE(arg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse argument as string slice: %v", err)
	}
	tree := radix.New()
	for _, entry := range entries {
		tree.Insert(entry, struct{}{})
	}
	return func(md types.Metadata) bool {
		_, ok := tree.Get(md.Get(key))
		return ok
	}, nil
}

func metadataEqualsCSOperator(key string, arg interface{}) (metadataOperator, error) {
	argStr, err := cast.ToStringE(arg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse argument as string: %v", err)
	}
	return func(md types.Metadata) bool {
		return md.Get(key) == argStr
	}, nil
}

func metadataEqualsOperator(key string, arg interface{}) (metadataOperator, error) {
	argStr, err := cast.ToStringE(arg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse argument as string: %v", err)
	}
	return func(md types.Metadata) bool {
		return strings.ToLower(md.Get(key)) == strings.ToLower(argStr)
	}, nil
}

func metadataExistsOperator(key string) metadataOperator {
	return func(md types.Metadata) bool {
		return len(md.Get(key)) > 0
	}
}

func metadataGreaterThanOperator(key string, arg interface{}) (metadataOperator, error) {
	v, err := cast.ToFloat64E(arg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse argument as float64: %v", err)
	}
	return func(md types.Metadata) bool {
		val, verr := strconv.ParseFloat(md.Get(key), 10)
		if verr != nil {
			return false
		}
		return val > v
	}, nil
}

func metadataHasPrefixOperator(key string, arg interface{}) (metadataOperator, error) {
	if prefix, ok := arg.(string); ok {
		return func(md types.Metadata) bool {
			return strings.HasPrefix(md.Get(key), prefix)
		}, nil
	}
	entries, err := cast.ToStringSliceE(arg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse argument as string or string slice: %v", err)
	}
	tree := radix.New()
	for _, entry := range entries {
		tree.Insert(entry, struct{}{})
	}
	return func(md types.Metadata) bool {
		_, _, ok := tree.LongestPrefix(md.Get(key))
		return ok
	}, nil
}

func metadataLessThanOperator(key string, arg interface{}) (metadataOperator, error) {
	v, err := cast.ToFloat64E(arg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse argument as float64: %v", err)
	}
	return func(md types.Metadata) bool {
		val, verr := strconv.ParseFloat(md.Get(key), 10)
		if verr != nil {
			return false
		}
		return val < v
	}, nil
}

func metadataRegexpPartialOperator(key string, arg interface{}) (metadataOperator, error) {
	argStr, err := cast.ToStringE(arg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse argument as string: %v", err)
	}
	compiled, err := regexp.Compile(argStr)
	if err != nil {
		return nil, err
	}
	return func(md types.Metadata) bool {
		return compiled.MatchString(md.Get(key))
	}, nil
}

func metadataRegexpExactOperator(key string, arg interface{}) (metadataOperator, error) {
	argStr, err := cast.ToStringE(arg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse argument as string: %v", err)
	}
	compiled, err := regexp.Compile(argStr)
	if err != nil {
		return nil, err
	}
	return func(md types.Metadata) bool {
		val := md.Get(key)
		return len(compiled.FindString(val)) == len(val)
	}, nil
}

func strToMetadataOperator(str, key string, arg interface{}) (metadataOperator, error) {
	switch str {
	case "enum":
		return metadataEnumOperator(key, arg)
	case "equals":
		return metadataEqualsOperator(key, arg)
	case "equals_cs":
		return metadataEqualsCSOperator(key, arg)
	case "exists":
		return metadataExistsOperator(key), nil
	case "greater_than":
		return metadataGreaterThanOperator(key, arg)
	case "has_prefix":
		return metadataHasPrefixOperator(key, arg)
	case "less_than":
		return metadataLessThanOperator(key, arg)
	case "regexp_partial":
		return metadataRegexpPartialOperator(key, arg)
	case "regexp_exact":
		return metadataRegexpExactOperator(key, arg)
	}
	return nil, ErrInvalidMetadataOperator
}

//------------------------------------------------------------------------------

// Metadata is a condition that checks message text against logical operators.
type Metadata struct {
	stats    metrics.Type
	operator metadataOperator
	part     int

	mCount metrics.StatCounter
	mTrue  metrics.StatCounter
	mFalse metrics.StatCounter
}

// NewMetadata returns a Metadata condition.
func NewMetadata(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	op, err := strToMetadataOperator(conf.Metadata.Operator, conf.Metadata.Key, conf.Metadata.Arg)
	if err != nil {
		return nil, fmt.Errorf("operator '%v': %v", conf.Metadata.Operator, err)
	}
	return &Metadata{
		stats:    stats,
		operator: op,
		part:     conf.Metadata.Part,

		mCount: stats.GetCounter("count"),
		mTrue:  stats.GetCounter("true"),
		mFalse: stats.GetCounter("false"),
	}, nil
}

//------------------------------------------------------------------------------

// Check attempts to check a message part against a configured condition.
func (c *Metadata) Check(msg types.Message) bool {
	c.mCount.Incr(1)
	index := c.part
	lParts := msg.Len()
	if lParts == 0 {
		c.mFalse.Incr(1)
		return false
	}

	res := c.operator(msg.Get(index).Metadata())
	if res {
		c.mTrue.Incr(1)
	} else {
		c.mFalse.Incr(1)
	}
	return res
}

//------------------------------------------------------------------------------
