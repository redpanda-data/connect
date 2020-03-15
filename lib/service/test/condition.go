package test

import (
	"fmt"
	"regexp"
	"sort"

	"github.com/Jeffail/benthos/v3/lib/types"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// Condition is a test case against a message part.
type Condition interface {
	Check(part types.Part) error
}

//------------------------------------------------------------------------------

// ConditionsMap contains a map of conditions to condition string types.
type ConditionsMap map[string]Condition

// UnmarshalYAML extracts a ConditionsMap from a YAML node.
func (c *ConditionsMap) UnmarshalYAML(value *yaml.Node) error {
	*c = map[string]Condition{}

	rawMap := map[string]yaml.Node{}
	if err := value.Decode(&rawMap); err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}

	for k, v := range rawMap {
		var cond Condition
		switch k {
		case "content_equals":
			val := ContentEqualsCondition("")
			if err := v.Decode(&val); err != nil {
				return fmt.Errorf("line %v: %v", v.Line, err)
			}
			cond = val
		case "content_matches":
			val := ContentMatchesCondition("")
			if err := v.Decode(&val); err != nil {
				return fmt.Errorf("line %v: %v", v.Line, err)
			}
			cond = val
		case "metadata_equals":
			val := MetadataEqualsCondition{}
			if err := v.Decode(&val); err != nil {
				return fmt.Errorf("line %v: %v", v.Line, err)
			}
			cond = val
		default:
			return fmt.Errorf("line %v: message part condition type not recognised: %v", v.Line, k)
		}
		(*c)[k] = cond
	}
	return nil
}

// CheckAll checks all conditions against a message part. Conditions are
// executed in alphabetical order.
func (c ConditionsMap) CheckAll(part types.Part) (errs []error) {
	condTypes := []string{}
	for k := range c {
		condTypes = append(condTypes, k)
	}
	sort.Strings(condTypes)
	for _, k := range condTypes {
		if err := c[k].Check(part); err != nil {
			errs = append(errs, fmt.Errorf("%v: %v", k, err))
		}
	}
	return
}

//------------------------------------------------------------------------------

// ContentEqualsCondition is a string condition that tests the string against
// the contents of a message.
type ContentEqualsCondition string

// Check this condition against a message part.
func (c ContentEqualsCondition) Check(p types.Part) error {
	if exp, act := string(c), string(p.Get()); exp != act {
		return fmt.Errorf("content mismatch\n  expected: %v\n  received: %v", blue(exp), red(act))
	}
	return nil
}

//------------------------------------------------------------------------------

// ContentMatchesCondition is a string condition that tests parses the string as
// a regular expression and tests that regular expression against the contents of a message.
type ContentMatchesCondition string

// Check this condition against a message part.
func (c ContentMatchesCondition) Check(p types.Part) error {
	re := regexp.MustCompile(string(c))
	if !re.MatchString(string(p.Get())) {
		return fmt.Errorf("pattern mismatch\n   pattern: %v\n  received: %v", blue(string(c)), red(string(p.Get())))
	}
	return nil
}

//------------------------------------------------------------------------------

// MetadataEqualsCondition checks whether a metadata keys contents matches a
// value.
type MetadataEqualsCondition map[string]string

// Check this condition against a message part.
func (m MetadataEqualsCondition) Check(p types.Part) error {
	for k, v := range m {
		if exp, act := v, p.Metadata().Get(k); exp != act {
			return fmt.Errorf("metadata key '%v' mismatch\n  expected: %v\n  received: %v", k, blue(exp), red(act))
		}
	}
	return nil
}

//------------------------------------------------------------------------------
