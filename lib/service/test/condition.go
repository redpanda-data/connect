// Copyright (c) 2019 Ashley Jeffs
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
		return fmt.Errorf("content mismatch, expected '%v', got '%v'", exp, act)
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
		return fmt.Errorf("content mismatch, expected '%v', got '%v'", string(c), string(p.Get()))
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
			return fmt.Errorf("metadata key '%v' mismatch, expected '%v', got '%v'", k, exp, act)
		}
	}
	return nil
}

//------------------------------------------------------------------------------
