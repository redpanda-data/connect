package test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"

	"github.com/nsf/jsondiff"
	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/bloblang"
	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// Condition is a test case against a message part.
type Condition interface {
	Check(part *message.Part) error
}

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
		case "bloblang":
			b, err := parseBloblangCondition(v)
			if err != nil {
				return fmt.Errorf("line %v: %v", v.Line, err)
			}
			cond = b
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
		case "json_equals":
			val := ContentJSONEqualsCondition("")
			if err := yamlNodeToTestString(&v, (*string)(&val)); err != nil {
				return fmt.Errorf("line %v: %v", v.Line, err)
			}
			cond = val
		case "json_contains":
			val := ContentJSONContainsCondition("")
			if err := yamlNodeToTestString(&v, (*string)(&val)); err != nil {
				return fmt.Errorf("line %v: %v", v.Line, err)
			}
			cond = val
		case "file_equals":
			val := FileEqualsCondition("")
			if err := v.Decode(&val); err != nil {
				return fmt.Errorf("line %v: %v", v.Line, err)
			}
			cond = val
		case "file_json_equals":
			val := FileJSONEqualsCondition("")
			if err := v.Decode(&val); err != nil {
				return fmt.Errorf("line %v: %v", v.Line, err)
			}
			cond = val
		case "file_json_contains":
			val := FileJSONContainsCondition("")
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
func (c ConditionsMap) CheckAll(dir string, part *message.Part) (errs []error) {
	condTypes := []string{}
	for k := range c {
		condTypes = append(condTypes, k)
	}
	sort.Strings(condTypes)
	for _, k := range condTypes {
		if relCheck, ok := c[k].(interface {
			checkFrom(string, *message.Part) error
		}); ok {
			if err := relCheck.checkFrom(dir, part); err != nil {
				errs = append(errs, fmt.Errorf("%v: %v", k, err))
			}
		} else if err := c[k].Check(part); err != nil {
			errs = append(errs, fmt.Errorf("%v: %v", k, err))
		}
	}
	return
}

//------------------------------------------------------------------------------

type bloblangCondition struct {
	m *mapping.Executor
}

func parseBloblangCondition(n yaml.Node) (*bloblangCondition, error) {
	var expr string

	if err := n.Decode(&expr); err != nil {
		return nil, err
	}

	m, err := bloblang.GlobalEnvironment().NewMapping(expr)
	if err != nil {
		return nil, err
	}

	return &bloblangCondition{m}, nil
}

// Check this condition against a message part.
func (b *bloblangCondition) Check(p *message.Part) error {
	msg := message.Batch{p}
	res, err := b.m.QueryPart(0, msg)
	if err != nil {
		return err
	}
	if !res {
		return errors.New("bloblang expression was false")
	}
	return nil
}

//------------------------------------------------------------------------------

// ContentEqualsCondition is a string condition that tests the string against
// the contents of a message.
type ContentEqualsCondition string

// Check this condition against a message part.
func (c ContentEqualsCondition) Check(p *message.Part) error {
	if exp, act := string(c), string(p.AsBytes()); exp != act {
		return fmt.Errorf("content mismatch\n  expected: %v\n  received: %v", blue(exp), red(act))
	}
	return nil
}

//------------------------------------------------------------------------------

// ContentMatchesCondition is a string condition that tests parses the string as
// a regular expression and tests that regular expression against the contents of a message.
type ContentMatchesCondition string

// Check this condition against a message part.
func (c ContentMatchesCondition) Check(p *message.Part) error {
	re := regexp.MustCompile(string(c))
	if !re.MatchString(string(p.AsBytes())) {
		return fmt.Errorf("pattern mismatch\n   pattern: %v\n  received: %v", blue(string(c)), red(string(p.AsBytes())))
	}
	return nil
}

//------------------------------------------------------------------------------

// ContentJSONEqualsCondition is a string condition that tests the string against
// the contents of a message using JSON comparison and is true if the expected
// and actual documents are both valid JSON and deeply equal.
type ContentJSONEqualsCondition string

// Check this condition against a message part.
func (c ContentJSONEqualsCondition) Check(p *message.Part) error {
	jdopts := jsondiff.DefaultConsoleOptions()
	diff, explanation := jsondiff.Compare(p.AsBytes(), []byte(c), &jdopts)
	if diff != jsondiff.FullMatch {
		return fmt.Errorf("JSON content mismatch\n%v", explanation)
	}
	return nil
}

//------------------------------------------------------------------------------

// ContentJSONContainsCondition is a string condition that tests the string against
// the contents of a message using JSON comparison and is true if the expected
// and actual documents are both valid JSON, and the actual is a superset of the expected.
type ContentJSONContainsCondition string

// Check this condition against a message part.
func (c ContentJSONContainsCondition) Check(p *message.Part) error {
	jdopts := jsondiff.DefaultConsoleOptions()
	diff, explanation := jsondiff.Compare(p.AsBytes(), []byte(c), &jdopts)
	if diff != jsondiff.FullMatch && diff != jsondiff.SupersetMatch {
		return fmt.Errorf("JSON superset mismatch\n%v", explanation)
	}
	return nil
}

//------------------------------------------------------------------------------

// FileEqualsCondition is a string condition that reads a file at the string
// path and compares it against the contents of a message.
type FileEqualsCondition string

// Check this condition against a message part.
func (c FileEqualsCondition) Check(p *message.Part) error {
	return c.checkFrom("", p)
}

func (c FileEqualsCondition) checkFrom(dir string, p *message.Part) error {
	relPath := filepath.Join(dir, string(c))

	fileContent, err := ifs.ReadFile(ifs.OS(), relPath)
	if err != nil {
		return fmt.Errorf("failed to read comparison file: %w", err)
	}

	if exp, act := string(fileContent), string(p.AsBytes()); exp != act {
		return fmt.Errorf("content mismatch\n  expected: %v\n  received: %v", blue(exp), red(act))
	}
	return nil
}

//------------------------------------------------------------------------------

// FileJSONEqualsCondition is a string condition that tests the contents of the file
// against the contents of a message using JSON comparison and is true if the expected
// and actual documents are both valid JSON and deeply equal.
type FileJSONEqualsCondition string

// Check this condition against a message part.
func (c FileJSONEqualsCondition) Check(p *message.Part) error {
	return c.checkFrom("", p)
}

func (c FileJSONEqualsCondition) checkFrom(dir string, p *message.Part) error {
	relPath := filepath.Join(dir, string(c))

	fileContent, err := ifs.ReadFile(ifs.OS(), relPath)
	if err != nil {
		return fmt.Errorf("failed to read comparison JSON file: %w", err)
	}

	comparison := ContentJSONEqualsCondition(fileContent)
	return comparison.Check(p)
}

//------------------------------------------------------------------------------

// FileJSONContainsCondition is a string condition that tests the contents of the file
// against the contents of a message using JSON comparison and is true if the expected
// and actual documents are both valid JSON and the actual is a superset of the expected.
type FileJSONContainsCondition string

// Check this condition against a message part.
func (c FileJSONContainsCondition) Check(p *message.Part) error {
	return c.checkFrom("", p)
}

func (c FileJSONContainsCondition) checkFrom(dir string, p *message.Part) error {
	relPath := filepath.Join(dir, string(c))

	fileContent, err := ifs.ReadFile(ifs.OS(), relPath)
	if err != nil {
		return fmt.Errorf("failed to read comparison JSON file: %w", err)
	}

	comparison := ContentJSONContainsCondition(fileContent)
	return comparison.Check(p)
}

//------------------------------------------------------------------------------

// MetadataEqualsCondition checks whether a metadata keys contents matches a
// value.
type MetadataEqualsCondition map[string]any

// Check this condition against a message part.
func (m MetadataEqualsCondition) Check(p *message.Part) error {
	for k, exp := range m {
		act, exists := p.MetaGetMut(k)
		if !exists {
			return fmt.Errorf("metadata key '%v' expected but not found", k)
		}
		if !query.ICompare(exp, act) {
			return fmt.Errorf("metadata key '%v' mismatch\n  expected: %v\n  received: %v", k, blue(exp), red(act))
		}
	}
	return nil
}

//------------------------------------------------------------------------------

// Helper function for converting yaml.Node to a string
// simple nodes are converted to their string equivalents
// complex nodes are converted to a JSON representation
// assumption is that only the subset of YAML compatible
// with JSON will be present; decode errors will trigger
// if this is not the case.
func yamlNodeToTestString(n *yaml.Node, tgt *string) error {
	switch n.Kind {
	case yaml.SequenceNode:
		var aval []any
		err := n.Decode(&aval)
		if err != nil {
			return err
		}
		bval, err := json.Marshal(aval)
		*tgt = bytes.NewBuffer(bval).String()
		return err
	case yaml.MappingNode:
		var mval map[string]any
		err := n.Decode(&mval)
		if err != nil {
			return err
		}
		bval, err := json.Marshal(mval)
		*tgt = bytes.NewBuffer(bval).String()
		return err
	case yaml.ScalarNode:
		return n.Decode(tgt)
	case yaml.AliasNode:
		return yamlNodeToTestString(n.Alias, tgt)
	}
	return fmt.Errorf("unsupported yaml node type %s", n.ShortTag())
}
