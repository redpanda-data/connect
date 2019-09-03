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

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/metadata"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// InputPart defines an input part for a test case.
type InputPart struct {
	Content  string            `yaml:"content"`
	Metadata map[string]string `yaml:"metadata"`
}

// Case contains a definition of a single Benthos config test case.
type Case struct {
	Name             string            `yaml:"name"`
	Environment      map[string]string `yaml:"environment"`
	TargetProcessors string            `yaml:"target_processors"`
	InputBatch       []InputPart       `yaml:"input_batch"`
	OutputBatches    [][]ConditionsMap `yaml:"output_batches"`

	line int
}

// NewCase returns a default test case.
func NewCase() Case {
	return Case{
		Name:             "Example test case",
		Environment:      map[string]string{},
		TargetProcessors: "/pipeline/processors",
		InputBatch: []InputPart{
			{
				Content: "A sample document",
				Metadata: map[string]string{
					"example_key": "some value",
				},
			},
		},
		OutputBatches: [][]ConditionsMap{
			{
				ConditionsMap{
					"content_equals": ContentEqualsCondition("A SAMPLE DOCUMENT"),
					"metadata_equals": MetadataEqualsCondition{
						"example_key": "some other value now",
					},
				},
			},
		},
	}
}

// UnmarshalYAML extracts a Case from a YAML node.
func (c *Case) UnmarshalYAML(value *yaml.Node) error {
	type caseAlias Case
	aliased := caseAlias(NewCase())

	if err := value.Decode(&aliased); err != nil {
		return err
	}

	*c = Case(aliased)
	c.line = value.Line
	return nil
}

//------------------------------------------------------------------------------

// CaseFailure encapsulates information about a failed test case.
type CaseFailure struct {
	Name     string
	TestLine int
	Reason   string
}

// String returns a string representation of the case failure.
func (c CaseFailure) String() string {
	return fmt.Sprintf("%v [line %v]: %v", c.Name, c.TestLine, c.Reason)
}

// ProcProvider returns compiled processors extracted from a Benthos config
// using a JSON Pointer.
type ProcProvider interface {
	Provide(jsonPtr string, environment map[string]string) ([]types.Processor, error)
}

// Execute attempts to execute a test case against a Benthos configuration.
func (c *Case) Execute(provider ProcProvider) (failures []CaseFailure, err error) {
	var procSet []types.Processor
	if procSet, err = provider.Provide(c.TargetProcessors, c.Environment); err != nil {
		return nil, fmt.Errorf("failed to initialise processors '%v': %v", c.TargetProcessors, err)
	}

	reportFailure := func(reason string) {
		failures = append(failures, CaseFailure{
			Name:     c.Name,
			TestLine: c.line,
			Reason:   reason,
		})
	}

	parts := make([]types.Part, len(c.InputBatch))
	for i, v := range c.InputBatch {
		part := message.NewPart([]byte(v.Content))
		part.SetMetadata(metadata.New(v.Metadata))
		parts[i] = part
	}

	inputMsg := message.New(nil)
	inputMsg.SetAll(parts)
	outputBatches, result := processor.ExecuteAll(procSet, inputMsg)
	if result != nil {
		if len(c.OutputBatches) == 0 {
			return
		}
		if result.Error() != nil {
			reportFailure(fmt.Sprintf("processors resulted in error: %v", result.Error()))
		} else {
			reportFailure("processors resulted in zero output batches")
		}
		return
	}

	if lExp, lAct := len(c.OutputBatches), len(outputBatches); lAct < lExp {
		reportFailure(fmt.Sprintf("wrong batch count, expected %v, got %v", lExp, lAct))
	}

	for i, v := range outputBatches {
		if len(c.OutputBatches) <= i {
			reportFailure(fmt.Sprintf("unexpected batch: %s", message.GetAllBytes(v)))
			continue
		}
		expectedBatch := c.OutputBatches[i]
		if lExp, lAct := len(expectedBatch), v.Len(); lExp != lAct {
			reportFailure(fmt.Sprintf("mismatch of output batch %v message counts, expected %v, got %v", i, lExp, lAct))
		}
		v.Iter(func(i2 int, part types.Part) error {
			if len(expectedBatch) <= i2 {
				reportFailure(fmt.Sprintf("unexpected message from batch %v: %s", i, part.Get()))
				return nil
			}
			for _, condErr := range expectedBatch[i2].CheckAll(part) {
				reportFailure(fmt.Sprintf("batch %v message %v: %v", i, i2, condErr))
			}
			return nil
		})
	}
	return
}

//------------------------------------------------------------------------------
