package test

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// InputPart defines an input part for a test case.
type InputPart struct {
	Content  string            `yaml:"content"`
	Metadata map[string]string `yaml:"metadata"`
	filePath string
}

func (i *InputPart) getContent(dir string) (string, error) {
	if i.filePath == "" {
		return i.Content, nil
	}
	relPath := filepath.Join(dir, i.filePath)
	rawBytes, err := os.ReadFile(relPath)
	if err != nil {
		return "", err
	}
	return string(rawBytes), nil
}

// UnmarshalYAML extracts an InputPart from a YAML node.
func (i *InputPart) UnmarshalYAML(value *yaml.Node) error {
	rawMap := map[string]yaml.Node{}
	if err := value.Decode(&rawMap); err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}
	for k, v := range rawMap {
		switch k {
		case "content":
			if err := v.Decode(&i.Content); err != nil {
				return fmt.Errorf("line %v: %v", v.Line, err)
			}
		case "json_content":
			if err := yamlNodeToTestString(&v, &i.Content); err != nil {
				return fmt.Errorf("line %v: %v", v.Line, err)
			}
		case "file_content":
			if err := v.Decode(&i.filePath); err != nil {
				return fmt.Errorf("line %v: %v", v.Line, err)
			}
		case "metadata":
			if err := v.Decode(&i.Metadata); err != nil {
				return fmt.Errorf("line %v: %v", v.Line, err)
			}
		default:
			return fmt.Errorf("line %v: input part field not recognised: %v", v.Line, k)
		}
	}
	return nil
}

//------------------------------------------------------------------------------

// Case contains a definition of a single Benthos config test case.
type Case struct {
	Name             string               `yaml:"name"`
	Environment      map[string]string    `yaml:"environment"`
	TargetProcessors string               `yaml:"target_processors"`
	TargetMapping    string               `yaml:"target_mapping"`
	Mocks            map[string]yaml.Node `yaml:"mocks"`
	InputBatch       []InputPart          `yaml:"input_batch"`
	OutputBatches    [][]ConditionsMap    `yaml:"output_batches"`

	line int
}

// AtLine returns a test case at a given line.
func (c Case) AtLine(l int) Case {
	c.line = l
	return c
}

// NewCase returns a default test case.
func NewCase() Case {
	return Case{
		Name:             "Example test case",
		Environment:      map[string]string{},
		TargetProcessors: "/pipeline/processors",
		TargetMapping:    "",
		Mocks:            map[string]yaml.Node{},
		InputBatch:       []InputPart{},
		OutputBatches:    [][]ConditionsMap{},
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
	ProvideBloblang(path string) ([]types.Processor, error)
}

type mockedProcProvider interface {
	ProvideMocked(jsonPtr string, environment map[string]string, mocks map[string]yaml.Node) ([]types.Processor, error)
}

// Execute attempts to execute a test case against a Benthos configuration.
func (c *Case) Execute(provider ProcProvider) (failures []CaseFailure, err error) {
	return c.executeFrom("", provider)
}

func (c *Case) executeFrom(dir string, provider ProcProvider) (failures []CaseFailure, err error) {
	var procSet []types.Processor
	if c.TargetMapping != "" {
		if procSet, err = provider.ProvideBloblang(c.TargetMapping); err != nil {
			return nil, fmt.Errorf("failed to initialise Bloblang mapping '%v': %v", c.TargetMapping, err)
		}
	} else if mockedProcProv, ok := provider.(mockedProcProvider); ok {
		if procSet, err = mockedProcProv.ProvideMocked(c.TargetProcessors, c.Environment, c.Mocks); err != nil {
			return nil, fmt.Errorf("failed to initialise processors '%v': %v", c.TargetProcessors, err)
		}
	} else if procSet, err = provider.Provide(c.TargetProcessors, c.Environment); err != nil {
		return nil, fmt.Errorf("failed to initialise processors '%v': %v", c.TargetProcessors, err)
	}

	reportFailure := func(reason string) {
		failures = append(failures, CaseFailure{
			Name:     c.Name,
			TestLine: c.line,
			Reason:   reason,
		})
	}

	parts := make([]*message.Part, len(c.InputBatch))
	for i, v := range c.InputBatch {
		var content string
		if content, err = v.getContent(dir); err != nil {
			err = fmt.Errorf("failed to create mock input %v: %w", i, err)
			return
		}
		part := message.NewPart([]byte(content))
		for k, v := range v.Metadata {
			part.MetaSet(k, v)
		}
		parts[i] = part
	}

	inputMsg := message.QuickBatch(nil)
	inputMsg.SetAll(parts)
	outputBatches, result := processor.ExecuteAll(procSet, inputMsg)
	if result != nil {
		reportFailure(fmt.Sprintf("processors resulted in error: %v", result))
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
		_ = v.Iter(func(i2 int, part *message.Part) error {
			if len(expectedBatch) <= i2 {
				reportFailure(fmt.Sprintf("unexpected message from batch %v: %s", i, part.Get()))
				return nil
			}
			condErrs := expectedBatch[i2].checkAllFrom(dir, part)
			for _, condErr := range condErrs {
				reportFailure(fmt.Sprintf("batch %v message %v: %v", i, i2, condErr))
			}
			if procErr := processor.GetFail(part); len(procErr) > 0 && len(condErrs) > 0 {
				reportFailure(fmt.Sprintf("batch %v message %v: %v", i, i2, red(procErr)))
			}
			return nil
		})
	}
	return
}

//------------------------------------------------------------------------------
