package test

import (
	"context"
	"fmt"
	"io/fs"

	iprocessor "github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/config/test"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// CaseFailure encapsulates information about a failed test case.
type CaseFailure struct {
	Name     string
	TestLine int
	Reason   string
}

// String returns a string representation of the case failure.
func (c CaseFailure) String() string {
	if c.TestLine == 0 {
		return fmt.Sprintf("%v: %v", c.Name, c.Reason)
	}
	return fmt.Sprintf("%v [line %v]: %v", c.Name, c.TestLine, c.Reason)
}

// ProcProvider returns compiled processors extracted from a Benthos config
// using a JSON Pointer.
type ProcProvider interface {
	Provide(jsonPtr string, environment map[string]string, mocks map[string]any) ([]iprocessor.V1, error)
	ProvideBloblang(path string) ([]iprocessor.V1, error)
}

// ExecuteFrom executes a test case from the perspective of a given directory,
// which is used for obtaining relative condition file imports.
func ExecuteFrom(fs fs.FS, dir string, c test.Case, provider ProcProvider) (failures []CaseFailure, err error) {
	var procSet []iprocessor.V1
	if c.TargetMapping != "" {
		if procSet, err = provider.ProvideBloblang(c.TargetMapping); err != nil {
			return nil, fmt.Errorf("failed to initialise Bloblang mapping '%v': %v", c.TargetMapping, err)
		}
	} else {
		if procSet, err = provider.Provide(c.TargetProcessors, c.Environment, c.Mocks); err != nil {
			return nil, fmt.Errorf("failed to initialise processors '%v': %v", c.TargetProcessors, err)
		}
	}

	reportFailure := func(reason string) {
		failures = append(failures, CaseFailure{
			Name:     c.Name,
			TestLine: c.Line(),
			Reason:   reason,
		})
	}

	var inputMsg []message.Batch

	for _, inputBatch := range c.InputBatches {
		parts := make([]*message.Part, len(inputBatch))
		for i, v := range inputBatch {
			if parts[i], err = v.ToMessage(fs, dir); err != nil {
				err = fmt.Errorf("failed to create test input %v: %w", i, err)
				return
			}
		}

		currentBatch := message.Batch(parts)
		inputMsg = append(inputMsg, currentBatch)

	}

	outputBatches, result := iprocessor.ExecuteAll(context.Background(), procSet, inputMsg...)
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
				reportFailure(fmt.Sprintf("unexpected message from batch %v: %s", i, part.AsBytes()))
				return nil
			}
			condErrs := expectedBatch[i2].CheckAll(fs, dir, part)
			for _, condErr := range condErrs {
				reportFailure(fmt.Sprintf("batch %v message %v: %v", i, i2, condErr))
			}
			if procErr := part.ErrorGet(); procErr != nil && len(condErrs) > 0 {
				reportFailure(fmt.Sprintf("batch %v message %v: %v", i, i2, red(procErr)))
			}
			return nil
		})
	}
	return
}
