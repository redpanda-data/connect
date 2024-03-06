package neo

import (
	"context"
	"fmt"
	"io/fs"

	iprocessor "github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/config/test"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// CaseExecution encapsulates information about an executed test case.
type CaseExecution struct {
	Name     string
	TestLine int
	Status   ExecStatus
	Err      error
	Failures []string
}

func (ce *CaseExecution) ReportFailure(reason string) {
	ce.Failures = append(ce.Failures, reason)

	if ce.Status != Errored {
		ce.Status = Failed
	}
}

// ProcProvider returns compiled processors extracted from a Benthos config
// using a JSON Pointer.
type ProcProvider interface {
	Provide(jsonPtr string, environment map[string]string, mocks map[string]any) ([]iprocessor.V1, error)
	ProvideBloblang(path string) ([]iprocessor.V1, error)
}

// ExecuteFrom executes a test case from the perspective of a given directory,
// which is used for obtaining relative condition file imports.
func RunCase(fs fs.FS, dir string, c test.Case, provider ProcProvider) *CaseExecution {
	ce := &CaseExecution{
		Name:     c.Name,
		TestLine: c.Line(),
		Status:   Passed, // Assume success until proven otherwise.
	}

	var err error
	var procSet []iprocessor.V1
	if c.TargetMapping != "" {
		if procSet, err = provider.ProvideBloblang(c.TargetMapping); err != nil {
			ce.Err = fmt.Errorf("failed to initialise Bloblang mapping '%v': %w", c.TargetMapping, err)
			ce.Status = Errored
			return ce
		}
	} else {
		if procSet, err = provider.Provide(c.TargetProcessors, c.Environment, c.Mocks); err != nil {
			ce.Err = fmt.Errorf("failed to initialise processors '%v': %w", c.TargetProcessors, err)
			ce.Status = Errored
			return ce
		}
	}

	var inputMsg []message.Batch

	for _, inputBatch := range c.InputBatches {
		parts := make([]*message.Part, len(inputBatch))
		for i, v := range inputBatch {
			if parts[i], err = v.ToMessage(fs, dir); err != nil {
				ce.Err = fmt.Errorf("failed to create test input %v: %w", i, err)
				return ce
			}
		}

		currentBatch := message.Batch(parts)
		inputMsg = append(inputMsg, currentBatch)

	}

	outputBatches, result := iprocessor.ExecuteAll(context.Background(), procSet, inputMsg...)
	if result != nil {
		ce.Err = fmt.Errorf("processors resulted in error: %v", result)
		ce.Status = Errored
		return ce
	}

	if lExp, lAct := len(c.OutputBatches), len(outputBatches); lAct < lExp {
		ce.ReportFailure(fmt.Sprintf("wrong batch count, expected %v, got %v", lExp, lAct))
	}

	for i, v := range outputBatches {
		if len(c.OutputBatches) <= i {
			ce.ReportFailure(fmt.Sprintf("unexpected batch: %s", message.GetAllBytes(v)))
			continue
		}
		expectedBatch := c.OutputBatches[i]
		if lExp, lAct := len(expectedBatch), v.Len(); lExp != lAct {
			ce.ReportFailure(fmt.Sprintf("mismatch of output batch %v message counts, expected %v, got %v", i, lExp, lAct))
		}
		_ = v.Iter(func(i2 int, part *message.Part) error {
			if len(expectedBatch) <= i2 {
				ce.ReportFailure(fmt.Sprintf("unexpected message from batch %v: %s", i, part.AsBytes()))
				return nil
			}
			condErrs := expectedBatch[i2].CheckAll(fs, dir, part)
			for _, condErr := range condErrs {
				ce.ReportFailure(fmt.Sprintf("batch %v message %v: %v", i, i2, condErr))
			}
			if procErr := part.ErrorGet(); procErr != nil && len(condErrs) > 0 {
				ce.ReportFailure(fmt.Sprintf("batch %v message %v: %v", i, i2, Red(procErr)))
			}
			return nil
		})
	}

	return ce
}
