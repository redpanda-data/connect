package test

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/log"
	"golang.org/x/sync/errgroup"
)

//------------------------------------------------------------------------------

// Definition of a group of tests for a Benthos config file.
type Definition struct {
	Parallel bool   `yaml:"parallel"`
	Cases    []Case `yaml:"tests"`
}

// ExampleDefinition returns a Definition containing an example case.
func ExampleDefinition() Definition {
	return Definition{
		Parallel: true,
		Cases:    []Case{NewCase()},
	}
}

//------------------------------------------------------------------------------

// ExecuteWithLogger attempts to run a test definition on a target config file,
// with a logger. Returns an array of test failures or an error.
func (d Definition) ExecuteWithLogger(filepath string, logger log.Modular) ([]CaseFailure, error) {
	return d.execute(filepath, logger)
}

// Execute attempts to run a test definition on a target config file. Returns
// an array of test failures or an error.
func (d Definition) Execute(filepath string) ([]CaseFailure, error) {
	return d.execute(filepath, log.Noop())
}

func (d Definition) execute(filepath string, logger log.Modular) ([]CaseFailure, error) {
	procsProvider := NewProcessorsProvider(filepath, OptProcessorsProviderSetLogger(logger))
	if d.Parallel {
		// Warm the cache of processor configs.
		for _, c := range d.Cases {
			if _, err := procsProvider.getConfs(c.TargetProcessors, c.Environment); err != nil {
				return nil, err
			}
		}
	}

	var totalFailures []CaseFailure
	if !d.Parallel {
		for i, c := range d.Cases {
			failures, err := c.Execute(procsProvider)
			if err != nil {
				return nil, fmt.Errorf("test case %v failed: %v", i, err)
			}
			totalFailures = append(totalFailures, failures...)
		}
	} else {
		var g errgroup.Group

		failureSlices := make([][]CaseFailure, len(d.Cases))
		for i, c := range d.Cases {
			i := i
			c := c
			g.Go(func() error {
				failures, err := c.Execute(procsProvider)
				if err != nil {
					return fmt.Errorf("test case %v failed: %v", i, err)
				}
				failureSlices[i] = failures
				return nil
			})
		}

		// Wait for all test cases to complete.
		if err := g.Wait(); err != nil {
			return nil, err
		}

		for _, fs := range failureSlices {
			totalFailures = append(totalFailures, fs...)
		}
	}

	return totalFailures, nil
}

//------------------------------------------------------------------------------
