package test

import (
	"fmt"
	"path/filepath"

	"github.com/benthosdev/benthos/v4/internal/log"
)

// Definition of a group of tests for a Benthos config file.
type Definition struct {
	Cases []Case `yaml:"tests"`
}

// Execute the test definition.
func (d Definition) Execute(testFilePath string, resourcesPaths []string, logger log.Modular) ([]CaseFailure, error) {
	procsProvider := NewProcessorsProvider(
		testFilePath,
		OptAddResourcesPaths(resourcesPaths),
		OptProcessorsProviderSetLogger(logger),
	)

	dir := filepath.Dir(testFilePath)

	var totalFailures []CaseFailure
	for i, c := range d.Cases {
		cleanupEnv := setEnvironment(c.Environment)
		failures, err := c.ExecuteFrom(dir, procsProvider)
		if err != nil {
			cleanupEnv()
			return nil, fmt.Errorf("test case %v failed: %v", i, err)
		}
		totalFailures = append(totalFailures, failures...)
		cleanupEnv()
	}

	return totalFailures, nil
}
