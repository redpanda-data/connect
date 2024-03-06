package test

import (
	"fmt"
	"io"
	"sort"

	"github.com/benthosdev/benthos/v4/internal/cli/test/neo"
)

type neoRunner struct {
	w  io.Writer
	ew io.Writer
}

func (n *neoRunner) Run(config RunConfig) bool {
	formatter, err := neo.GetFormatter(config.Format, n.w, n.ew)
	if err != nil {
		formatter.Error(fmt.Errorf("failed to obtain formatter: %v", err))
		return false
	}

	suites, err := neo.GetTestSuites(config.Paths, config.TestSuffix)
	if err != nil {
		formatter.Error(fmt.Errorf("failed to obtain test targets: %v", err))
		return false
	}
	if len(suites) == 0 {
		formatter.Warn("No tests were found")
		return false
	}

	suiteIds := make([]string, 0, len(suites))
	for k := range suites {
		suiteIds = append(suiteIds, k)
	}
	sort.Strings(suiteIds)

	executions := map[string]*neo.SuiteExecution{}
	for _, sid := range suiteIds {
		suite := suites[sid]

		executions[sid] = suite.Run(config.Lint, config.TestSuffix, config.Logger, config.ResourcePaths)
	}

	formatter.Render(executions)

	for _, exec := range executions {
		if exec.HasFailures() {
			return false
		}
	}

	return true
}
