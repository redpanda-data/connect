package neo

import (
	"path/filepath"
	"sort"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/config/test"
	"github.com/benthosdev/benthos/v4/internal/docs"
	ifilepath "github.com/benthosdev/benthos/v4/internal/filepath"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/log"
)

type Suite struct {
	Name  string
	Path  string
	Cases map[string]test.Case
}

func (t *Suite) Run(shouldLint bool, testSuffix string, logger log.Modular, resourcesPaths []string) *SuiteExecution {
	var err error

	exec := &SuiteExecution{
		Suite:  t,
		Cases:  map[string]*CaseExecution{},
		Lints:  []docs.Lint{},
		Status: Passed, // assume success
	}

	if shouldLint {
		if exec.Lints, err = t.runLint(testSuffix); err != nil {
			exec.Err = err
			exec.Status = Errored
			return exec
		}
	}

	exec.Cases = t.runCases(logger, resourcesPaths)
	for _, ce := range exec.Cases {
		if ce.Status == Errored {
			exec.Status = Errored
			break
		} else if ce.Status == Failed {
			exec.Status = Failed
			break
		}
	}

	return exec
}

// Lints the config target of a test definition and either returns linting
// errors (false for Failed) or returns an error.
func (t *Suite) runLint(testSuffix string) ([]docs.Lint, error) {
	confPath, _ := GetPathPair(t.Path, testSuffix)

	// This is necessary as each test case can provide a different set of
	// environment variables, so in order to test env vars properly we would
	// need to runLint for each case.
	skipEnvVarCheck := true
	_, lints, err := config.ReadYAMLFileLinted(ifs.OS(), config.Spec(), confPath, skipEnvVarCheck, docs.NewLintConfig(bundle.GlobalEnvironment))
	if err != nil {
		return nil, err
	}
	return lints, nil
}

func (t *Suite) runCases(logger log.Modular, resourcesPaths []string) map[string]*CaseExecution {
	caseKeys := make([]string, 0, len(t.Cases))
	for k := range t.Cases {
		caseKeys = append(caseKeys, k)
	}
	sort.Strings(caseKeys)

	procsProvider := NewProcessorsProvider(
		t.Path,
		OptAddResourcesPaths(resourcesPaths),
		OptProcessorsProviderSetLogger(logger),
	)

	dir := filepath.Dir(t.Path)

	executions := map[string]*CaseExecution{}
	for _, c := range t.Cases {
		var ce *CaseExecution
		cleanupEnv := setEnvironment(c.Environment)
		ce = RunCase(ifs.OS(), dir, c, procsProvider)
		cleanupEnv()

		executions[c.Name] = ce
	}

	return executions
}

type SuiteExecution struct {
	*Suite
	Status ExecStatus
	Cases  map[string]*CaseExecution
	Lints  []docs.Lint
	Err    error
}

func (e *SuiteExecution) HasFailures() bool {
	if e.Err != nil {
		return true
	}

	if len(e.Lints) > 0 {
		return true
	}

	for _, ce := range e.Cases {
		if ce.Status != Passed {
			return true
		}
	}

	return false
}

func (e *SuiteExecution) CaseFailures() []*CaseExecution {
	var failures []*CaseExecution

	for _, ce := range e.Cases {
		failures = append(failures, ce)
	}

	return failures
}

// GetTestSuites searches for test definition targets in a path with a given
// test suffix.
func GetTestSuites(targetPaths []string, testSuffix string) (map[string]Suite, error) {
	targetPaths, err := ifilepath.GlobsAndSuperPaths(ifs.OS(), targetPaths, "yaml", "yml")
	if err != nil {
		return nil, err
	}

	targetDefinitions := map[string]Suite{}
	for _, tPath := range targetPaths {
		configPath, definitionPath := GetPathPair(tPath, testSuffix)
		def, err := getDefinition(configPath, definitionPath)
		if err != nil {
			return nil, err
		}
		if len(def) == 0 {
			continue
		}
		targetDefinitions[filepath.Clean(configPath)] = Suite{
			Name:  filepath.Base(configPath),
			Path:  filepath.Base(configPath),
			Cases: map[string]test.Case{},
		}
	}
	return targetDefinitions, nil
}
