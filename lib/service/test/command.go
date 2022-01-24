package test

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/fatih/color"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

var green = color.New(color.FgGreen).SprintFunc()
var red = color.New(color.FgRed).SprintFunc()
var yellow = color.New(color.FgYellow).SprintFunc()
var blue = color.New(color.FgBlue).SprintFunc()

//------------------------------------------------------------------------------

// GetPathPair returns the config path and expected accompanying test definition
// path for a given syntax and a path for either file.
func GetPathPair(fullPath, testSuffix string) (configPath, definitionPath string) {
	path, file := filepath.Split(fullPath)
	ext := filepath.Ext(file)
	filename := strings.TrimSuffix(file, ext)
	if strings.HasSuffix(filename, testSuffix) {
		definitionPath = filepath.Clean(fullPath)
		configPath = filepath.Join(path, strings.TrimSuffix(filename, testSuffix)+ext)
	} else {
		configPath = filepath.Clean(fullPath)
		definitionPath = filepath.Join(path, filename+testSuffix+ext)
	}
	return
}

func getDefinition(targetPath, definitionPath string) (*Definition, error) {
	if _, err := os.Stat(targetPath); err != nil {
		return nil, fmt.Errorf("unable to access target config file '%v': %v", targetPath, err)
	}
	if _, err := os.Stat(definitionPath); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("unable to access test definition file '%v': %v", definitionPath, err)
		}
		if !strings.HasSuffix(targetPath, ".yaml") && !strings.HasSuffix(targetPath, ".yml") {
			return &Definition{}, nil
		}
		definitionPath = targetPath
	}
	var definition Definition
	defBytes, err := os.ReadFile(definitionPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read test definition from '%v': %v", definitionPath, err)
	}
	if err := yaml.Unmarshal(defBytes, &definition); err != nil {
		return nil, fmt.Errorf("failed to parse test definition from '%v': %v", definitionPath, err)
	}
	return &definition, nil
}

// GetTestTargets searches for test definition targets in a path with a given
// test suffix.
func GetTestTargets(targetPath, testSuffix string, recurse bool) (map[string]Definition, error) {
	targetPath = filepath.Clean(targetPath)
	info, err := os.Stat(targetPath)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		configPath, definitionPath := GetPathPair(targetPath, testSuffix)
		def, err := getDefinition(configPath, definitionPath)
		if err != nil {
			return nil, err
		}
		if len(def.Cases) == 0 {
			return nil, fmt.Errorf("no tests found for %v", configPath)
		}
		return map[string]Definition{
			configPath: *def,
		}, nil
	}

	pathMap := map[string]Definition{}
	err = filepath.Walk(targetPath, func(path string, info os.FileInfo, werr error) error {
		if werr != nil {
			return werr
		}
		if info.IsDir() {
			if recurse || path == targetPath {
				return nil
			}
			return filepath.SkipDir
		}
		configPath, definitionPath := GetPathPair(path, testSuffix)
		if _, exists := pathMap[configPath]; exists {
			return nil
		}
		def, err := getDefinition(configPath, definitionPath)
		if err != nil {
			return err
		}
		pathMap[configPath] = *def
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Tests without cases are skipped.
	for k, v := range pathMap {
		if len(v.Cases) == 0 {
			delete(pathMap, k)
		}
	}
	return pathMap, nil
}

// Lints the config target of a test definition and either returns linting
// errors (false for failed) or returns an error.
func lintTarget(path, testSuffix string) ([]string, error) {
	confPath, _ := GetPathPair(path, testSuffix)
	dummyConf := config.New()
	lints, err := config.ReadV2(confPath, true, false, &dummyConf)
	if err != nil {
		return nil, err
	}
	return lints, nil
}

//------------------------------------------------------------------------------

func resolveTestPath(path string) (string, bool) {
	recurse := false
	if path == "./..." || path == "..." {
		recurse = true
		path = "."
	}
	if strings.HasSuffix(path, "/...") {
		recurse = true
		path = strings.TrimSuffix(path, "/...")
	}
	return path, recurse
}

// Run executes the test command for a specified path. The path can either be a
// config file, a config files test definition file, a directory, or the
// wildcard pattern './...'.
func Run(path, testSuffix string, lint bool) bool {
	return RunAll([]string{path}, testSuffix, lint)
}

// RunAll executes the test command for a slice of paths. The path can either be
// a config file, a config files test definition file, a directory, or the
// wildcard pattern './...'.
func RunAll(paths []string, testSuffix string, lint bool) bool {
	return runAll(paths, testSuffix, lint, log.Noop(), nil)
}

// RunAllWithLogger executes the test command for a slice of paths. The path can
// either be a config file, a config files test definition file, a directory, or
// the wildcard pattern './...'.
func RunAllWithLogger(paths []string, testSuffix string, lint bool, logger log.Modular) bool {
	return runAll(paths, testSuffix, lint, logger, nil)
}

func runAll(paths []string, testSuffix string, lint bool, logger log.Modular, resourcesPaths []string) bool {
	targets := map[string]Definition{}

	for _, path := range paths {
		var recurse bool
		path, recurse = resolveTestPath(path)
		lTargets, err := GetTestTargets(path, testSuffix, recurse)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to obtain test targets: %v\n", err)
			return false
		}
		for k, v := range lTargets {
			targets[k] = v
		}
	}

	if len(targets) == 0 {
		fmt.Printf("%v\n", yellow("No tests were found"))
		return false
	}

	type failedTarget struct {
		target string
		lints  []string
		cases  []CaseFailure
	}
	fails := []failedTarget{}

	targetPaths := make([]string, 0, len(targets))
	for k := range targets {
		targetPaths = append(targetPaths, k)
	}
	sort.Strings(targetPaths)

	var err error
	for _, target := range targetPaths {
		var lints []string
		var failCases []CaseFailure
		if lint {
			if lints, err = lintTarget(target, testSuffix); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to execute test target '%v': %v\n", target, err)
				return false
			}
		}
		if failCases, err = targets[target].execute(target, resourcesPaths, logger); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to execute test target '%v': %v\n", target, err)
			return false
		}
		if len(lints) > 0 || len(failCases) > 0 {
			fails = append(fails, failedTarget{
				target: target,
				lints:  lints,
				cases:  failCases,
			})
			fmt.Printf("Test '%v' %v\n", target, red("failed"))
		} else {
			fmt.Printf("Test '%v' %v\n", target, green("succeeded"))
		}
	}
	if len(fails) > 0 {
		fmt.Printf("\nFailures:\n\n")
		for i, fail := range fails {
			if i > 0 {
				fmt.Println("")
			}
			fmt.Printf("--- %v ---\n\n", fail.target)
			for _, lint := range fail.lints {
				fmt.Printf("Lint: %v\n", lint)
			}
			if len(fail.cases) > 0 {
				if len(fail.lints) > 0 {
					fmt.Println("")
				}
				var namePrev string
				for i, fail := range fail.cases {
					if namePrev != fail.Name {
						if i > 0 {
							fmt.Println("")
						}
						fmt.Printf("%v [line %v]:\n", fail.Name, fail.TestLine)
						namePrev = fail.Name
					}
					fmt.Println(fail.Reason)
				}
			}
		}
		return false
	}
	return true
}

//------------------------------------------------------------------------------
