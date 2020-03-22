package test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/fatih/color"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

var green = color.New(color.FgGreen).SprintFunc()
var red = color.New(color.FgRed).SprintFunc()
var yellow = color.New(color.FgYellow).SprintFunc()
var blue = color.New(color.FgBlue).SprintFunc()

//------------------------------------------------------------------------------

func getBothPaths(fullPath, testSuffix string) (configPath string, definitionPath string) {
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

// Searches for test definition targets.
func getTestTargets(targetPath, testSuffix string, recurse bool) ([]string, error) {
	targetPath = filepath.Clean(targetPath)
	info, err := os.Stat(targetPath)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		configPath, definitionPath := getBothPaths(targetPath, testSuffix)
		if _, err = os.Stat(configPath); err != nil {
			return nil, fmt.Errorf("unable to access target config file '%v': %v", configPath, err)
		}
		if _, err = os.Stat(definitionPath); err != nil {
			return nil, fmt.Errorf("unable to access test definition file '%v': %v", definitionPath, err)
		}
		return []string{definitionPath}, nil
	}

	pathMap := map[string]struct{}{}
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

		configPath, definitionPath := getBothPaths(path, testSuffix)
		if _, exists := pathMap[definitionPath]; exists {
			return nil
		}

		if _, err = os.Stat(configPath); err != nil {
			return fmt.Errorf("unable to access target config file '%v': %v", configPath, err)
		}
		if _, err = os.Stat(definitionPath); err != nil {
			if os.IsNotExist(err) {
				// We simply skip files where there is no test definition.
				return nil
			}
			return fmt.Errorf("unable to access test definition file '%v': %v", definitionPath, err)
		}
		pathMap[definitionPath] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, err
	}

	list := []string{}
	for k := range pathMap {
		list = append(list, k)
	}
	sort.Strings(list)
	return list, nil
}

// Executes a test definition and either returns fails or returns an error.
func testTarget(path, testSuffix string) ([]CaseFailure, error) {
	confPath, _ := getBothPaths(path, testSuffix)
	var definition Definition
	defBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read test definition '%v': %v", path, err)
	}
	if err := yaml.Unmarshal(defBytes, &definition); err != nil {
		return nil, fmt.Errorf("failed to parse test definition '%v': %v", path, err)
	}
	return definition.Execute(confPath)
}

// Lints the config target of a test definition and either returns linting
// errors (false for failed) or returns an error.
func lintTarget(path, testSuffix string) ([]string, error) {
	confPath, _ := getBothPaths(path, testSuffix)
	dummyConf := config.New()
	lints, err := config.Read(confPath, true, &dummyConf)
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
	var targets []string

	for _, path := range paths {
		var recurse bool
		path, recurse = resolveTestPath(path)
		lTargets, err := getTestTargets(path, testSuffix, recurse)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to obtain test targets: %v\n", err)
			return false
		}
		targets = append(targets, lTargets...)
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

	var err error
	for _, target := range targets {
		var lints []string
		var failCases []CaseFailure
		if lint {
			if lints, err = lintTarget(target, testSuffix); err != nil {
				fmt.Fprintf(os.Stderr, "Failed to execute test target '%v': %v\n", target, err)
				return false
			}
		}
		if failCases, err = testTarget(target, testSuffix); err != nil {
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
