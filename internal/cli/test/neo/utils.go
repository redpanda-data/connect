package neo

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/fatih/color"

	"github.com/benthosdev/benthos/v4/internal/config/test"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
)

var (
	Green  = color.New(color.FgGreen).SprintFunc()
	Red    = color.New(color.FgRed).SprintFunc()
	Yellow = color.New(color.FgYellow).SprintFunc()
)

type ExecStatus string

var (
	Passed  = ExecStatus("passed")
	Failed  = ExecStatus("failed")
	Errored = ExecStatus("errored")
)

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

func getDefinition(targetPath, definitionPath string) ([]test.Case, error) {
	if _, err := ifs.OS().Stat(targetPath); err != nil {
		return nil, fmt.Errorf("unable to access target config file '%v': %v", targetPath, err)
	}
	if _, err := ifs.OS().Stat(definitionPath); err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("unable to access test definition file '%v': %v", definitionPath, err)
		}
		if !strings.HasSuffix(targetPath, ".yaml") && !strings.HasSuffix(targetPath, ".yml") {
			return nil, nil
		}
		definitionPath = targetPath
	}
	defBytes, err := ifs.ReadFile(ifs.OS(), definitionPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read test definition from '%v': %v", definitionPath, err)
	}

	node, err := docs.UnmarshalYAML(defBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse test definition from '%v': %v", definitionPath, err)
	}

	cases, err := test.FromAny(node)
	if err != nil {
		return nil, fmt.Errorf("failed to parse test definition from '%v': %v", definitionPath, err)
	}
	return cases, nil
}
