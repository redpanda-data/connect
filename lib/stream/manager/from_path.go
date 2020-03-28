package manager

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/stream"
)

//------------------------------------------------------------------------------

func loadFile(dir, path, testSuffix string, confs map[string]stream.Config) ([]string, error) {
	var id string
	if len(dir) > 0 {
		var err error
		if id, err = filepath.Rel(dir, path); err != nil {
			return nil, err
		}
	} else {
		id = filepath.Base(path)
	}
	id = strings.Trim(id, string(filepath.Separator))
	id = strings.TrimSuffix(id, ".yaml")
	id = strings.TrimSuffix(id, ".yml")

	// Do not run unit test files
	if len(testSuffix) > 0 && strings.HasSuffix(id, testSuffix) {
		return nil, nil
	}

	id = strings.Replace(id, string(filepath.Separator), "_", -1)

	if _, exists := confs[id]; exists {
		return nil, fmt.Errorf("stream id (%v) collision from file: %v", id, path)
	}

	conf := config.New()
	lints, err := config.Read(path, true, &conf)
	if err != nil {
		return nil, err
	}
	for i := range lints {
		lints[i] = path + ": " + lints[i]
	}

	confs[id] = conf.Config
	return lints, nil
}

// LoadStreamConfigsFromPath reads a map of stream ids to configurations
// by either walking a directory of .json and .yaml files or by reading a file
// directly. Returns linting errors prefixed with their path.
func LoadStreamConfigsFromPath(target, testSuffix string, streamMap map[string]stream.Config) ([]string, error) {
	pathLints := []string{}
	target = filepath.Clean(target)

	if info, err := os.Stat(target); err != nil {
		return nil, err
	} else if !info.IsDir() {
		if pathLints, err = loadFile("", target, "", streamMap); err != nil {
			return nil, fmt.Errorf("failed to load config '%v': %v", target, err)
		}
		return pathLints, nil
	}

	err := filepath.Walk(target, func(path string, info os.FileInfo, werr error) error {
		if werr != nil {
			return werr
		}
		if info.IsDir() ||
			(!strings.HasSuffix(info.Name(), ".yaml") &&
				!strings.HasSuffix(info.Name(), ".yml")) {
			return nil
		}

		var lints []string
		if lints, werr = loadFile(target, path, testSuffix, streamMap); werr != nil {
			return fmt.Errorf("failed to load config '%v': %v", path, werr)
		}

		pathLints = append(pathLints, lints...)
		return nil
	})

	return pathLints, err
}

//------------------------------------------------------------------------------
