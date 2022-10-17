package manager

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/stream"
)

func loadFile(dir, path, testSuffix string, confs map[string]stream.Config) ([]string, error) {
	id, err := config.InferStreamID(dir, path)
	if err != nil {
		return nil, err
	}

	// Do not run unit test files
	if len(testSuffix) > 0 && strings.HasSuffix(id, testSuffix) {
		return nil, nil
	}

	if _, exists := confs[id]; exists {
		return nil, fmt.Errorf("stream id (%v) collision from file: %v", id, path)
	}

	conf, lints, err := config.ReadStreamFile(path)
	if err != nil {
		return nil, err
	}

	confs[id] = conf
	return lints, nil
}

// LoadStreamConfigsFromPath reads a map of stream ids to configurations
// by either walking a directory of .json and .yaml files or by reading a file
// directly. Returns linting errors prefixed with their path.
//
// Deprecated: The streams builder is using ./internal/config now.
func LoadStreamConfigsFromPath(target, testSuffix string, streamMap map[string]stream.Config) ([]string, error) {
	pathLints := []string{}
	target = filepath.Clean(target)

	if info, err := ifs.OS().Stat(target); err != nil {
		return nil, err
	} else if !info.IsDir() {
		if pathLints, err = loadFile("", target, "", streamMap); err != nil {
			return nil, fmt.Errorf("failed to load config '%v': %v", target, err)
		}
		return pathLints, nil
	}

	err := fs.WalkDir(ifs.OS(), target, func(path string, info fs.DirEntry, werr error) error {
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
