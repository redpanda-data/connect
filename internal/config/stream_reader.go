package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	tdocs "github.com/benthosdev/benthos/v4/internal/cli/test/docs"
	"github.com/benthosdev/benthos/v4/internal/docs"
	ifilepath "github.com/benthosdev/benthos/v4/internal/filepath"
	"github.com/benthosdev/benthos/v4/internal/stream"
)

// InferStreamID attempts to infer a stream identifier from a file path and
// containing directory. If the dir field is non-empty then the identifier will
// include all sub-directories in the path as an id prefix, this means loading
// streams with the same file name from different branches are still given
// unique names.
func InferStreamID(dir, path string) (string, error) {
	var id string
	if len(dir) > 0 {
		var err error
		if id, err = filepath.Rel(dir, path); err != nil {
			return "", err
		}
	} else {
		id = filepath.Base(path)
	}

	id = strings.Trim(id, string(filepath.Separator))
	id = strings.TrimSuffix(id, ".yaml")
	id = strings.TrimSuffix(id, ".yml")
	id = strings.ReplaceAll(id, string(filepath.Separator), "_")

	return id, nil
}

// ReadStreamFile attempts to read stream configs and returns the result
// but bails at the first error while decoding
func ReadStreamFile(path string) (confs []stream.Config, lints []string, err error) {
	var confBytes []byte
	if confBytes, lints, err = ReadFileEnvSwap(path); err != nil {
		return
	}

	enableLint := !bytes.HasPrefix(confBytes, []byte("# BENTHOS LINT DISABLE"))
	decoder := yaml.NewDecoder(bytes.NewBuffer(confBytes))
	for {
		conf := stream.NewConfig()
		var rawNode yaml.Node
		if err = decoder.Decode(&rawNode); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return
		}

		confSpec := stream.Spec()
		confSpec = append(confSpec, tdocs.ConfigSpec())
		if enableLint {
			for _, lint := range confSpec.LintYAML(docs.NewLintContext(), &rawNode) {
				lints = append(lints, fmt.Sprintf("%v: line %v: %v", path, lint.Line, lint.What))
			}
		}

		err = rawNode.Decode(&conf)
		if err != nil {
			return
		}
		confs = append(confs, conf)
	}
}

func IndexedStreamID(id string, idx int) string {
	return fmt.Sprintf("%v@%d", id, idx)
}

func (r *Reader) readStreamFile(dir, path string, confs map[string]stream.Config) ([]string, error) {
	id, err := InferStreamID(dir, path)
	if err != nil {
		return nil, err
	}

	// Do not run unit test files
	if len(r.testSuffix) > 0 && strings.HasSuffix(id, r.testSuffix) {
		return nil, nil
	}

	parsedConfs, lints, err := ReadStreamFile(path)
	if err != nil {
		return nil, err
	}

	strmInfo := streamFileInfo{id: id}
	// This is an unlikely race condition, see readMain for more info.
	strmInfo.updatedAt = time.Now()

	r.streamFileInfo[path] = strmInfo

	if len(parsedConfs) > 1 {
		for idx, conf := range parsedConfs {
			id := IndexedStreamID(id, idx)
			if _, exists := confs[id]; exists {
				return nil, fmt.Errorf("stream id (%v) collision from file: %v", id, path)
			}
			confs[id] = conf
		}
	} else if len(parsedConfs) > 0 {
		if _, exists := confs[id]; exists {
			return nil, fmt.Errorf("stream id (%v) collision from file: %v", id, path)
		}
		confs[id] = parsedConfs[0]
	}
	return lints, nil
}

func (r *Reader) streamPathsExpanded() ([][2]string, error) {
	streamsPaths, err := ifilepath.Globs(r.streamsPaths)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve stream glob pattern: %w", err)
	}

	var paths [][2]string
	for _, target := range streamsPaths {
		target = filepath.Clean(target)

		if info, err := os.Stat(target); err != nil {
			return nil, err
		} else if !info.IsDir() {
			paths = append(paths, [2]string{"", target})
			continue
		}

		if err := filepath.Walk(target, func(path string, info os.FileInfo, werr error) error {
			if werr != nil {
				return werr
			}
			if info.IsDir() ||
				(!strings.HasSuffix(info.Name(), ".yaml") &&
					!strings.HasSuffix(info.Name(), ".yml")) {
				return nil
			}

			paths = append(paths, [2]string{target, path})
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return paths, nil
}

func (r *Reader) readStreamFiles(streamMap map[string]stream.Config) (pathLints []string, err error) {
	var streamsPaths [][2]string
	if streamsPaths, err = r.streamPathsExpanded(); err != nil {
		return nil, err
	}

	for _, target := range streamsPaths {
		tmpPathLints, err := r.readStreamFile(target[0], target[1], streamMap)
		if err != nil {
			return nil, fmt.Errorf("failed to load config '%v': %v", target, err)
		}
		pathLints = append(pathLints, tmpPathLints...)
	}
	return
}

func (r *Reader) reactStreamUpdate(mgr bundle.NewManagement, strict bool, path string) bool {
	if r.streamUpdateFn == nil {
		return true
	}

	info, exists := r.streamFileInfo[path]
	if !exists {
		mgr.Logger().Warnf("Skipping resource update for unknown path: %v", path)
		return true
	}

	mgr.Logger().Infof("Stream %v config updated, attempting to update stream.", info.id)

	confs, lints, err := ReadStreamFile(path)
	if err != nil {
		mgr.Logger().Errorf("Failed to read updated stream config: %v", err)
		return true
	}

	lintlog := mgr.Logger()
	for _, lint := range lints {
		lintlog.Infoln(lint)
	}
	if strict && len(lints) > 0 {
		mgr.Logger().Errorf("Rejecting updated stream %v config due to linter errors, to allow linting errors run Benthos with --chilled", info.id)
		return true
	}

	return r.streamUpdateFn(info.id, confs)
}
