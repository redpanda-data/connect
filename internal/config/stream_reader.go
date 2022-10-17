package config

import (
	"bytes"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	tdocs "github.com/benthosdev/benthos/v4/internal/cli/test/docs"
	"github.com/benthosdev/benthos/v4/internal/docs"
	ifilepath "github.com/benthosdev/benthos/v4/internal/filepath"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
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

// ReadStreamFile attempts to read a stream config and returns the result.
func ReadStreamFile(path string) (conf stream.Config, lints []string, err error) {
	conf = stream.NewConfig()

	var confBytes []byte
	var dLints []docs.Lint
	if confBytes, dLints, err = ReadFileEnvSwap(path); err != nil {
		return
	}
	for _, l := range dLints {
		lints = append(lints, l.Error())
	}

	var rawNode yaml.Node
	if err = yaml.Unmarshal(confBytes, &rawNode); err != nil {
		return
	}

	confSpec := stream.Spec()
	confSpec = append(confSpec, tdocs.ConfigSpec())

	if !bytes.HasPrefix(confBytes, []byte("# BENTHOS LINT DISABLE")) {
		for _, lint := range confSpec.LintYAML(docs.NewLintContext(), &rawNode) {
			lints = append(lints, fmt.Sprintf("%v%v", path, lint.Error()))
		}
	}

	err = rawNode.Decode(&conf)
	return
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

	if _, exists := confs[id]; exists {
		return nil, fmt.Errorf("stream id (%v) collision from file: %v", id, path)
	}

	conf, lints, err := ReadStreamFile(path)
	if err != nil {
		return nil, err
	}

	strmInfo := streamFileInfo{id: id}
	// This is an unlikely race condition, see readMain for more info.
	strmInfo.updatedAt = time.Now()

	r.streamFileInfo[path] = strmInfo

	confs[id] = conf
	return lints, nil
}

func (r *Reader) streamPathsExpanded() ([][2]string, error) {
	streamsPaths, err := ifilepath.Globs(ifs.OS(), r.streamsPaths)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve stream glob pattern: %w", err)
	}

	var paths [][2]string
	for _, target := range streamsPaths {
		target = filepath.Clean(target)

		if info, err := ifs.OS().Stat(target); err != nil {
			return nil, err
		} else if !info.IsDir() {
			paths = append(paths, [2]string{"", target})
			continue
		}

		if err := fs.WalkDir(ifs.OS(), target, func(path string, info fs.DirEntry, werr error) error {
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

	conf, lints, err := ReadStreamFile(path)
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

	return r.streamUpdateFn(info.id, conf)
}
