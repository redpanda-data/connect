package config

import (
	"bytes"
	"errors"
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
	"github.com/benthosdev/benthos/v4/internal/stream"
)

// inferStreamID attempts to infer a stream identifier from a file path and
// containing directory. If the dir field is non-empty then the identifier will
// include all sub-directories in the path as an id prefix, this means loading
// streams with the same file name from different branches are still given
// unique names.
func inferStreamID(dir, path string) (string, error) {
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

func (r *Reader) readStreamFileConfig(path string) (conf stream.Config, lints []string, err error) {
	conf = stream.NewConfig()

	var confBytes []byte
	var dLints []docs.Lint
	var modTime time.Time
	if confBytes, dLints, modTime, err = ReadFileEnvSwap(r.fs, path); err != nil {
		return
	}
	for _, l := range dLints {
		lints = append(lints, l.Error())
	}
	r.modTimeLastRead[path] = modTime

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

func (r *Reader) readStreamFile(id, path string, confs map[string]stream.Config) ([]string, error) {
	if id == "" {
		return nil, fmt.Errorf("stream id could not be inferred from file: %v", path)
	}
	if _, exists := confs[id]; exists {
		return nil, fmt.Errorf("stream id (%v) collision from file: %v", id, path)
	}

	conf, lints, err := r.readStreamFileConfig(path)
	if err != nil {
		return nil, err
	}

	confs[id] = conf
	return lints, nil
}

func (r *Reader) streamPathsExpanded() ([]string, error) {
	streamsPaths, err := ifilepath.Globs(r.fs, r.streamsPaths)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve stream glob pattern: %w", err)
	}

	var paths []string
	for _, target := range streamsPaths {
		target = filepath.Clean(target)

		if info, err := r.fs.Stat(target); err != nil {
			return nil, err
		} else if !info.IsDir() {
			id, err := inferStreamID("", target)
			if err != nil {
				return nil, err
			}

			if _, exists := r.streamFileInfo[target]; !exists {
				r.streamFileInfo[target] = streamFileInfo{id: id}
			}
			paths = append(paths, target)
			continue
		}

		if err := fs.WalkDir(r.fs, target, func(path string, info fs.DirEntry, werr error) error {
			if werr != nil {
				return werr
			}
			if info.IsDir() ||
				(!strings.HasSuffix(info.Name(), ".yaml") &&
					!strings.HasSuffix(info.Name(), ".yml")) {
				return nil
			}

			id, err := inferStreamID(target, path)
			if err != nil {
				return err
			}

			// TODO: This is quite lazy and might run into issues e.g. the path
			// `foo/bar.yaml` would collide with a test suffix of `_bar`.
			if len(r.testSuffix) > 0 && strings.HasSuffix(id, r.testSuffix) {
				return nil
			}

			path = filepath.Clean(path)
			if _, exists := r.streamFileInfo[path]; !exists {
				r.streamFileInfo[path] = streamFileInfo{id: id}
			}
			paths = append(paths, path)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return paths, nil
}

func (r *Reader) readStreamFiles(streamMap map[string]stream.Config) (pathLints []string, err error) {
	var streamsPaths []string
	if streamsPaths, err = r.streamPathsExpanded(); err != nil {
		return nil, err
	}

	for _, target := range streamsPaths {
		tmpPathLints, err := r.readStreamFile(r.streamFileInfo[target].id, target, streamMap)
		if err != nil {
			return nil, fmt.Errorf("failed to load config '%v': %v", target, err)
		}
		pathLints = append(pathLints, tmpPathLints...)
	}
	return
}

func (r *Reader) findStreamPathWalkedDir(streamPath string) (dir string) {
	for _, p := range r.streamsPaths {
		if strings.HasPrefix(streamPath, p) && len(p) > len(dir) {
			dir = p
		}
	}
	return
}

// TriggerStreamUpdate attempts to re-read a stream configuration file, and
// trigger the provided stream update func.
func (r *Reader) TriggerStreamUpdate(mgr bundle.NewManagement, strict bool, path string) error {
	if r.streamUpdateFn == nil {
		return nil
	}

	conf, lints, err := r.readStreamFileConfig(path)
	if errors.Is(err, fs.ErrNotExist) {
		info, exists := r.streamFileInfo[path]
		if !exists {
			return nil
		}
		mgr.Logger().Infof("Stream %v config deleted, attempting to remove stream.", info.id)

		if err := r.streamUpdateFn(info.id, nil); err != nil {
			mgr.Logger().Errorf("Failed to remove deleted stream %v config: %v", info.id, err)
			return err
		}
		mgr.Logger().Infof("Removed stream %v.", info.id)
		return nil
	}
	if err != nil {
		mgr.Logger().Errorf("Failed to read updated stream config: %v", err)
		return noReread(err)
	}

	info, exists := r.streamFileInfo[path]
	if exists {
		mgr.Logger().Infof("Stream %v config updated, attempting to update stream.", info.id)
	} else {
		id, err := inferStreamID(r.findStreamPathWalkedDir(path), path)
		if err != nil {
			return err
		}
		info = streamFileInfo{id: id}
		r.streamFileInfo[path] = info
		mgr.Logger().Infof("Stream %v config added, attempting to create stream.", info.id)
	}

	lintlog := mgr.Logger()
	for _, lint := range lints {
		lintlog.Infoln(lint)
	}
	if strict && len(lints) > 0 {
		mgr.Logger().Errorf("Rejecting updated stream %v config due to linter errors, to allow linting errors run Benthos with --chilled.", info.id)
		return noReread(errors.New("file contained linting errors and is running in strict mode"))
	}

	if err := r.streamUpdateFn(info.id, &conf); err != nil {
		mgr.Logger().Errorf("Failed to apply updated stream %v config: %v", info.id, err)
		return err
	}
	mgr.Logger().Infof("Updated stream %v config from file.", info.id)
	return nil
}
