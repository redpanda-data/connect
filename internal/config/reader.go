package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/stream"
	"github.com/Jeffail/gabs/v2"
	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v3"
)

const (
	defaultChangeFlushPeriod = 50 * time.Millisecond
	defaultChangeDelayPeriod = time.Second
)

type configFileInfo struct {
	updatedAt time.Time
}

type streamFileInfo struct {
	configFileInfo

	id string
}

// Reader provides utilities for parsing a Benthos config as a main file with
// a collection of resource files, and options such as overrides.
type Reader struct {
	// The suffix given to unit test definition files, this is used in order to
	// exclude unit tests from being run in streams mode with arbitrary
	// directory walking.
	testSuffix string

	mainPath      string
	resourcePaths []string
	streamsPaths  []string
	overrides     []string

	// Controls whether the main config should include input, output, etc.
	streamsMode bool

	// Tracks the details of the config file when we last read it.
	configFileInfo configFileInfo

	// Tracks the details of stream config files when we last read them.
	streamFileInfo map[string]streamFileInfo

	// Tracks the details of resource config files when we last read them,
	// including information such as the specific resources that were created
	// from it.
	resourceFileInfo    map[string]resourceFileInfo
	resourceFileInfoMut sync.Mutex

	mainUpdateFn   MainUpdateFunc
	streamUpdateFn StreamUpdateFunc
	watcher        *fsnotify.Watcher

	changeFlushPeriod time.Duration
	changeDelayPeriod time.Duration
}

// NewReader creates a new config reader.
func NewReader(mainPath string, resourcePaths []string, opts ...OptFunc) *Reader {
	r := &Reader{
		testSuffix:        "_benthos_test",
		mainPath:          mainPath,
		resourcePaths:     resourcePaths,
		streamFileInfo:    map[string]streamFileInfo{},
		resourceFileInfo:  map[string]resourceFileInfo{},
		changeFlushPeriod: defaultChangeFlushPeriod,
		changeDelayPeriod: defaultChangeDelayPeriod,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

//------------------------------------------------------------------------------

// OptFunc is an opt function that changes the behaviour of a config reader.
type OptFunc func(*Reader)

// OptTestSuffix configures the suffix given to unit test definition files, this
// is used in order to exclude unit tests from being run in streams mode with
// arbitrary directory walking.
func OptTestSuffix(suffix string) OptFunc {
	return func(r *Reader) {
		r.testSuffix = suffix
	}
}

// OptAddOverrides adds one or more override expressions to the config reader,
// each of the form `path=value`.
func OptAddOverrides(overrides ...string) OptFunc {
	return func(r *Reader) {
		r.overrides = append(r.overrides, overrides...)
	}
}

// OptSetStreamPaths marks this config reader as operating in streams mode, and
// adds a list of paths to obtain individual stream configs from.
func OptSetStreamPaths(streamsPaths ...string) OptFunc {
	return func(r *Reader) {
		r.streamsPaths = streamsPaths
		r.streamsMode = true
	}
}

//------------------------------------------------------------------------------

// Read a Benthos config from the files and options specified.
func (r *Reader) Read(conf *config.Type) (lints []string, err error) {
	if lints, err = r.readMain(conf); err != nil {
		return
	}
	var rLints []string
	if rLints, err = r.readResources(&conf.ResourceConfig); err != nil {
		return
	}
	lints = append(lints, rLints...)
	return
}

// ReadStreams attempts to read Benthos stream configs from one or more paths.
// Stream configs are extracted and added to a provided map, where the id is
// derived from the path of the stream config file.
func (r *Reader) ReadStreams(confs map[string]stream.Config) (lints []string, err error) {
	return r.readStreamFiles(confs)
}

// MainUpdateFunc is a closure function called whenever a main config has been
// updated. A boolean should be returned indicating whether the stream was
// successfully updated, if false then the attempt will be made again after a
// grace period.
type MainUpdateFunc func(conf stream.Config) bool

// SubscribeConfigChanges registers a closure function to be called whenever the
// main configuration file is updated.
//
// The provided closure should return true if the stream was successfully
// replaced.
func (r *Reader) SubscribeConfigChanges(fn MainUpdateFunc) error {
	if r.watcher != nil {
		return errors.New("a file watcher has already been started")
	}

	r.mainUpdateFn = fn
	return nil
}

// StreamUpdateFunc is a closure function called whenever a stream config has
// been updated. A boolean should be returned indicating whether the stream was
// successfully updated, if false then the attempt will be made again after a
// grace period.
type StreamUpdateFunc func(id string, conf stream.Config) bool

// SubscribeStreamChanges registers a closure to be called whenever the
// configuration of a stream is updated.
//
// The provided closure should return true if the stream was successfully
// replaced.
func (r *Reader) SubscribeStreamChanges(fn StreamUpdateFunc) error {
	if r.watcher != nil {
		return errors.New("a file watcher has already been started")
	}

	r.streamUpdateFn = fn
	return nil
}

// BeginFileWatching creates a goroutine that watches all active configuration
// files for changes. If a resource is changed then it is swapped out
// automatically through the provided manager. If a main config or stream config
// changes then the closures registered with either SubscribeConfigChanges or
// SubscribeStreamChanges will be called.
//
// WARNING: Either SubscribeConfigChanges or SubscribeStreamChanges must be
// called before this, as otherwise it is unsafe to register them during
// watching.
func (r *Reader) BeginFileWatching(mgr bundle.NewManagement, strict bool) error {
	if r.watcher != nil {
		return errors.New("a file watcher has already been started")
	}
	if r.mainUpdateFn == nil && r.streamUpdateFn == nil {
		return errors.New("a file watcher cannot be started without a subscription function registered")
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	r.watcher = watcher

	go func() {
		ticker := time.NewTicker(r.changeFlushPeriod)
		defer ticker.Stop()

		collapsedChanges := map[string]time.Time{}
		lostNames := map[string]struct{}{}
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				switch {
				case event.Op&fsnotify.Write == fsnotify.Write:
					collapsedChanges[filepath.Clean(event.Name)] = time.Now()

				case event.Op&fsnotify.Remove == fsnotify.Remove ||
					event.Op&fsnotify.Rename == fsnotify.Rename:
					_ = watcher.Remove(event.Name)
					lostNames[filepath.Clean(event.Name)] = struct{}{}
				}
			case <-ticker.C:
				for nameClean, changed := range collapsedChanges {
					if time.Since(changed) < r.changeDelayPeriod {
						continue
					}
					var succeeded bool
					if nameClean == filepath.Clean(r.mainPath) {
						succeeded = r.reactMainUpdate(mgr, strict)
					} else if _, exists := r.streamFileInfo[nameClean]; exists {
						succeeded = r.reactStreamUpdate(mgr, strict, nameClean)
					} else {
						succeeded = r.reactResourceUpdate(mgr, strict, nameClean)
					}
					if succeeded {
						delete(collapsedChanges, nameClean)
					} else {
						collapsedChanges[nameClean] = time.Now()
					}
				}
				for lostName := range lostNames {
					if err := watcher.Add(lostName); err == nil {
						collapsedChanges[lostName] = time.Now()
						delete(lostNames, lostName)
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				mgr.Logger().Errorf("Config watcher error: %v", err)
			}
		}
	}()

	if !r.streamsMode && r.mainPath != "" {
		if err := watcher.Add(r.mainPath); err != nil {
			_ = watcher.Close()
			return err
		}
	}
	for _, p := range r.streamsPaths {
		if err := watcher.Add(p); err != nil {
			_ = watcher.Close()
			return err
		}
	}
	for _, p := range r.resourcePaths {
		if err := watcher.Add(p); err != nil {
			_ = watcher.Close()
			return err
		}
	}
	return nil
}

// Close the reader, when this method exits all reloading will be stopped.
func (r *Reader) Close(ctx context.Context) error {
	if r.watcher != nil {
		return r.watcher.Close()
	}
	return nil
}

//------------------------------------------------------------------------------

func applyOverrides(specs docs.FieldSpecs, root *yaml.Node, overrides ...string) error {
	for _, override := range overrides {
		eqIndex := strings.Index(override, "=")
		if eqIndex == -1 {
			return fmt.Errorf("invalid set expression '%v': expected foo=bar syntax", override)
		}

		path := override[:eqIndex]
		value := override[eqIndex+1:]
		if path == "" || value == "" {
			return fmt.Errorf("invalid set expression '%v': expected foo=bar syntax", override)
		}

		valNode := yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: value,
		}
		if err := specs.SetYAMLPath(nil, root, &valNode, gabs.DotPathToSlice(path)...); err != nil {
			return fmt.Errorf("failed to set config field override: %w", err)
		}
	}
	return nil
}

func (r *Reader) readMain(conf *config.Type) (lints []string, err error) {
	defer func() {
		if err != nil && r.mainPath != "" {
			err = fmt.Errorf("%v: %w", r.mainPath, err)
		}
	}()

	if r.mainPath == "" && len(r.overrides) == 0 {
		return
	}

	var rawNode yaml.Node
	var confBytes []byte
	if r.mainPath != "" {
		if confBytes, lints, err = config.ReadBytes(r.mainPath, true); err != nil {
			return
		}
		if err = yaml.Unmarshal(confBytes, &rawNode); err != nil {
			return
		}
	}

	// This is an unlikely race condition as the file could've been updated
	// exactly when we were reading/linting. However, we'd need to fork
	// ReadWithJSONPointersLinted in order to pull the file info out, and since
	// it's going to be removed in V4 I'm just going with the simpler option for
	// now (ignoring the issue).
	r.configFileInfo.updatedAt = time.Now()

	confSpec := config.Spec()
	if r.streamsMode {
		// Spec is limited to just non-stream fields when in streams mode (no
		// input, output, etc)
		confSpec = config.SpecWithoutStream()
	}
	if err = applyOverrides(confSpec, &rawNode, r.overrides...); err != nil {
		return
	}

	if !bytes.HasPrefix(confBytes, []byte("# BENTHOS LINT DISABLE")) {
		lintFilePrefix := ""
		if r.mainPath != "" {
			lintFilePrefix = fmt.Sprintf("%v: ", r.mainPath)
		}
		for _, lint := range confSpec.LintYAML(docs.NewLintContext(), &rawNode) {
			lints = append(lints, fmt.Sprintf("%vline %v: %v", lintFilePrefix, lint.Line, lint.What))
		}
	}

	err = rawNode.Decode(conf)
	return
}

func (r *Reader) reactMainUpdate(mgr bundle.NewManagement, strict bool) bool {
	if r.mainUpdateFn == nil {
		return true
	}

	mgr.Logger().Infoln("Main config updated, attempting to update pipeline.")

	conf := config.New()
	lints, err := r.readMain(&conf)
	if err != nil {
		mgr.Logger().Errorf("Failed to read updated config: %v", err)

		// Rejecting due to invalid file means we do not want to try again.
		return true
	}

	lintlog := mgr.Logger().NewModule(".linter")
	for _, lint := range lints {
		lintlog.Infoln(lint)
	}
	if strict && len(lints) > 0 {
		mgr.Logger().Errorln("Rejecting updated main config due to linter errors, to allow linting errors run Benthos with --chilled")

		// Rejecting from linters means we do not want to try again.
		return true
	}

	// Update any resources within the file.
	if newInfo := resInfoFromConfig(&conf.ResourceConfig); !newInfo.applyChanges(mgr) {
		return false
	}

	return r.mainUpdateFn(conf.Config)
}
