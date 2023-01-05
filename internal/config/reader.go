package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"github.com/Jeffail/gabs/v2"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/stream"
)

const (
	defaultChangeFlushPeriod  = 50 * time.Millisecond
	defaultChangeDelayPeriod  = time.Second
	defaultFilesRefreshPeriod = time.Second
)

type streamFileInfo struct {
	id string
}

type fileWatcher interface {
	Close() error
}

// Reader provides utilities for parsing a Benthos config as a main file with
// a collection of resource files, and options such as overrides.
type Reader struct {
	// The suffix given to unit test definition files, this is used in order to
	// exclude unit tests from being run in streams mode with arbitrary
	// directory walking.
	testSuffix string

	// The filesystem used for reading config files.
	fs ifs.FS

	mainPath      string
	resourcePaths []string
	streamsPaths  []string
	overrides     []string

	modTimeLastRead map[string]time.Time

	// Controls whether the main config should include input, output, etc.
	streamsMode bool

	// Tracks the details of the config file when we last read it.
	configFileInfo resourceFileInfo

	// Tracks the details of stream config files when we last read them.
	streamFileInfo map[string]streamFileInfo

	// Tracks the details of resource config files when we last read them,
	// including information such as the specific resources that were created
	// from it.
	resourceFileInfo map[string]resourceFileInfo
	resourceSources  *resourceSourceInfo

	mainUpdateFn   MainUpdateFunc
	streamUpdateFn StreamUpdateFunc
	watcher        fileWatcher

	changeFlushPeriod  time.Duration
	changeDelayPeriod  time.Duration
	filesRefreshPeriod time.Duration
}

// NewReader creates a new config reader.
func NewReader(mainPath string, resourcePaths []string, opts ...OptFunc) *Reader {
	if mainPath != "" {
		mainPath = filepath.Clean(mainPath)
	}
	r := &Reader{
		testSuffix:         "_benthos_test",
		fs:                 ifs.OS(),
		mainPath:           mainPath,
		resourcePaths:      resourcePaths,
		modTimeLastRead:    map[string]time.Time{},
		streamFileInfo:     map[string]streamFileInfo{},
		resourceFileInfo:   map[string]resourceFileInfo{},
		resourceSources:    newResourceSourceInfo(),
		changeFlushPeriod:  defaultChangeFlushPeriod,
		changeDelayPeriod:  defaultChangeDelayPeriod,
		filesRefreshPeriod: defaultFilesRefreshPeriod,
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

// OptUseFS sets the ifs.FS implementation for the reader to use. By default the
// OS filesystem is used, and when overridden it is no longer possible to use
// BeginFileWatching.
func OptUseFS(fs ifs.FS) OptFunc {
	return func(r *Reader) {
		r.fs = fs
	}
}

//------------------------------------------------------------------------------

// Read a Benthos config from the files and options specified.
func (r *Reader) Read(conf *Type) (lints []string, err error) {
	if lints, err = r.readMain(conf); err != nil {
		return
	}
	r.configFileInfo = resInfoFromConfig(&conf.ResourceConfig)
	r.resourceSources.populateFrom(r.mainPath, &r.configFileInfo)

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
// updated. If an error is returned then the attempt will be made again after a
// grace period.
type MainUpdateFunc func(conf stream.Config) error

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
// been updated. If an error is returned then the attempt will be made again
// after a grace period.
//
// When the provided config is nil it is a signal that the stream has been
// deleted, and it is expected that the provided update func should shut that
// stream down.
type StreamUpdateFunc func(id string, conf *stream.Config) error

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
		if err := specs.SetYAMLPath(docs.DeprecatedProvider, root, &valNode, gabs.DotPathToSlice(path)...); err != nil {
			return fmt.Errorf("failed to set config field override: %w", err)
		}
	}
	return nil
}

func (r *Reader) readMain(conf *Type) (lints []string, err error) {
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
		var dLints []docs.Lint
		var modTime time.Time
		if confBytes, dLints, modTime, err = ReadFileEnvSwap(r.fs, r.mainPath); err != nil {
			return
		}
		for _, l := range dLints {
			lints = append(lints, l.Error())
		}
		r.modTimeLastRead[r.mainPath] = modTime
		if err = yaml.Unmarshal(confBytes, &rawNode); err != nil {
			return
		}
	}

	confSpec := Spec()
	if r.streamsMode {
		// Spec is limited to just non-stream fields when in streams mode (no
		// input, output, etc)
		confSpec = SpecWithoutStream()
	}
	if err = applyOverrides(confSpec, &rawNode, r.overrides...); err != nil {
		return
	}

	if !bytes.HasPrefix(confBytes, []byte("# BENTHOS LINT DISABLE")) {
		lintFilePrefix := r.mainPath
		for _, lint := range confSpec.LintYAML(docs.NewLintContext(), &rawNode) {
			lints = append(lints, fmt.Sprintf("%v%v", lintFilePrefix, lint.Error()))
		}
	}

	err = rawNode.Decode(conf)
	return
}

// TriggerMainUpdate attempts to re-read the main configuration file, trigger
// the provided main update func, and apply changes to resources to the provided
// manager as appropriate.
func (r *Reader) TriggerMainUpdate(mgr bundle.NewManagement, strict bool) error {
	conf := New()
	lints, err := r.readMain(&conf)
	if errors.Is(err, fs.ErrNotExist) {
		// Ignore main file deletes for now
		return nil
	}
	if err != nil {
		mgr.Logger().Errorf("Failed to read updated config: %v", err)

		// Rejecting due to invalid file means we do not want to try again.
		return noReread(err)
	}
	mgr.Logger().Infoln("Main config updated, attempting to update pipeline.")

	lintlog := mgr.Logger()
	for _, lint := range lints {
		lintlog.Infoln(lint)
	}
	if strict && len(lints) > 0 {
		mgr.Logger().Errorln("Rejecting updated main config due to linter errors, to allow linting errors run Benthos with --chilled")

		// Rejecting from linters means we do not want to try again.
		return noReread(errors.New("file contained linting errors and is running in strict mode"))
	}

	// Update any resources within the file.
	newInfo := resInfoFromConfig(&conf.ResourceConfig)
	if err := r.applyResourceChanges(r.mainPath, mgr, newInfo, r.configFileInfo); err != nil {
		return err
	}
	r.configFileInfo = newInfo

	if r.mainUpdateFn != nil {
		if err := r.mainUpdateFn(conf.Config); err != nil {
			mgr.Logger().Errorf("Failed to apply updated config: %v", err)
			return err
		}
		mgr.Logger().Infoln("Updated main config")
	}
	return nil
}
