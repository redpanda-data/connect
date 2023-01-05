//go:build !wasm

package config

import (
	"errors"
	"path/filepath"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"

	"github.com/fsnotify/fsnotify"
)

// ErrNoReread is an error type returned from update triggers that indicates an
// attempt should not be re-made unless the source file has been modified.
type ErrNoReread struct {
	wrapped error
}

func noReread(err error) error {
	return &ErrNoReread{wrapped: err}
}

// ShouldReread returns true if the error returned from an update trigger is non
// nil and also temporal, and therefore it is worth trying the update again even
// if the content has not changed.
func ShouldReread(err error) bool {
	if err == nil {
		return false
	}
	var nr *ErrNoReread
	return !errors.As(err, &nr)
}

// Unwrap the underlying error.
func (e *ErrNoReread) Unwrap() error {
	return e.wrapped
}

// Error returns a human readable error string.
func (e *ErrNoReread) Error() string {
	return e.wrapped.Error()
}

type fileChange struct {
	at time.Time
}

func (r *Reader) modifiedSinceLastRead(name string) bool {
	info, err := r.fs.Stat(name)
	if err != nil {
		return true // Better to be safe than sorry
	}
	if info.ModTime().IsZero() {
		return true
	}
	return info.ModTime().After(r.modTimeLastRead[name])
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

	if !ifs.IsOS(r.fs) {
		return errors.New("config file watching is only supported when accessing configs from the OS")
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	r.watcher = watcher
	watching := map[string]struct{}{}
	collapsedChanges := map[string]fileChange{}

	addNotWatching := func(paths []string) error {
		for _, p := range paths {
			if _, exists := watching[p]; !exists {
				if err := watcher.Add(p); err != nil {
					return err
				}
				watching[p] = struct{}{}
				collapsedChanges[p] = fileChange{at: time.Now()}
			}
		}
		return nil
	}

	refreshFiles := func() error {
		if !r.streamsMode && r.mainPath != "" {
			if _, err := r.fs.Stat(r.mainPath); err == nil {
				if err := addNotWatching([]string{r.mainPath}); err != nil {
					return err
				}
			}
		}

		streamsPaths, err := r.streamPathsExpanded()
		if err != nil {
			return err
		}
		if err := addNotWatching(streamsPaths); err != nil {
			return err
		}

		resourcePaths, err := r.resourcePathsExpanded()
		if err != nil {
			return err
		}
		if err := addNotWatching(resourcePaths); err != nil {
			return err
		}
		return nil
	}

	if err := refreshFiles(); err != nil {
		_ = watcher.Close()
		return err
	}

	// Don't bother re-reading if the files haven't changed since the last read.
	for k := range collapsedChanges {
		if !r.modifiedSinceLastRead(k) {
			delete(collapsedChanges, k)
		}
	}

	go func() {
		filesTicker := time.NewTicker(r.filesRefreshPeriod)
		defer filesTicker.Stop()

		changeTicker := time.NewTicker(r.changeFlushPeriod)
		defer changeTicker.Stop()

		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				cleanPath := filepath.Clean(event.Name)
				switch {
				case event.Op&fsnotify.Write == fsnotify.Write:
					watching[cleanPath] = struct{}{}
					collapsedChanges[cleanPath] = fileChange{at: time.Now()}

				case event.Op&fsnotify.Remove == fsnotify.Remove ||
					event.Op&fsnotify.Rename == fsnotify.Rename:
					delete(watching, cleanPath)
					delete(r.modTimeLastRead, cleanPath) // Keeps the cache small
					_ = watcher.Remove(cleanPath)
					collapsedChanges[cleanPath] = fileChange{at: time.Now()}
				}
			case <-changeTicker.C:
				for nameClean, change := range collapsedChanges {
					if time.Since(change.at) < r.changeDelayPeriod {
						continue
					}
					var succeeded bool
					if nameClean == r.mainPath {
						succeeded = !ShouldReread(r.TriggerMainUpdate(mgr, strict))
					} else if _, exists := r.streamFileInfo[nameClean]; exists {
						succeeded = !ShouldReread(r.TriggerStreamUpdate(mgr, strict, nameClean))
					} else {
						succeeded = !ShouldReread(r.TriggerResourceUpdate(mgr, strict, nameClean))
					}
					if succeeded {
						delete(collapsedChanges, nameClean)
					} else {
						change.at = time.Now()
						collapsedChanges[nameClean] = change
					}
				}
			case <-filesTicker.C:
				if err := refreshFiles(); err != nil {
					mgr.Logger().Errorf("Failed to refresh watched paths: %v", err)
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				mgr.Logger().Errorf("Config watcher error: %v", err)
			}
		}
	}()
	return nil
}
