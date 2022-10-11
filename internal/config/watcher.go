//go:build !wasm

package config

import (
	"errors"
	"path/filepath"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"

	"github.com/fsnotify/fsnotify"
)

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

	// TODO: Refresh this occasionally?
	streamsPaths, err := r.streamPathsExpanded()
	if err != nil {
		return err
	}
	for _, p := range streamsPaths {
		if err := watcher.Add(p[1]); err != nil {
			_ = watcher.Close()
			return err
		}
	}

	// TODO: Refresh this occasionally?
	resourcePaths, err := r.resourcePathsExpanded()
	if err != nil {
		return err
	}
	for _, p := range resourcePaths {
		if err := watcher.Add(p); err != nil {
			_ = watcher.Close()
			return err
		}
	}
	return nil
}
