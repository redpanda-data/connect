package common

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/stream"
	strmmgr "github.com/benthosdev/benthos/v4/internal/stream/manager"

	"github.com/urfave/cli/v2"
)

// RunService runs a service command (either the default or the streams
// subcommand).
func RunService(c *cli.Context, version, dateBuilt string, streamsMode bool) int {
	mainPath, inferredMainPath, confReader := ReadConfig(c, streamsMode)

	conf, lints, err := confReader.Read()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Configuration file read error: %v\n", err)
		return 1
	}
	defer func() {
		_ = confReader.Close(c.Context)
	}()

	logger, err := CreateLogger(c, conf, streamsMode)
	if err != nil {
		fmt.Printf("Failed to create logger: %v\n", err)
		return 1
	}

	verLogger := logger.With("benthos_version", version)
	if mainPath == "" {
		verLogger.Info("Running without a main config file")
	} else if inferredMainPath {
		verLogger.With("path", mainPath).Info("Running main config from file found in a default path")
	} else {
		verLogger.With("path", mainPath).Info("Running main config from specified file")
	}

	strict := !c.Bool("chilled")
	for _, lint := range lints {
		if strict {
			logger.With("lint", lint).Error("Config lint error")
		} else {
			logger.With("lint", lint).Warn("Config lint error")
		}
	}
	if strict && len(lints) > 0 {
		logger.Error("Shutting down due to linter errors, to prevent shutdown run Benthos with --chilled")
		return 1
	}

	stoppableManager, err := CreateManager(c, logger, streamsMode, version, dateBuilt, conf)
	if err != nil {
		logger.Error(err.Error())
		return 1
	}

	var stoppableStream Stoppable
	var dataStreamClosedChan chan struct{}

	// Create data streams.
	watching := c.Bool("watcher")
	if streamsMode {
		enableStreamsAPI := !c.Bool("no-api")
		stoppableStream = initStreamsMode(strict, watching, enableStreamsAPI, confReader, stoppableManager.Manager())
	} else {
		stoppableStream, dataStreamClosedChan = initNormalMode(conf, strict, watching, confReader, stoppableManager.Manager())
	}

	return RunManagerUntilStopped(c, conf, stoppableManager, stoppableStream, dataStreamClosedChan)
}

// DelayShutdown attempts to block until either:
// - The delay period ends
// - The provided context is cancelled
// - The process receives an interrupt or sigterm
func DelayShutdown(ctx context.Context, duration time.Duration) error {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	delayCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	select {
	case <-delayCtx.Done():
		err := delayCtx.Err()
		if err != nil && err != context.DeadlineExceeded {
			return err
		}
	case sig := <-sigChan:
		return fmt.Errorf("shutdown delay interrupted by signal: %s", sig)
	}

	return nil
}

func initStreamsMode(
	strict, watching, enableAPI bool,
	confReader *config.Reader,
	mgr *manager.Type,
) Stoppable {
	logger := mgr.Logger()
	streamMgr := strmmgr.New(mgr, strmmgr.OptAPIEnabled(enableAPI))

	streamConfs := map[string]stream.Config{}
	lints, err := confReader.ReadStreams(streamConfs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Stream configuration file read error: %v\n", err)
		os.Exit(1)
	}

	for _, lint := range lints {
		if strict {
			logger.With("lint", lint).Error("Config lint error")
		} else {
			logger.With("lint", lint).Warn("Config lint error")
		}
	}
	if strict && len(lints) > 0 {
		logger.Error("Shutting down due to stream linter errors, to prevent shutdown run Benthos with --chilled")
		os.Exit(1)
	}

	for id, conf := range streamConfs {
		if err := streamMgr.Create(id, conf); err != nil {
			logger.Error("Failed to create stream (%v): %v\n", id, err)
			os.Exit(1)
		}
	}
	logger.Info("Launching benthos in streams mode, use CTRL+C to close")

	if err := confReader.SubscribeStreamChanges(func(id string, newStreamConf *stream.Config) error {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		defer done()

		var updateErr error
		if newStreamConf != nil {
			if updateErr = streamMgr.Update(ctx, id, *newStreamConf); updateErr != nil && errors.Is(updateErr, strmmgr.ErrStreamDoesNotExist) {
				updateErr = streamMgr.Create(id, *newStreamConf)
			}
		} else {
			if updateErr = streamMgr.Delete(ctx, id); updateErr != nil && errors.Is(updateErr, strmmgr.ErrStreamDoesNotExist) {
				updateErr = nil
			}
		}
		return updateErr
	}); err != nil {
		logger.Error("Failed to create stream config watcher: %v", err)
		os.Exit(1)
	}

	if watching {
		if err := confReader.BeginFileWatching(mgr, strict); err != nil {
			logger.Error("Failed to create stream config watcher: %v", err)
			os.Exit(1)
		}
	}
	return streamMgr
}

func initNormalMode(
	conf config.Type,
	strict, watching bool,
	confReader *config.Reader,
	mgr *manager.Type,
) (newStream Stoppable, stoppedChan chan struct{}) {
	logger := mgr.Logger()

	stoppedChan = make(chan struct{})
	var closeOnce sync.Once
	streamInit := func() (Stoppable, error) {
		return stream.New(conf.Config, mgr, stream.OptOnClose(func() {
			if !watching {
				closeOnce.Do(func() {
					close(stoppedChan)
				})
			}
		}))
	}

	initStream, err := streamInit()
	if err != nil {
		logger.Error("Service closing due to: %v\n", err)
		os.Exit(1)
	}

	stoppableStream := NewSwappableStopper(initStream)

	logger.Info("Launching a benthos instance, use CTRL+C to close")

	if err := confReader.SubscribeConfigChanges(func(newStreamConf *config.Type) error {
		ctx, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		// NOTE: We're ignoring observability field changes for now.
		return stoppableStream.Replace(ctx, func() (Stoppable, error) {
			conf.Config = newStreamConf.Config
			return streamInit()
		})
	}); err != nil {
		logger.Error("Failed to create config file watcher: %v", err)
		os.Exit(1)
	}

	if watching {
		if err := confReader.BeginFileWatching(mgr, strict); err != nil {
			logger.Error("Failed to create config file watcher: %v", err)
			os.Exit(1)
		}
	}

	newStream = stoppableStream
	return
}
