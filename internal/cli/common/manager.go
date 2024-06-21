package common

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"
	"go.opentelemetry.io/otel/trace"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/api"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

// CreateManager from a CLI context and a stream config.
func CreateManager(
	c *cli.Context,
	logger log.Modular,
	streamsMode bool,
	version, dateBuilt string,
	conf config.Type,
	mgrOpts ...manager.OptFunc,
) (stoppableMgr *StoppableManager, err error) {
	var stats *metrics.Namespaced
	var trac trace.TracerProvider
	defer func() {
		if err == nil {
			return
		}
		if trac != nil {
			if shutter, ok := trac.(interface {
				Shutdown(context.Context) error
			}); ok {
				_ = shutter.Shutdown(context.Background())
			}
		}
		if stats != nil {
			_ = stats.Close()
		}
	}()

	// We use a temporary manager with just the logger initialised for metrics
	// instantiation. Doing this means that metrics plugins will use a global
	// environment for child plugins and bloblang mappings, which we might want
	// to revise in future.
	tmpMgr := mock.NewManager()
	tmpMgr.L = logger

	// Create our metrics type.
	if stats, err = bundle.AllMetrics.Init(conf.Metrics, tmpMgr); err != nil {
		err = fmt.Errorf("failed to connect to metrics aggregator: %w", err)
		return
	}

	// Create our tracer type.
	if trac, err = bundle.AllTracers.Init(conf.Tracer, tmpMgr); err != nil {
		err = fmt.Errorf("failed to initialise tracer: %w", err)
		return
	}

	// Create HTTP API with a sanitised service config.
	var sanitNode yaml.Node
	if err = sanitNode.Encode(conf); err == nil {
		sanitConf := docs.NewSanitiseConfig(bundle.GlobalEnvironment)
		sanitConf.RemoveTypeField = true
		sanitConf.ScrubSecrets = true
		sanitSpec := config.Spec()
		if streamsMode {
			sanitSpec = config.SpecWithoutStream()
		}
		err = sanitSpec.SanitiseYAML(&sanitNode, sanitConf)
	}
	if err != nil {
		err = fmt.Errorf("failed to generate sanitised config: %w", err)
		return
	}

	var httpServer *api.Type
	if httpServer, err = api.New(version, dateBuilt, conf.HTTP, sanitNode, logger, stats); err != nil {
		err = fmt.Errorf("failed to initialise API: %w", err)
		return
	}

	mgrOpts = append([]manager.OptFunc{
		manager.OptSetAPIReg(httpServer),
		manager.OptSetStreamHTTPNamespacing(c.Bool("prefix-stream-endpoints")),
		manager.OptSetLogger(logger),
		manager.OptSetMetrics(stats),
		manager.OptSetTracer(trac),
		manager.OptSetStreamsMode(streamsMode),
	}, mgrOpts...)

	// Create resource manager.
	var mgr *manager.Type
	if mgr, err = manager.New(conf.ResourceConfig, mgrOpts...); err != nil {
		err = fmt.Errorf("failed to initialise resources: %w", err)
		return
	}

	stoppableMgr = newStoppableManager(httpServer, mgr)
	return
}

// RunManagerUntilStopped will run the provided HTTP server and block until
// either a provided stream stoppable is gracefully terminated (via the
// dataStreamClosedChan) or a signal is given to the process to terminate, at
// which point the provided HTTP server, the manager, and the stoppable is
// stopped according to the configured shutdown timeout.
func RunManagerUntilStopped(
	c *cli.Context,
	conf config.Type,
	stopMgr *StoppableManager,
	stopStrm Stoppable,
	dataStreamClosedChan chan struct{},
) int {
	var exitDelay time.Duration
	if td := conf.SystemCloseDelay; td != "" {
		var err error
		if exitDelay, err = time.ParseDuration(td); err != nil {
			stopMgr.Manager().Logger().Error("Failed to parse shutdown delay period string: %v\n", err)
			return 1
		}
	}

	var exitTimeout time.Duration
	if tout := conf.SystemCloseTimeout; tout != "" {
		var err error
		if exitTimeout, err = time.ParseDuration(tout); err != nil {
			stopMgr.Manager().Logger().Error("Failed to parse shutdown timeout period string: %v\n", err)
			return 1
		}
	}

	// Defer clean up.
	defer func() {
		if exitDelay > 0 {
			stopMgr.Manager().Logger().Info("Shutdown delay is in effect for %s\n", exitDelay)
			if err := DelayShutdown(c.Context, exitDelay); err != nil {
				stopMgr.Manager().Logger().Error("Shutdown delay failed: %s", err)
			}
		}

		go func() {
			<-time.After(exitTimeout + time.Second)
			stopMgr.Manager().Logger().Warn(
				"Service failed to close cleanly within allocated time." +
					" Exiting forcefully and dumping stack trace to stderr",
			)
			_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			os.Exit(1)
		}()

		ctx, done := context.WithTimeout(c.Context, exitTimeout)
		if err := stopStrm.Stop(ctx); err != nil {
			os.Exit(1)
		}

		if err := stopMgr.Stop(ctx); err != nil {
			stopMgr.Manager().Logger().Warn(
				"Service failed to close resources cleanly within allocated time: %v."+
					" Exiting forcefully and dumping stack trace to stderr\n", err,
			)
			_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1)
			os.Exit(1)
		}
		done()
	}()

	var deadLineTrigger <-chan time.Time
	if dl, exists := c.Context.Deadline(); exists {
		// If a deadline has been set by the cli context then we need to trigger
		// graceful termination before it's reached, otherwise it'll never
		// happen as the context will cancel the cleanup.
		//
		// We make a best attempt at doing this by starting termination earlier
		// than the deadline (by 10%, capped at one second).
		dlTriggersBy := time.Until(dl)

		earlierBy := dlTriggersBy / 10
		if earlierBy > time.Second {
			earlierBy = time.Second
		}
		deadLineTrigger = time.After(dlTriggersBy - earlierBy)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for termination signal
	select {
	case sig := <-sigChan:
		var sigName string
		switch sig {
		case os.Interrupt:
			sigName = "SIGINT"
		case syscall.SIGTERM:
			sigName = "SIGTERM"
		default:
			sigName = sig.String()
		}
		stopMgr.Manager().Logger().Info("Received %s, the service is closing", sigName)
	case <-dataStreamClosedChan:
		stopMgr.Manager().Logger().Info("Pipeline has terminated. Shutting down the service")
	case <-deadLineTrigger:
		stopMgr.Manager().Logger().Info("Run context deadline about to be reached. Shutting down the service")
	case <-c.Context.Done():
		stopMgr.Manager().Logger().Info("Run context was cancelled. Shutting down the service")
	}
	return 0
}

func newStoppableManager(api *api.Type, mgr *manager.Type) *StoppableManager {
	s := &StoppableManager{
		api:           api,
		apiClosedChan: make(chan struct{}),
		mgr:           mgr,
	}
	// Start HTTP server.
	go func() {
		httpErr := api.ListenAndServe()
		if httpErr != nil && httpErr != http.ErrServerClosed {
			mgr.Logger().Error("HTTP Server error: %v\n", httpErr)
		}
		close(s.apiClosedChan)
	}()
	return s
}

// StoppableManager wraps a manager and API type that potentially outlives one
// or more dependent streams and encapsulates the logic for shutting them down
// within the deadline of a given context.
type StoppableManager struct {
	api           *api.Type
	apiClosedChan chan struct{}
	mgr           *manager.Type
}

// Manager returns the underlying manager type.
func (s *StoppableManager) Manager() *manager.Type {
	return s.mgr
}

// API returns the underlying api type.
func (s *StoppableManager) API() *api.Type {
	return s.api
}

// Stop the manager and the API server, gracefully if possible. If the context
// has a deadline then this will be used as a mechanism for pre-emptively
// attempting ungraceful stopping when nearing the deadline.
func (s *StoppableManager) Stop(ctx context.Context) error {
	var gracefulCutOff <-chan time.Time
	if dl, exists := ctx.Deadline(); exists {
		gracefulCutOff = time.After(time.Until(dl) / 2)
	}

	go func() {
		_ = s.api.Shutdown(ctx)
		select {
		case <-s.apiClosedChan:
			return
		case <-ctx.Done():
		case <-gracefulCutOff:
		}
		s.mgr.Logger().Warn("Service failed to close HTTP server gracefully in time")
	}()

	s.mgr.TriggerStopConsuming()
	if err := s.mgr.WaitForClose(ctx); err != nil {
		return err
	}
	if err := s.mgr.CloseObservability(ctx); err != nil {
		s.mgr.Logger().Error("Failed to cleanly close observability components: %w", err)
	}
	return nil
}
