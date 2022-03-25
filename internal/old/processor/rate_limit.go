package processor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	Constructors[TypeRateLimit] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newRateLimit(conf.RateLimit, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2ToV1Processor("rate_limit", p, mgr.Metrics()), nil
		},
		Categories: []string{
			"Utility",
		},
		Summary: `
Throttles the throughput of a pipeline according to a specified
` + "[`rate_limit`](/docs/components/rate_limits/about)" + ` resource. Rate limits are
shared across components and therefore apply globally to all processing
pipelines.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("resource", "The target [`rate_limit` resource](/docs/components/rate_limits/about)."),
		),
	}
}

//------------------------------------------------------------------------------

// RateLimitConfig contains configuration fields for the RateLimit processor.
type RateLimitConfig struct {
	Resource string `json:"resource" yaml:"resource"`
}

// NewRateLimitConfig returns a RateLimitConfig with default values.
func NewRateLimitConfig() RateLimitConfig {
	return RateLimitConfig{
		Resource: "",
	}
}

//------------------------------------------------------------------------------

type rateLimitProc struct {
	rlName string
	mgr    interop.Manager

	closeChan chan struct{}
	closeOnce sync.Once
}

func newRateLimit(conf RateLimitConfig, mgr interop.Manager) (*rateLimitProc, error) {
	if !mgr.ProbeRateLimit(conf.Resource) {
		return nil, fmt.Errorf("rate limit resource '%v' was not found", conf.Resource)
	}
	r := &rateLimitProc{
		rlName:    conf.Resource,
		mgr:       mgr,
		closeChan: make(chan struct{}),
	}
	return r, nil
}

func (r *rateLimitProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	for {
		var waitFor time.Duration
		var err error
		if rerr := r.mgr.AccessRateLimit(ctx, r.rlName, func(rl ratelimit.V1) {
			waitFor, err = rl.Access(ctx)
		}); rerr != nil {
			err = rerr
		}
		if ctx.Err() != nil {
			return nil, err
		}
		if err != nil {
			r.mgr.Logger().Errorf("Failed to access rate limit: %v", err)
			waitFor = time.Second
		}
		if waitFor == 0 {
			return []*message.Part{msg}, nil
		}
		select {
		case <-time.After(waitFor):
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.closeChan:
			return nil, component.ErrTypeClosed
		}
	}
}

func (r *rateLimitProc) Close(ctx context.Context) error {
	r.closeOnce.Do(func() {
		close(r.closeChan)
	})
	return nil
}
