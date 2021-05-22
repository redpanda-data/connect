package ratelimit

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// V2 is a simpler interface to implement than types.RateLimit.
type V2 interface {
	// Access the rate limited resource. Returns a duration or an error if the
	// rate limit check fails. The returned duration is either zero (meaning the
	// resource can be accessed) or a reasonable length of time to wait before
	// requesting again.
	Access(ctx context.Context) (time.Duration, error)

	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}

//------------------------------------------------------------------------------

// Implements types.RateLimit
type v2ToV1RateLimit struct {
	r   V2
	sig *shutdown.Signaller

	mChecked metrics.StatCounter
	mLimited metrics.StatCounter
	mErr     metrics.StatCounter
}

// NewV2ToV1RateLimit wraps a ratelimit.V2 with a struct that implements
// types.RateLimit.
func NewV2ToV1RateLimit(r V2, stats metrics.Type) types.RateLimit {
	return &v2ToV1RateLimit{
		r: r, sig: shutdown.NewSignaller(),

		mChecked: stats.GetCounter("checked"),
		mLimited: stats.GetCounter("limited"),
		mErr:     stats.GetCounter("error"),
	}
}

func (r *v2ToV1RateLimit) Access() (time.Duration, error) {
	r.mChecked.Incr(1)
	tout, err := r.r.Access(context.Background())
	if err != nil {
		r.mErr.Incr(1)
	} else if tout > 0 {
		r.mLimited.Incr(1)
	}
	return tout, err
}

func (r *v2ToV1RateLimit) CloseAsync() {
	go func() {
		if err := r.r.Close(context.Background()); err == nil {
			r.sig.ShutdownComplete()
		}
	}()
}

func (r *v2ToV1RateLimit) WaitForClose(tout time.Duration) error {
	select {
	case <-r.sig.HasClosedChan():
	case <-time.After(tout):
		return types.ErrTimeout
	}
	return nil
}
