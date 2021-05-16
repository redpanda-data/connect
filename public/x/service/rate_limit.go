package service

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// RateLimit is an interface implemented by Benthos rate limits.
type RateLimit interface {
	// Access the rate limited resource. Returns a duration or an error if the
	// rate limit check fails. The returned duration is either zero (meaning the
	// resource may be accessed) or a reasonable length of time to wait before
	// requesting again.
	Access(context.Context) (time.Duration, error)

	Closer
}

//------------------------------------------------------------------------------

// Implements types.RateLimit
type airGapRateLimit struct {
	c RateLimit

	sig *shutdown.Signaller
}

func newAirGapRateLimit(c RateLimit) types.RateLimit {
	return &airGapRateLimit{c, shutdown.NewSignaller()}
}

func (a *airGapRateLimit) Access() (time.Duration, error) {
	return a.c.Access(context.Background())
}

func (a *airGapRateLimit) CloseAsync() {
	go func() {
		if err := a.c.Close(context.Background()); err == nil {
			a.sig.ShutdownComplete()
		}
	}()
}

func (a *airGapRateLimit) WaitForClose(tout time.Duration) error {
	select {
	case <-a.sig.HasClosedChan():
	case <-time.After(tout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------

// Implements RateLimit around a types.RateLimit
type reverseAirGapRateLimit struct {
	r types.RateLimit
}

func newReverseAirGapRateLimit(r types.RateLimit) *reverseAirGapRateLimit {
	return &reverseAirGapRateLimit{r}
}

func (a *reverseAirGapRateLimit) Access(context.Context) (time.Duration, error) {
	return a.r.Access()
}

func (a *reverseAirGapRateLimit) Close(ctx context.Context) error {
	a.r.CloseAsync()
	for {
		// Gross but will do for now until we replace these with context params.
		if err := a.r.WaitForClose(time.Millisecond * 100); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}
