package ratelimit

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
)

type closableRateLimit struct {
	closed bool
}

func (c *closableRateLimit) Access(ctx context.Context) (time.Duration, error) {
	return 0, nil
}

func (c *closableRateLimit) Close(ctx context.Context) error {
	c.closed = true
	return nil
}

func TestRateLimitAirGapShutdown(t *testing.T) {
	rl := &closableRateLimit{}
	agrl := MetricsForRateLimit(rl, metrics.Noop())

	err := agrl.Close(context.Background())
	assert.NoError(t, err)
	assert.True(t, rl.closed)
}
