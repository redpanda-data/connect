package pure

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLocalRateLimitConfErrors(t *testing.T) {
	conf, err := localRatelimitConfig().ParseYAML(`count: -1`, nil)
	require.NoError(t, err)

	_, err = newLocalRatelimitFromConfig(conf)
	require.Error(t, err)

	_, err = localRatelimitConfig().ParseYAML(`interval: nope`, nil)
	require.NoError(t, err)

	_, err = newLocalRatelimitFromConfig(conf)
	require.Error(t, err)
}

func TestLocalRateLimitBasic(t *testing.T) {
	conf, err := localRatelimitConfig().ParseYAML(`
count: 10
interval: 1s
`, nil)
	require.NoError(t, err)

	rl, err := newLocalRatelimitFromConfig(conf)
	require.NoError(t, err)

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		period, _ := rl.Access(ctx)
		assert.LessOrEqual(t, period, time.Duration(0))
	}

	if period, _ := rl.Access(ctx); period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}
}

func TestLocalRateLimitRefresh(t *testing.T) {
	conf, err := localRatelimitConfig().ParseYAML(`
count: 10
interval: 10ms
`, nil)
	require.NoError(t, err)

	rl, err := newLocalRatelimitFromConfig(conf)
	require.NoError(t, err)

	ctx := context.Background()

	for i := 0; i < 10; i++ {
		period, _ := rl.Access(ctx)
		if period > 0 {
			t.Errorf("Period above zero: %v", period)
		}
	}

	if period, _ := rl.Access(ctx); period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}

	<-time.After(time.Millisecond * 15)

	for i := 0; i < 10; i++ {
		period, _ := rl.Access(ctx)
		if period != 0 {
			t.Errorf("Rate limited on get %v", i)
		}
	}

	if period, _ := rl.Access(ctx); period == 0 {
		t.Error("Expected limit on final request")
	} else if period > time.Second {
		t.Errorf("Period beyond interval: %v", period)
	}
}

//------------------------------------------------------------------------------

func BenchmarkRateLimit(b *testing.B) {
	/* A rate limit is typically going to be protecting a networked resource
	 * where the request will likely be measured at least in hundreds of
	 * microseconds. It would be reasonable to assume the rate limit might be
	 * shared across tens of components.
	 *
	 * Therefore, we can probably sit comfortably with lock contention across
	 * one hundred or so parallel components adding an overhead of single digit
	 * microseconds. Since this benchmark doesn't take into account the actual
	 * request duration after receiving a rate limit I've set the number of
	 * components to ten in order to compensate.
	 */
	b.ReportAllocs()

	nParallel := 10
	startChan := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(nParallel)

	conf, err := localRatelimitConfig().ParseYAML(`
count: 1000
interval: 1ns
`, nil)
	require.NoError(b, err)

	rl, err := newLocalRatelimitFromConfig(conf)
	require.NoError(b, err)

	ctx := context.Background()

	for i := 0; i < nParallel; i++ {
		go func() {
			<-startChan
			for j := 0; j < b.N; j++ {
				period, _ := rl.Access(ctx)
				if period > 0 {
					time.Sleep(period)
				}
			}
			wg.Done()
		}()
	}

	b.ResetTimer()
	close(startChan)
	wg.Wait()
}
