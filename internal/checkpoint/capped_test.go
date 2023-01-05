package checkpoint

import (
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCappedSequential(t *testing.T) {
	c, cancel := newCheckpointTester(t, 1000, 5*time.Second)
	defer cancel()

	c.AssertNoHighest()

	c.AssertTrackAllowed(1, 1)
	c.AssertTrackAllowed(2, 1)
	c.AssertTrackAllowed(3, 1)

	c.AssertNoHighest()

	c.Resolve(1, 1)
	c.AssertHighest(1)

	c.Resolve(2, 2)
	c.AssertHighest(2)

	c.Resolve(3, 3)
	c.AssertHighest(3)

	c.AssertTrackAllowed(4, 1)
	c.AssertHighest(3)

	c.Resolve(4, 4)
	c.AssertHighest(4)
}

func TestCappedBigJumps(t *testing.T) {
	c, cancel := newCheckpointTester(t, 1, 5*time.Second)
	defer cancel()

	c.AssertNoHighest()

	c.AssertTrackAllowed(1000, 1)

	c.AssertNoHighest()

	c.Resolve(1000, 1000)

	c.AssertHighest(1000)

	c.AssertTrackAllowed(2000, 1)

	c.AssertHighest(1000)

	c.Resolve(2000, 2000)

	c.AssertHighest(2000)
}

func TestCappedBigJumpsMore(t *testing.T) {
	c, cancel := newCheckpointTester(t, 2, 5*time.Second)
	defer cancel()

	c.AssertNoHighest()

	c.AssertTrackAllowed(1000, 1)

	c.AssertNoHighest()

	c.AssertTrackAllowed(2000, 1)

	c.AssertNoHighest()

	c.Resolve(1000, 1000)

	c.AssertHighest(1000)

	c.Resolve(2000, 2000)

	c.AssertHighest(2000)
}

func TestCappedStartsBig(t *testing.T) {
	c, cancel := newCheckpointTester(t, 100, 5*time.Second)
	defer cancel()

	c.AssertNoHighest()

	c.AssertTrackAllowed(500, 1)
	c.AssertTrackAllowed(501, 1)
	c.AssertTrackAllowed(502, 1)

	c.AssertNoHighest()

	c.Resolve(500, 500)
	c.AssertHighest(500)

	c.Resolve(501, 501)
	c.AssertHighest(501)

	c.Resolve(502, 502)
	c.AssertHighest(502)

	c.AssertTrackAllowed(503, 1)

	c.AssertHighest(502)

	c.Resolve(503, 503)
	c.AssertHighest(503)
}

func TestCappedCapHappy(t *testing.T) {
	c, cancel := newCheckpointTester(t, 100, 5*time.Second)
	defer cancel()

	c.AssertNoHighest()

	c.AssertTrackAllowed(100, 1)
	c.AssertNoHighest()
	c.Resolve(100, 100)
	c.AssertHighest(100)

	for i := 101; i <= 200; i++ {
		c.AssertTrackAllowed(int64(i), 1)
	}

	c.AssertTrackBlocked(201, 1)

	time.Sleep(50 * time.Millisecond)

	c.AssertNotPending(201)

	c.Resolve(101, 101)

	time.Sleep(50 * time.Millisecond)

	c.AssertPending(201)

	for i := int64(102); i <= 201; i++ {
		c.Resolve(i, i)
		c.AssertHighest(i)
	}
}

func TestCappedOutOfSync(t *testing.T) {
	c, cancel := newCheckpointTester(t, 1000, 5*time.Second)
	defer cancel()

	c.AssertNoHighest()

	c.AssertTrackAllowed(1, 1)
	c.AssertTrackAllowed(2, 1)
	c.AssertTrackAllowed(3, 1)
	c.AssertTrackAllowed(4, 1)

	c.AssertNoHighest()

	c.Resolve(2, -1)
	c.AssertNoHighest()

	c.Resolve(1, 2)
	c.AssertHighest(2)

	c.Resolve(3, 3)
	c.AssertHighest(3)

	c.Resolve(4, 4)
	c.AssertHighest(4)
}

func TestCappedSequentialLarge(t *testing.T) {
	c, cancel := newCheckpointTester(t, 1000, 5*time.Second)
	defer cancel()

	c.AssertNoHighest()

	for i := int64(0); i < 1000; i++ {
		c.AssertTrackAllowed(i, 1)
		c.AssertNoHighest()
	}

	for i := int64(0); i < 1000; i++ {
		c.Resolve(i, i)
		c.AssertHighest(i)
	}
}

func TestCappedSequentialChunks(t *testing.T) {
	c, cancel := newCheckpointTester(t, 1000, 5*time.Second)
	defer cancel()

	chunkSize := int64(100)
	for i := int64(0); i < 10; i++ {
		for j := int64(0); j < chunkSize; j++ {
			offset := i*chunkSize + j
			c.AssertTrackAllowed(offset, 1)
		}
		for j := int64(0); j < chunkSize; j++ {
			offset := i*chunkSize + j
			c.Resolve(offset, offset)
			c.AssertHighest(offset)
		}
	}
}

func TestCappedSequentialReverseLarge(t *testing.T) {
	c, cancel := newCheckpointTester(t, 1000, 5*time.Second)
	defer cancel()

	for i := int64(0); i < 1000; i++ {
		c.AssertTrackAllowed(i, 1)
	}
	for i := int64(999); i > 0; i-- {
		c.Resolve(i, -1)
		c.AssertNoHighest()
	}

	c.Resolve(0, 999)
	c.AssertHighest(999)
}

func TestCappedSequentialRandomLarge(t *testing.T) {
	c, cancel := newCheckpointTester(t, 1000, 5*time.Second)
	defer cancel()

	indexes := make([]int64, 1000)
	for i := int64(0); i < 1000; i++ {
		c.AssertTrackAllowed(i, 1)
		indexes[int(i)] = i
	}

	rand.Shuffle(len(indexes), func(i, j int) {
		indexes[i], indexes[j] = indexes[j], indexes[i]
	})

	resolved := 0
	for index, i := range indexes {
		c.Resolve(i, -2) // -2 means don't check highest
		resolved++

		highestI := c.checkpointer.Highest()
		if highestI == nil {
			highestI = int64(-1)
		}

		if resolved == len(indexes) {
			assert.Equal(t, int64(999), highestI.(int64))
		} else {
			// Assert that the remaining offsets are all higher
			for _, k := range indexes[index+1:] {
				assert.True(t, k > highestI.(int64))
			}
		}
	}
}

func BenchmarkCappedChunked100(b *testing.B) {
	checkpointLimit := int64(1000)
	chunkSize := int64(100)
	resolvers := make([]func() any, chunkSize)

	b.ReportAllocs()

	c := NewCapped(checkpointLimit)
	ctx := context.Background()

	N := int64(b.N) / chunkSize
	batchSize := checkpointLimit / chunkSize

	for i := int64(0); i < N; i++ {
		for j := int64(0); j < chunkSize; j++ {
			offset := i*chunkSize + j
			resolver, err := c.Track(ctx, offset, batchSize)
			if err != nil {
				b.Fatal(err)
			}
			resolvers[j] = resolver
		}
		for j := int64(0); j < chunkSize; j++ {
			resolver := resolvers[j]
			resolvers[j] = nil
			v, ok := resolver().(int64)
			if !ok {
				b.Fatal("should always resolve with a maximum")
			}

			offset := i*chunkSize + j
			if offset != v {
				b.Errorf("Wrong value: %v != %v", offset, v)
			}
		}
	}
}

func BenchmarkCappedChunkedReverse100(b *testing.B) {
	checkpointLimit := int64(1000)
	chunkSize := int64(100)
	resolvers := make([]func() any, chunkSize)

	b.ReportAllocs()

	c := NewCapped(checkpointLimit)
	ctx := context.Background()

	N := int64(b.N) / chunkSize
	batchSize := checkpointLimit / chunkSize

	for i := int64(0); i < N; i++ {
		for j := int64(0); j < chunkSize; j++ {
			offset := i*chunkSize + j
			resolver, err := c.Track(ctx, offset, batchSize)
			if err != nil {
				b.Fatal(err)
			}
			resolvers[j] = resolver
		}
		for j := chunkSize - 1; j >= 0; j-- {
			resolver := resolvers[j]
			resolvers[j] = nil
			v, ok := resolver().(int64)

			exp := int64(-1)

			if i > 0 {
				exp = (i * chunkSize) - 1
			}

			if j == 0 {
				exp = ((i + 1) * chunkSize) - 1
			}

			if exp >= 0 {
				if !ok {
					b.Fatal("should resolve with a maximum")
				} else if exp != v {
					b.Errorf("Wrong value: %v != %v", exp, v)
				}
			}
		}
	}
}

func BenchmarkCappedChunkedReverse1000(b *testing.B) {
	checkpointLimit := int64(1000)
	chunkSize := int64(1000)
	resolvers := make([]func() any, chunkSize)

	b.ReportAllocs()

	c := NewCapped(checkpointLimit)
	ctx := context.Background()

	N := int64(b.N) / chunkSize
	batchSize := checkpointLimit / chunkSize

	for i := int64(0); i < N; i++ {
		for j := int64(0); j < chunkSize; j++ {
			offset := i*chunkSize + j
			resolver, err := c.Track(ctx, offset, batchSize)
			if err != nil {
				b.Fatal(err)
			}
			resolvers[j] = resolver
		}
		for j := chunkSize - 1; j >= 0; j-- {
			resolver := resolvers[j]
			resolvers[j] = nil
			v, ok := resolver().(int64)

			exp := int64(-1)

			if i > 0 {
				exp = (i * chunkSize) - 1
			}

			if j == 0 {
				exp = ((i + 1) * chunkSize) - 1
			}

			if exp >= 0 {
				if !ok {
					b.Fatal("should resolve with a maximum")
				} else if exp != v {
					b.Errorf("Wrong value: %v != %v", exp, v)
				}
			}
		}
	}
}

func BenchmarkCappedSequential(b *testing.B) {
	resolvers := make([]func() any, b.N)

	b.ReportAllocs()

	c := NewCapped(int64(b.N))
	ctx := context.Background()

	var err error
	for i := int64(0); i < int64(b.N); i++ {
		resolvers[i], err = c.Track(ctx, i, 1)
		if err != nil {
			b.Fatal(err)
		}
	}

	for i := 0; i < b.N; i++ {
		v, ok := resolvers[i]().(int64)
		if !ok {
			b.Fatal("should resolve with a maximum")
		}
		if int64(i) != v {
			b.Errorf("Wrong value: %v != %v", i, v)
		}
	}
}

type checkpointTester struct {
	mu           sync.Mutex
	ctx          context.Context
	t            *testing.T
	checkpointer *Capped
	resolvers    map[int64]func() any
}

func newCheckpointTester(t *testing.T, capacity int64, timeout time.Duration) (*checkpointTester, func()) { //nolint: gocritic // Ignore unnamedResult false positive
	ctx, cancel := context.WithTimeout(context.Background(), timeout)

	return &checkpointTester{
		ctx:          ctx,
		t:            t,
		checkpointer: NewCapped(capacity),
		resolvers:    map[int64]func() any{},
	}, cancel
}

func (c *checkpointTester) AssertNoHighest() {
	c.t.Helper()
	actual := c.checkpointer.Highest()
	require.Nil(c.t, actual, "should not have a highest offset")
}

func (c *checkpointTester) AssertHighest(expected int64) {
	c.t.Helper()
	actual := c.checkpointer.Highest()
	require.NotNil(c.t, actual, "should have a highest offset")
	assert.Equal(c.t, expected, actual, "highest offset should match expected")
}

func (c *checkpointTester) AssertPending(offset int64) {
	c.t.Helper()

	c.mu.Lock()
	_, ok := c.resolvers[offset]
	c.mu.Unlock()

	require.True(c.t, ok, "offset should be pending")
}

func (c *checkpointTester) AssertNotPending(offset int64) {
	c.t.Helper()

	c.mu.Lock()
	_, ok := c.resolvers[offset]
	c.mu.Unlock()

	require.False(c.t, ok, "offset should not be pending")
}

func (c *checkpointTester) AssertTrackAllowed(offset, batchSize int64) {
	c.t.Helper()

	c.track(offset, batchSize)
}

func (c *checkpointTester) AssertTrackBlocked(offset, batchSize int64) {
	c.t.Helper()

	go c.track(offset, batchSize)

	time.Sleep(50 * time.Millisecond)

	c.mu.Lock()
	_, ok := c.resolvers[offset]
	c.mu.Unlock()

	assert.False(c.t, ok, "Track call should be blocked")
}

func (c *checkpointTester) Resolve(offset, expectedHighest int64) {
	c.t.Helper()

	c.mu.Lock()
	resolve, ok := c.resolvers[offset]
	delete(c.resolvers, offset)
	c.mu.Unlock()

	require.True(c.t, ok)

	actualHighest := resolve()
	if expectedHighest == -1 {
		assert.Nil(c.t, actualHighest, "should not yet have a highest")
	} else if expectedHighest >= 0 {
		require.NotNil(c.t, actualHighest, "should have a highest at this point")
		assert.Equal(c.t, expectedHighest, actualHighest)
	}
}

func (c *checkpointTester) track(offset, batchSize int64) {
	c.t.Helper()

	resolve, err := c.checkpointer.Track(c.ctx, offset, batchSize)
	require.NoError(c.t, err, "Track should succeed")
	c.mu.Lock()
	c.resolvers[offset] = resolve
	c.mu.Unlock()
}
