package checkpoint

import (
	"context"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSequential(t *testing.T) {
	c := NewCapped(1000)
	assert.Equal(t, 0, c.Highest())

	ctx := context.Background()

	require.NoError(t, c.Track(ctx, 1))
	require.NoError(t, c.Track(ctx, 2))
	require.NoError(t, c.Track(ctx, 3))
	assert.Equal(t, 0, c.Highest())

	v, err := c.Resolve(1)
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	assert.Equal(t, 1, c.Highest())

	v, err = c.Resolve(2)
	require.NoError(t, err)
	assert.Equal(t, 2, v)
	assert.Equal(t, 2, c.Highest())

	v, err = c.Resolve(3)
	require.NoError(t, err)
	assert.Equal(t, 3, v)
	assert.Equal(t, 3, c.Highest())

	require.NoError(t, c.Track(ctx, 4))

	v, err = c.Resolve(4)
	require.NoError(t, err)
	assert.Equal(t, 4, v)
	assert.Equal(t, 4, c.Highest())
}

func TestBigJumps(t *testing.T) {
	c := NewCapped(1)
	assert.Equal(t, 0, c.Highest())

	ctx, done := context.WithTimeout(context.Background(), time.Second*5)
	defer done()

	require.NoError(t, c.Track(ctx, 1000))
	assert.Equal(t, 0, c.Highest())

	v, err := c.Resolve(1000)
	require.NoError(t, err)
	assert.Equal(t, 1000, v)
	assert.Equal(t, 1000, c.Highest())

	require.NoError(t, c.Track(ctx, 2000))
	assert.Equal(t, 1000, c.Highest())

	v, err = c.Resolve(2000)
	require.NoError(t, err)
	assert.Equal(t, 2000, v)
	assert.Equal(t, 2000, c.Highest())
}

func TestStartBig(t *testing.T) {
	c := NewCapped(100)
	assert.Equal(t, 0, c.Highest())

	ctx := context.Background()

	require.NoError(t, c.Track(ctx, 500))
	require.NoError(t, c.Track(ctx, 501))
	require.NoError(t, c.Track(ctx, 502))
	assert.Equal(t, 0, c.Highest())

	v, err := c.Resolve(500)
	require.NoError(t, err)
	assert.Equal(t, 500, v)
	assert.Equal(t, 500, c.Highest())

	v, err = c.Resolve(501)
	require.NoError(t, err)
	assert.Equal(t, 501, v)
	assert.Equal(t, 501, c.Highest())

	v, err = c.Resolve(502)
	require.NoError(t, err)
	assert.Equal(t, 502, v)
	assert.Equal(t, 502, c.Highest())

	require.NoError(t, c.Track(ctx, 503))

	v, err = c.Resolve(503)
	require.NoError(t, err)
	assert.Equal(t, 503, v)
	assert.Equal(t, 503, c.Highest())
}

func TestCapHappy(t *testing.T) {
	c := NewCapped(100)
	assert.Equal(t, 0, c.Highest())

	ctx, cancel := context.WithCancel(context.Background())

	require.NoError(t, c.Track(ctx, 100))
	n, err := c.Resolve(100)
	require.NoError(t, err)
	assert.Equal(t, 100, n)

	for i := 101; i <= 200; i++ {
		require.NoError(t, c.Track(ctx, i))
	}

	cancel()
	require.Equal(t, types.ErrTimeout, c.Track(ctx, 201))

	go func() {
		<-time.After(time.Millisecond * 100)
		c.Resolve(101)
	}()
	require.NoError(t, c.Track(context.Background(), 201))

	for i := 101; i <= 200; i++ {
		n, err := c.Resolve(i + 1)
		require.NoError(t, err)
		assert.Equal(t, i+1, n)
	}
}

func TestOutOfSync(t *testing.T) {
	c := NewCapped(1000)
	assert.Equal(t, 0, c.Highest())

	ctx := context.Background()

	require.NoError(t, c.Track(ctx, 1))
	require.NoError(t, c.Track(ctx, 2))
	require.NoError(t, c.Track(ctx, 3))
	require.NoError(t, c.Track(ctx, 4))
	assert.Equal(t, 0, c.Highest())

	v, err := c.Resolve(2)
	require.NoError(t, err)
	assert.Equal(t, 0, v)
	assert.Equal(t, 0, c.Highest())

	v, err = c.Resolve(1)
	require.NoError(t, err)
	assert.Equal(t, 2, v)
	assert.Equal(t, 2, c.Highest())

	v, err = c.Resolve(3)
	require.NoError(t, err)
	assert.Equal(t, 3, v)
	assert.Equal(t, 3, c.Highest())

	v, err = c.Resolve(4)
	require.NoError(t, err)
	assert.Equal(t, 4, v)
	assert.Equal(t, 4, c.Highest())
}

func TestInsertOutOfSequence(t *testing.T) {
	c := NewCapped(1000)
	ctx := context.Background()
	require.NoError(t, c.Track(ctx, 10))
	require.EqualError(t, c.Track(ctx, 5), "provided offset was out of sync")
}

func TestResolveUnfound(t *testing.T) {
	c := NewCapped(1000)
	ctx := context.Background()
	require.NoError(t, c.Track(ctx, 10))
	_, err := c.Resolve(5)
	require.EqualError(t, err, "resolved offset was not tracked")
}

func TestSequentialLarge(t *testing.T) {
	c := NewCapped(1000)
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		require.NoError(t, c.Track(ctx, i))
	}
	for i := 0; i < 1000; i++ {
		v, err := c.Resolve(i)
		require.NoError(t, err)
		assert.Equal(t, i, v)
		assert.Equal(t, i, c.Highest())
	}
}

func TestSequentialChunks(t *testing.T) {
	c := NewCapped(1000)
	ctx := context.Background()
	chunkSize := 100
	for i := 0; i < 10; i++ {
		for j := 0; j < chunkSize; j++ {
			offset := i*chunkSize + j
			require.NoError(t, c.Track(ctx, offset))
		}
		for j := 0; j < chunkSize; j++ {
			offset := i*chunkSize + j
			v, err := c.Resolve(offset)
			require.NoError(t, err)
			assert.Equal(t, offset, v)
			assert.Equal(t, offset, c.Highest())
		}
	}
}

func TestSequentialReverseLarge(t *testing.T) {
	c := NewCapped(1000)
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		require.NoError(t, c.Track(ctx, i))
	}
	for i := 999; i >= 0; i-- {
		v, err := c.Resolve(i)
		require.NoError(t, err)
		if i == 0 {
			assert.Equal(t, 999, v)
			assert.Equal(t, 999, c.Highest())
		} else {
			assert.Equal(t, 0, v)
			assert.Equal(t, 0, c.Highest())
		}
	}
}

func TestSequentialRandomLarge(t *testing.T) {
	c := NewCapped(1000)
	ctx := context.Background()
	indexes := map[int]struct{}{}
	for i := 0; i < 1000; i++ {
		require.NoError(t, c.Track(ctx, i))
		indexes[i] = struct{}{}
	}
	for i := range indexes {
		delete(indexes, i)
		v, err := c.Resolve(i)
		require.NoError(t, err)
		if len(indexes) == 0 {
			assert.Equal(t, 999, v)
			assert.Equal(t, 999, c.Highest())
		} else {
			for k := range indexes {
				assert.False(t, k < v)
				assert.False(t, k < c.Highest())
			}
		}
	}
}

func BenchmarkChunked100(b *testing.B) {
	b.ReportAllocs()
	c := NewCapped(1000)
	ctx := context.Background()
	chunkSize := 100
	N := b.N / chunkSize
	for i := 0; i < N; i++ {
		for j := 0; j < chunkSize; j++ {
			offset := i*chunkSize + j
			err := c.Track(ctx, offset)
			if err != nil {
				b.Fatal(err)
			}
		}
		for j := 0; j < chunkSize; j++ {
			offset := i*chunkSize + j
			v, err := c.Resolve(offset)
			if err != nil {
				b.Fatal(err)
			}
			if offset != v {
				b.Errorf("Wrong value: %v != %v", offset, v)
			}
		}
	}
}

func BenchmarkChunkedReverse100(b *testing.B) {
	b.ReportAllocs()
	c := NewCapped(1000)
	ctx := context.Background()
	chunkSize := 100
	N := b.N / chunkSize
	for i := 0; i < N; i++ {
		for j := 0; j < chunkSize; j++ {
			offset := i*chunkSize + j
			err := c.Track(ctx, offset)
			if err != nil {
				b.Fatal(err)
			}
		}
		for j := chunkSize - 1; j >= 0; j-- {
			offset := i*chunkSize + j
			v, err := c.Resolve(offset)
			if err != nil {
				b.Fatal(err)
			}
			var exp int
			if i > 0 {
				exp = (i * chunkSize) - 1
			}
			if j == 0 {
				exp = ((i + 1) * chunkSize) - 1
			}
			if exp != v {
				b.Errorf("Wrong value: %v != %v", exp, v)
			}
		}
	}
}

func BenchmarkChunkedReverse1000(b *testing.B) {
	b.ReportAllocs()
	c := NewCapped(1001)
	ctx := context.Background()
	chunkSize := 1000
	N := b.N / chunkSize
	for i := 0; i < N; i++ {
		for j := 0; j < chunkSize; j++ {
			offset := i*chunkSize + j
			err := c.Track(ctx, offset)
			if err != nil {
				b.Fatal(err)
			}
		}
		for j := chunkSize - 1; j >= 0; j-- {
			offset := i*chunkSize + j
			v, err := c.Resolve(offset)
			if err != nil {
				b.Fatal(err)
			}
			var exp int
			if i > 0 {
				exp = (i * chunkSize) - 1
			}
			if j == 0 {
				exp = ((i + 1) * chunkSize) - 1
			}
			if exp != v {
				b.Errorf("Wrong value: %v != %v", exp, v)
			}
		}
	}
}

func BenchmarkSequential(b *testing.B) {
	b.ReportAllocs()
	c := NewCapped(b.N + 1)
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		err := c.Track(ctx, i)
		if err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < b.N; i++ {
		v, err := c.Resolve(i)
		if err != nil {
			b.Fatal(err)
		}
		if i != v {
			b.Errorf("Wrong value: %v != %v", i, v)
		}
	}
}
