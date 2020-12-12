package checkpoint

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSequential(t *testing.T) {
	c := New(0)
	assert.Equal(t, 0, c.Highest())
	assert.Equal(t, 0, c.Pending())

	require.NoError(t, c.Track(1))
	require.NoError(t, c.Track(2))
	require.NoError(t, c.Track(3))
	assert.Equal(t, 0, c.Highest())
	assert.Equal(t, 3, c.Pending())

	v, err := c.Resolve(1)
	require.NoError(t, err)
	assert.Equal(t, 1, v)
	assert.Equal(t, 1, c.Highest())
	assert.Equal(t, 2, c.Pending())

	v, err = c.Resolve(2)
	require.NoError(t, err)
	assert.Equal(t, 2, v)
	assert.Equal(t, 2, c.Highest())
	assert.Equal(t, 1, c.Pending())

	v, err = c.Resolve(3)
	require.NoError(t, err)
	assert.Equal(t, 3, v)
	assert.Equal(t, 3, c.Highest())
	assert.Equal(t, 0, c.Pending())

	require.NoError(t, c.Track(4))
	assert.Equal(t, 1, c.Pending())

	v, err = c.Resolve(4)
	require.NoError(t, err)
	assert.Equal(t, 4, v)
	assert.Equal(t, 4, c.Highest())
	assert.Equal(t, 0, c.Pending())
}

func TestOutOfSync(t *testing.T) {
	c := New(0)
	assert.Equal(t, 0, c.Highest())

	require.NoError(t, c.Track(1))
	require.NoError(t, c.Track(2))
	require.NoError(t, c.Track(3))
	require.NoError(t, c.Track(4))
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
	c := New(0)
	require.NoError(t, c.Track(10))
	require.EqualError(t, c.Track(5), "provided offset was out of sync")
}

func TestResolveUnfound(t *testing.T) {
	c := New(0)
	require.NoError(t, c.Track(10))
	_, err := c.Resolve(5)
	require.EqualError(t, err, "resolved offset was not tracked")
}

func TestSequentialLarge(t *testing.T) {
	c := New(0)
	for i := 0; i < 1000; i++ {
		require.NoError(t, c.Track(i))
	}
	for i := 0; i < 1000; i++ {
		v, err := c.Resolve(i)
		require.NoError(t, err)
		assert.Equal(t, i, v)
		assert.Equal(t, i, c.Highest())
	}
}

func TestSequentialChunks(t *testing.T) {
	c := New(0)
	chunkSize := 100
	for i := 0; i < 10; i++ {
		for j := 0; j < chunkSize; j++ {
			offset := i*chunkSize + j
			require.NoError(t, c.Track(offset))
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
	c := New(0)
	for i := 0; i < 1000; i++ {
		require.NoError(t, c.Track(i))
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
	c := New(0)
	indexes := map[int]struct{}{}
	for i := 0; i < 1000; i++ {
		require.NoError(t, c.Track(i))
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
	c := New(0)
	chunkSize := 100
	N := b.N / chunkSize
	for i := 0; i < N; i++ {
		for j := 0; j < chunkSize; j++ {
			offset := i*chunkSize + j
			err := c.Track(offset)
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
	c := New(0)
	chunkSize := 100
	N := b.N / chunkSize
	for i := 0; i < N; i++ {
		for j := 0; j < chunkSize; j++ {
			offset := i*chunkSize + j
			err := c.Track(offset)
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
	c := New(0)
	chunkSize := 1000
	N := b.N / chunkSize
	for i := 0; i < N; i++ {
		for j := 0; j < chunkSize; j++ {
			offset := i*chunkSize + j
			err := c.Track(offset)
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
	c := New(0)
	for i := 0; i < b.N; i++ {
		err := c.Track(i)
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
