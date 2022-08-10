package pure

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

func TestBloblangInterval(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*80)
	defer done()

	conf := input.NewGenerateConfig()
	conf.Mapping = `root = "hello world"`
	conf.Interval = "50ms"

	b, err := newGenerateReader(mock.NewManager(), conf)
	require.NoError(t, err)

	err = b.Connect(ctx)
	require.NoError(t, err)

	// First read is immediate.
	m, _, err := b.ReadBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())
	assert.Equal(t, "hello world", string(m.Get(0).AsBytes()))

	// Second takes 50ms.
	m, _, err = b.ReadBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())
	assert.Equal(t, "hello world", string(m.Get(0).AsBytes()))

	// Third takes another 50ms and therefore times out.
	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "action timed out")

	require.NoError(t, b.Close(context.Background()))
}

func TestBloblangZeroInterval(t *testing.T) {
	conf := input.NewGenerateConfig()
	conf.Mapping = `root = "hello world"`
	conf.Interval = "0s"
	_, err := newGenerateReader(mock.NewManager(), conf)
	require.NoError(t, err)
}

func TestBloblangCron(t *testing.T) {
	t.Skip()

	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*1100)
	defer done()

	conf := input.NewGenerateConfig()
	conf.Mapping = `root = "hello world"`
	conf.Interval = "@every 1s"

	b, err := newGenerateReader(mock.NewManager(), conf)
	require.NoError(t, err)
	assert.NotNil(t, b.schedule)
	assert.NotNil(t, b.location)

	err = b.Connect(ctx)
	require.NoError(t, err)

	// First takes 1s so.
	m, _, err := b.ReadBatch(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())
	assert.Equal(t, "hello world", string(m.Get(0).AsBytes()))

	// Second takes another 1s and therefore times out.
	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "action timed out")

	require.NoError(t, b.Close(context.Background()))
}

func TestBloblangMapping(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer done()

	conf := input.NewGenerateConfig()
	conf.Mapping = `root = {
		"id": count("docs")
	}`
	conf.Interval = "1ms"

	b, err := newGenerateReader(mock.NewManager(), conf)
	require.NoError(t, err)

	err = b.Connect(ctx)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		m, _, err := b.ReadBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, m.Len())
		assert.Equal(t, fmt.Sprintf(`{"id":%v}`, i+1), string(m.Get(0).AsBytes()))
	}
}

func TestBloblangRemaining(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer done()

	conf := input.NewGenerateConfig()
	conf.Mapping = `root = "foobar"`
	conf.Interval = "1ms"
	conf.Count = 10

	b, err := newGenerateReader(mock.NewManager(), conf)
	require.NoError(t, err)

	err = b.Connect(ctx)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		m, _, err := b.ReadBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, m.Len())
		assert.Equal(t, "foobar", string(m.Get(0).AsBytes()))
	}

	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "type was closed")

	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "type was closed")

	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "type was closed")
}

func TestBloblangRemainingBatched(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer done()

	conf := input.NewGenerateConfig()
	conf.Mapping = `root = "foobar"`
	conf.Interval = "1ms"
	conf.BatchSize = 2
	conf.Count = 9

	b, err := newGenerateReader(mock.NewManager(), conf)
	require.NoError(t, err)

	err = b.Connect(ctx)
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		m, _, err := b.ReadBatch(ctx)
		require.NoError(t, err)
		if i == 4 {
			require.Equal(t, 1, m.Len())
		} else {
			require.Equal(t, 2, m.Len())
			assert.Equal(t, "foobar", string(m[1].AsBytes()))
		}
		assert.Equal(t, "foobar", string(m[0].AsBytes()))
	}

	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "type was closed")

	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "type was closed")

	_, _, err = b.ReadBatch(ctx)
	assert.EqualError(t, err, "type was closed")
}

func TestBloblangUnbounded(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer done()

	conf := input.NewGenerateConfig()
	conf.Mapping = `root = "foobar"`
	conf.Interval = "0s"

	b, err := newGenerateReader(mock.NewManager(), conf)
	require.NoError(t, err)

	err = b.Connect(ctx)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		m, _, err := b.ReadBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, m.Len())
		assert.Equal(t, "foobar", string(m.Get(0).AsBytes()))
	}

	require.NoError(t, b.Close(context.Background()))
}

func TestBloblangUnboundedEmpty(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer done()

	conf := input.NewGenerateConfig()
	conf.Mapping = `root = "foobar"`
	conf.Interval = ""

	b, err := newGenerateReader(mock.NewManager(), conf)
	require.NoError(t, err)

	err = b.Connect(ctx)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		m, _, err := b.ReadBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, m.Len())
		assert.Equal(t, "foobar", string(m.Get(0).AsBytes()))
	}

	require.NoError(t, b.Close(context.Background()))
}
