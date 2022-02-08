package input

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBloblangInterval(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*80)
	defer done()

	conf := NewBloblangConfig()
	conf.Mapping = `root = "hello world"`
	conf.Interval = "50ms"

	b, err := newBloblang(mock.NewManager(), conf)
	require.NoError(t, err)

	err = b.ConnectWithContext(ctx)
	require.NoError(t, err)

	// First read is immediate.
	m, _, err := b.ReadWithContext(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())
	assert.Equal(t, "hello world", string(m.Get(0).Get()))

	// Second takes 50ms.
	m, _, err = b.ReadWithContext(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())
	assert.Equal(t, "hello world", string(m.Get(0).Get()))

	// Third takes another 50ms and therefore times out.
	_, _, err = b.ReadWithContext(ctx)
	assert.EqualError(t, err, "action timed out")

	b.CloseAsync()
}

func TestBloblangZeroInterval(t *testing.T) {
	conf := NewBloblangConfig()
	conf.Mapping = `root = "hello world"`
	conf.Interval = "0s"
	_, err := newBloblang(mock.NewManager(), conf)
	require.NoError(t, err)
}

func TestBloblangCron(t *testing.T) {
	t.Skip()

	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*1100)
	defer done()

	conf := NewBloblangConfig()
	conf.Mapping = `root = "hello world"`
	conf.Interval = "@every 1s"

	b, err := newBloblang(mock.NewManager(), conf)
	require.NoError(t, err)
	assert.NotNil(t, b.schedule)
	assert.NotNil(t, b.location)

	err = b.ConnectWithContext(ctx)
	require.NoError(t, err)

	// First takes 1s so.
	m, _, err := b.ReadWithContext(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, m.Len())
	assert.Equal(t, "hello world", string(m.Get(0).Get()))

	// Second takes another 1s and therefore times out.
	_, _, err = b.ReadWithContext(ctx)
	assert.EqualError(t, err, "action timed out")

	b.CloseAsync()
}

func TestBloblangMapping(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer done()

	conf := NewBloblangConfig()
	conf.Mapping = `root = {
		"id": count("docs")
	}`
	conf.Interval = "1ms"

	b, err := newBloblang(mock.NewManager(), conf)
	require.NoError(t, err)

	err = b.ConnectWithContext(ctx)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		m, _, err := b.ReadWithContext(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, m.Len())
		assert.Equal(t, fmt.Sprintf(`{"id":%v}`, i+1), string(m.Get(0).Get()))
	}
}

func TestBloblangRemaining(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer done()

	conf := NewBloblangConfig()
	conf.Mapping = `root = "foobar"`
	conf.Interval = "1ms"
	conf.Count = 10

	b, err := newBloblang(mock.NewManager(), conf)
	require.NoError(t, err)

	err = b.ConnectWithContext(ctx)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		m, _, err := b.ReadWithContext(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, m.Len())
		assert.Equal(t, "foobar", string(m.Get(0).Get()))
	}

	_, _, err = b.ReadWithContext(ctx)
	assert.EqualError(t, err, "type was closed")

	_, _, err = b.ReadWithContext(ctx)
	assert.EqualError(t, err, "type was closed")

	_, _, err = b.ReadWithContext(ctx)
	assert.EqualError(t, err, "type was closed")
}

func TestBloblangUnbounded(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer done()

	conf := NewBloblangConfig()
	conf.Mapping = `root = "foobar"`
	conf.Interval = "0s"

	b, err := newBloblang(mock.NewManager(), conf)
	require.NoError(t, err)

	err = b.ConnectWithContext(ctx)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		m, _, err := b.ReadWithContext(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, m.Len())
		assert.Equal(t, "foobar", string(m.Get(0).Get()))
	}

	b.CloseAsync()
	require.NoError(t, b.WaitForClose(time.Second))
}

func TestBloblangUnboundedEmpty(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer done()

	conf := NewBloblangConfig()
	conf.Mapping = `root = "foobar"`
	conf.Interval = ""

	b, err := newBloblang(mock.NewManager(), conf)
	require.NoError(t, err)

	err = b.ConnectWithContext(ctx)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		m, _, err := b.ReadWithContext(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, m.Len())
		assert.Equal(t, "foobar", string(m.Get(0).Get()))
	}

	b.CloseAsync()
	require.NoError(t, b.WaitForClose(time.Second))
}
