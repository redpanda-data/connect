package autoretry

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryListAllAcks(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	var acked []string

	l := NewList[string](nil)

	fooFn := l.Adopt(tCtx, "foo", func(ctx context.Context, err error) error {
		acked = append(acked, "foo")
		return nil
	})

	barFn := l.Adopt(tCtx, "bar", func(ctx context.Context, err error) error {
		acked = append(acked, "bar")
		return nil
	})

	bazFn := l.Adopt(tCtx, "baz", func(ctx context.Context, err error) error {
		acked = append(acked, "baz")
		return nil
	})

	assert.NoError(t, bazFn(tCtx, nil))
	assert.NoError(t, barFn(tCtx, nil))
	assert.NoError(t, fooFn(tCtx, nil))

	assert.Equal(t, []string{
		"baz", "bar", "foo",
	}, acked)

	_, _, err := l.Shift(tCtx)
	assert.Equal(t, io.EOF, err)
}

func TestRetryListNacks(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	var acked []string

	l := NewList[string](nil)

	fooFn := l.Adopt(tCtx, "foo", func(ctx context.Context, err error) error {
		acked = append(acked, "foo")
		return nil
	})

	barFn := l.Adopt(tCtx, "bar", func(ctx context.Context, err error) error {
		acked = append(acked, "bar")
		return nil
	})

	bazFn := l.Adopt(tCtx, "baz", func(ctx context.Context, err error) error {
		acked = append(acked, "baz")
		return nil
	})

	_, _, ok := l.TryShift(tCtx)
	assert.False(t, ok)

	assert.NoError(t, bazFn(tCtx, errors.New("baz nope")))
	assert.NoError(t, barFn(tCtx, errors.New("bar nope")))
	assert.NoError(t, fooFn(tCtx, errors.New("foo nope")))

	assert.Equal(t, []string(nil), acked)

	var v string
	v, bazFn, ok = l.TryShift(tCtx)
	require.True(t, ok)
	assert.Equal(t, "baz", v)

	v, barFn, ok = l.TryShift(tCtx)
	require.True(t, ok)
	assert.Equal(t, "bar", v)
	assert.NoError(t, barFn(tCtx, errors.New("bar nope again")))

	v, fooFn, ok = l.TryShift(tCtx)
	require.True(t, ok)
	assert.Equal(t, "foo", v)

	assert.NoError(t, fooFn(tCtx, nil))
	assert.NoError(t, bazFn(tCtx, nil))

	assert.Equal(t, []string{
		"foo", "baz",
	}, acked)

	var err error
	v, barFn, err = l.Shift(tCtx)
	require.NoError(t, err)
	assert.Equal(t, "bar", v)

	cancelledCtx, done := context.WithTimeout(tCtx, time.Millisecond*50)
	defer done()

	_, _, err = l.Shift(cancelledCtx)
	assert.Equal(t, cancelledCtx.Err(), err)

	assert.NoError(t, barFn(tCtx, nil))

	assert.Equal(t, []string{
		"foo", "baz", "bar",
	}, acked)

	_, _, err = l.Shift(tCtx)
	assert.Equal(t, io.EOF, err)
}

func TestRetryListNackMutator(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	var acked []string

	l := NewList(func(t string, err error) string {
		return t + " and " + err.Error()
	})

	fooFn := l.Adopt(tCtx, "foo", func(ctx context.Context, err error) error {
		acked = append(acked, "foo")
		return nil
	})

	_, _, ok := l.TryShift(tCtx)
	assert.False(t, ok)

	assert.NoError(t, fooFn(tCtx, errors.New("first error")))
	assert.Equal(t, []string(nil), acked)

	var v string
	v, fooFn, ok = l.TryShift(tCtx)
	require.True(t, ok)
	assert.Equal(t, "foo and first error", v)

	assert.NoError(t, fooFn(tCtx, errors.New("second error")))
	assert.Equal(t, []string(nil), acked)

	v, fooFn, ok = l.TryShift(tCtx)
	require.True(t, ok)
	assert.Equal(t, "foo and first error and second error", v)

	assert.NoError(t, fooFn(tCtx, errors.New("third error")))
	assert.Equal(t, []string(nil), acked)

	v, fooFn, ok = l.TryShift(tCtx)
	require.True(t, ok)
	assert.Equal(t, "foo and first error and second error and third error", v)

	assert.NoError(t, fooFn(tCtx, nil))

	assert.Equal(t, []string{
		"foo",
	}, acked)

	_, _, err := l.Shift(tCtx)
	assert.Equal(t, io.EOF, err)
}
