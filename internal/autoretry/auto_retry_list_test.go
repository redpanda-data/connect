package autoretry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errCustomEOF = errors.New("custom EOF")

func TestRetryListAllAcks(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	var acked []string

	data := []string{"foo", "bar", "baz"}
	l := NewList(func(ctx context.Context) (t string, aFn AckFunc, err error) {
		if len(data) == 0 {
			err = errCustomEOF
			return
		}
		next := data[0]
		data = data[1:]
		return next, func(ctx context.Context, err error) error {
			acked = append(acked, next)
			return nil
		}, nil
	}, nil)

	res, fooFn, err := l.Shift(tCtx, true)
	require.NoError(t, err)
	assert.Equal(t, "foo", res)

	res, barFn, err := l.Shift(tCtx, true)
	require.NoError(t, err)
	assert.Equal(t, "bar", res)

	res, bazFn, err := l.Shift(tCtx, true)
	require.NoError(t, err)
	assert.Equal(t, "baz", res)

	_, _, err = l.Shift(tCtx, true)
	require.Equal(t, errCustomEOF, err)

	assert.NoError(t, bazFn(tCtx, nil))
	assert.NoError(t, barFn(tCtx, nil))
	assert.NoError(t, fooFn(tCtx, nil))

	assert.Equal(t, []string{
		"baz", "bar", "foo",
	}, acked)

	fmt.Println("last shift")
	_, _, err = l.Shift(tCtx, false)
	assert.Equal(t, ErrExhausted, err)

	require.NoError(t, l.Close(tCtx))
}

func TestRetryListNacks(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	var acked []string

	data := []string{"foo", "bar", "baz"}
	l := NewList(func(ctx context.Context) (t string, aFn AckFunc, err error) {
		if len(data) == 0 {
			err = errCustomEOF
			return
		}
		next := data[0]
		data = data[1:]
		return next, func(ctx context.Context, err error) error {
			acked = append(acked, next)
			return nil
		}, nil
	}, nil)

	v, fooFn, err := l.Shift(tCtx, true)
	require.NoError(t, err)
	assert.Equal(t, "foo", v)

	v, barFn, err := l.Shift(tCtx, true)
	require.NoError(t, err)
	assert.Equal(t, "bar", v)

	v, bazFn, err := l.Shift(tCtx, true)
	require.NoError(t, err)
	assert.Equal(t, "baz", v)

	_, _, err = l.Shift(tCtx, true)
	require.Equal(t, errCustomEOF, err)

	assert.NoError(t, bazFn(tCtx, errors.New("baz nope")))
	assert.NoError(t, barFn(tCtx, errors.New("bar nope")))
	assert.NoError(t, fooFn(tCtx, errors.New("foo nope")))

	assert.Equal(t, []string(nil), acked)

	v, bazFn, err = l.Shift(tCtx, false)
	require.NoError(t, err)
	assert.Equal(t, "baz", v)

	v, barFn, err = l.Shift(tCtx, false)
	require.NoError(t, err)
	assert.Equal(t, "bar", v)
	assert.NoError(t, barFn(tCtx, errors.New("bar nope again")))

	v, fooFn, err = l.Shift(tCtx, false)
	require.NoError(t, err)
	assert.Equal(t, "foo", v)

	assert.NoError(t, fooFn(tCtx, nil))
	assert.NoError(t, bazFn(tCtx, nil))

	assert.Equal(t, []string{
		"foo", "baz",
	}, acked)

	v, barFn, err = l.Shift(tCtx, false)
	require.NoError(t, err)
	assert.Equal(t, "bar", v)

	cancelledCtx, done := context.WithTimeout(tCtx, time.Millisecond*50)
	defer done()

	_, _, err = l.Shift(cancelledCtx, false)
	assert.Equal(t, cancelledCtx.Err(), err)

	assert.NoError(t, barFn(tCtx, nil))

	assert.Equal(t, []string{
		"foo", "baz", "bar",
	}, acked)

	_, _, err = l.Shift(tCtx, false)
	assert.Equal(t, ErrExhausted, err)

	require.NoError(t, l.Close(tCtx))
}

func TestRetryListNackMutator(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	var acked []string

	data := []string{"foo"}
	l := NewList(func(ctx context.Context) (t string, aFn AckFunc, err error) {
		if len(data) == 0 {
			err = errCustomEOF
			return
		}
		next := data[0]
		data = data[1:]
		return next, func(ctx context.Context, err error) error {
			acked = append(acked, next)
			return nil
		}, nil
	}, func(t string, err error) string {
		return t + " and " + err.Error()
	})

	v, fooFn, err := l.Shift(tCtx, true)
	require.NoError(t, err)
	assert.Equal(t, "foo", v)

	_, _, err = l.Shift(tCtx, true)
	require.Equal(t, errCustomEOF, err)

	assert.NoError(t, fooFn(tCtx, errors.New("first error")))
	assert.Equal(t, []string(nil), acked)

	v, fooFn, err = l.Shift(tCtx, false)
	require.NoError(t, err)
	assert.Equal(t, "foo and first error", v)

	assert.NoError(t, fooFn(tCtx, errors.New("second error")))
	assert.Equal(t, []string(nil), acked)

	v, fooFn, err = l.Shift(tCtx, false)
	require.NoError(t, err)
	assert.Equal(t, "foo and first error and second error", v)

	assert.NoError(t, fooFn(tCtx, errors.New("third error")))
	assert.Equal(t, []string(nil), acked)

	v, fooFn, err = l.Shift(tCtx, false)
	require.NoError(t, err)
	assert.Equal(t, "foo and first error and second error and third error", v)

	assert.NoError(t, fooFn(tCtx, nil))

	assert.Equal(t, []string{
		"foo",
	}, acked)

	_, _, err = l.Shift(tCtx, false)
	assert.Equal(t, ErrExhausted, err)

	require.NoError(t, l.Close(tCtx))
}
