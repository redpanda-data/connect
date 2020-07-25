package input

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCSVReaderHappy(t *testing.T) {
	var handle bytes.Buffer

	for _, msg := range []string{
		"header1,header2,header3",
		"foo1,foo2,foo3",
		"bar1,bar2,bar3",
		"baz1,baz2,baz3",
	} {
		handle.Write([]byte(msg))
		handle.Write([]byte("\n"))
	}

	ctored := false
	f, err := newCSVReader(
		func(ctx context.Context) (io.Reader, error) {
			if ctored {
				return nil, io.EOF
			}
			ctored = true
			return &handle, nil
		},
		func(ctx context.Context) {},
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		f.CloseAsync()
		require.NoError(t, f.WaitForClose(time.Second))
	})

	require.NoError(t, f.ConnectWithContext(context.Background()))

	for _, exp := range []string{
		`{"header1":"foo1","header2":"foo2","header3":"foo3"}`,
		`{"header1":"bar1","header2":"bar2","header3":"bar3"}`,
		`{"header1":"baz1","header2":"baz2","header3":"baz3"}`,
	} {
		var resMsg types.Message
		resMsg, _, err = f.ReadWithContext(context.Background())
		require.NoError(t, err)

		assert.Equal(t, exp, string(resMsg.Get(0).Get()))
	}

	_, _, err = f.ReadWithContext(context.Background())
	assert.Equal(t, types.ErrNotConnected, err)

	err = f.ConnectWithContext(context.Background())
	assert.Equal(t, types.ErrTypeClosed, err)
}

func TestCSVReaderCustomComma(t *testing.T) {
	var handle bytes.Buffer

	for _, msg := range []string{
		"header1|header2|header3",
		"foo1|foo2|foo3",
		"bar1|bar2|bar3",
		"baz1|baz2|baz3",
	} {
		handle.Write([]byte(msg))
		handle.Write([]byte("\n"))
	}

	ctored := false
	f, err := newCSVReader(
		func(ctx context.Context) (io.Reader, error) {
			if ctored {
				return nil, io.EOF
			}
			ctored = true
			return &handle, nil
		},
		func(ctx context.Context) {},
		optCSVSetComma('|'),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		f.CloseAsync()
		require.NoError(t, f.WaitForClose(time.Second))
	})

	require.NoError(t, f.ConnectWithContext(context.Background()))

	for _, exp := range []string{
		`{"header1":"foo1","header2":"foo2","header3":"foo3"}`,
		`{"header1":"bar1","header2":"bar2","header3":"bar3"}`,
		`{"header1":"baz1","header2":"baz2","header3":"baz3"}`,
	} {
		var resMsg types.Message
		resMsg, _, err = f.ReadWithContext(context.Background())
		require.NoError(t, err)

		assert.Equal(t, exp, string(resMsg.Get(0).Get()))
	}

	_, _, err = f.ReadWithContext(context.Background())
	assert.Equal(t, types.ErrNotConnected, err)

	err = f.ConnectWithContext(context.Background())
	assert.Equal(t, types.ErrTypeClosed, err)
}
func TestCSVReaderRelaxed(t *testing.T) {
	var handle bytes.Buffer

	for _, msg := range []string{
		"header1,header2,header3",
		"foo1,foo2,foo3",
		"bar1,bar2,bar3,bar4",
		"baz1,baz2,baz3",
		"buz1,buz2",
	} {
		handle.Write([]byte(msg))
		handle.Write([]byte("\n"))
	}

	ctored := false
	f, err := newCSVReader(
		func(ctx context.Context) (io.Reader, error) {
			if ctored {
				return nil, io.EOF
			}
			ctored = true
			return &handle, nil
		},
		func(ctx context.Context) {},
		optCSVSetStrict(false),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		f.CloseAsync()
		require.NoError(t, f.WaitForClose(time.Second))
	})

	require.NoError(t, f.ConnectWithContext(context.Background()))

	for _, exp := range []string{
		`{"header1":"foo1","header2":"foo2","header3":"foo3"}`,
		`["bar1","bar2","bar3","bar4"]`,
		`{"header1":"baz1","header2":"baz2","header3":"baz3"}`,
		`{"header1":"buz1","header2":"buz2"}`,
	} {
		var resMsg types.Message
		resMsg, _, err = f.ReadWithContext(context.Background())
		require.NoError(t, err)

		assert.Equal(t, exp, string(resMsg.Get(0).Get()))
	}

	_, _, err = f.ReadWithContext(context.Background())
	assert.Equal(t, types.ErrNotConnected, err)

	err = f.ConnectWithContext(context.Background())
	assert.Equal(t, types.ErrTypeClosed, err)
}

func TestCSVReaderStrict(t *testing.T) {
	var handle bytes.Buffer

	for _, msg := range []string{
		"header1,header2,header3",
		"foo1,foo2,foo3",
		"bar1,bar2,bar3,bar4",
		"baz1,baz2,baz3",
		"buz1,buz2",
	} {
		handle.Write([]byte(msg))
		handle.Write([]byte("\n"))
	}

	ctored := false
	f, err := newCSVReader(
		func(ctx context.Context) (io.Reader, error) {
			if ctored {
				return nil, io.EOF
			}
			ctored = true
			return &handle, nil
		},
		func(ctx context.Context) {},
		optCSVSetStrict(true),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		f.CloseAsync()
		require.NoError(t, f.WaitForClose(time.Second))
	})

	require.NoError(t, f.ConnectWithContext(context.Background()))

	for _, exp := range []interface{}{
		`{"header1":"foo1","header2":"foo2","header3":"foo3"}`,
		errors.New("record on line 3: wrong number of fields"),
		`{"header1":"baz1","header2":"baz2","header3":"baz3"}`,
		errors.New("record on line 5: wrong number of fields"),
	} {
		var resMsg types.Message
		resMsg, _, err = f.ReadWithContext(context.Background())

		switch expT := exp.(type) {
		case string:
			require.NoError(t, err)
			assert.Equal(t, expT, string(resMsg.Get(0).Get()))
		case error:
			assert.EqualError(t, err, expT.Error())
		}
	}

	_, _, err = f.ReadWithContext(context.Background())
	assert.Equal(t, types.ErrNotConnected, err)

	err = f.ConnectWithContext(context.Background())
	assert.Equal(t, types.ErrTypeClosed, err)
}
