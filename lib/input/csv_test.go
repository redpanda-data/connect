package input

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/response"
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

func TestCSVReaderGroupCount(t *testing.T) {
	var handle bytes.Buffer

	for _, msg := range []string{
		"foo,bar,baz",
		"foo1,bar1,baz1",
		"foo2,bar2,baz2",
		"foo3,bar3,baz3",
		"foo4,bar4,baz4",
		"foo5,bar5,baz5",
		"foo6,bar6,baz6",
		"foo7,bar7,baz7",
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
		optCSVSetGroupCount(3),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		f.CloseAsync()
		require.NoError(t, f.WaitForClose(time.Second))
	})

	require.NoError(t, f.ConnectWithContext(context.Background()))

	for _, exp := range [][]string{
		{
			`{"bar":"bar1","baz":"baz1","foo":"foo1"}`,
			`{"bar":"bar2","baz":"baz2","foo":"foo2"}`,
			`{"bar":"bar3","baz":"baz3","foo":"foo3"}`,
		},
		{
			`{"bar":"bar4","baz":"baz4","foo":"foo4"}`,
			`{"bar":"bar5","baz":"baz5","foo":"foo5"}`,
			`{"bar":"bar6","baz":"baz6","foo":"foo6"}`,
		},
		{
			`{"bar":"bar7","baz":"baz7","foo":"foo7"}`,
		},
	} {
		var resMsg types.Message
		resMsg, _, err = f.ReadWithContext(context.Background())
		require.NoError(t, err)

		require.Equal(t, len(exp), resMsg.Len())
		for i := 0; i < len(exp); i++ {
			assert.Equal(t, exp[i], string(resMsg.Get(i).Get()))
		}
	}

	_, _, err = f.ReadWithContext(context.Background())
	assert.Equal(t, types.ErrNotConnected, err)

	err = f.ConnectWithContext(context.Background())
	assert.Equal(t, types.ErrTypeClosed, err)
}

func TestCSVReadersTwoFiles(t *testing.T) {
	var handleOne, handleTwo bytes.Buffer

	for _, msg := range []string{
		"header1,header2,header3",
		"foo1,foo2,foo3",
		"bar1,bar2,bar3",
		"baz1,baz2,baz3",
	} {
		handleOne.Write([]byte(msg))
		handleOne.Write([]byte("\n"))
	}

	for _, msg := range []string{
		"header4,header5,header6",
		"foo1,foo2,foo3",
		"bar1,bar2,bar3",
		"baz1,baz2,baz3",
	} {
		handleTwo.Write([]byte(msg))
		handleTwo.Write([]byte("\n"))
	}

	consumedFirst, consumedSecond := false, false

	f, err := newCSVReader(
		func(ctx context.Context) (io.Reader, error) {
			if !consumedFirst {
				consumedFirst = true
				return &handleOne, nil
			} else if !consumedSecond {
				consumedSecond = true
				return &handleTwo, nil
			}
			return nil, io.EOF
		},
		func(ctx context.Context) {},
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		f.CloseAsync()
		require.NoError(t, f.WaitForClose(time.Second))
	})

	require.NoError(t, f.ConnectWithContext(context.Background()))

	for i, exp := range []string{
		`{"header1":"foo1","header2":"foo2","header3":"foo3"}`,
		`{"header1":"bar1","header2":"bar2","header3":"bar3"}`,
		`{"header1":"baz1","header2":"baz2","header3":"baz3"}`,
		`{"header4":"foo1","header5":"foo2","header6":"foo3"}`,
		`{"header4":"bar1","header5":"bar2","header6":"bar3"}`,
		`{"header4":"baz1","header5":"baz2","header6":"baz3"}`,
	} {
		var resMsg types.Message
		var ackFn reader.AsyncAckFn
		resMsg, ackFn, err = f.ReadWithContext(context.Background())
		if err == types.ErrNotConnected {
			require.NoError(t, f.ConnectWithContext(context.Background()))
			resMsg, ackFn, err = f.ReadWithContext(context.Background())
		}
		require.NoError(t, err, i)
		assert.Equal(t, exp, string(resMsg.Get(0).Get()), i)
		ackFn(context.Background(), response.NewAck())
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
