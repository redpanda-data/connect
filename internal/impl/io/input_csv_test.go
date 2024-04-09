package io

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"
)

func TestCSVReaderHappy(t *testing.T) {
	var handle bytes.Buffer

	for _, msg := range []string{
		"header1,header2,header3",
		"foo1,foo2,foo3",
		"bar1,bar2,bar3",
		"baz1,baz2,baz3",
	} {
		handle.WriteString(msg)
		handle.WriteString("\n")
	}

	dummyFile := "foo/bar.csv"
	dummyTimeUTC := time.Now().UTC()
	ctored := false
	f, err := newCSVReader(
		func(ctx context.Context) (csvScannerInfo, error) {
			if ctored {
				return csvScannerInfo{}, io.EOF
			}
			ctored = true
			return csvScannerInfo{
				handle:      &handle,
				currentPath: dummyFile,
				modTimeUTC:  dummyTimeUTC,
			}, nil
		},
		func(ctx context.Context) {},
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		require.NoError(t, f.Close(ctx))
		done()
	})

	require.NoError(t, f.Connect(context.Background()))

	for _, exp := range []string{
		`{"header1":"foo1","header2":"foo2","header3":"foo3"}`,
		`{"header1":"bar1","header2":"bar2","header3":"bar3"}`,
		`{"header1":"baz1","header2":"baz2","header3":"baz3"}`,
	} {
		var resMsg service.MessageBatch
		resMsg, _, err = f.ReadBatch(context.Background())
		require.NoError(t, err)

		msgBytes, err := resMsg[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, exp, string(msgBytes))

		m, _ := resMsg[0].MetaGet("path")
		assert.Equal(t, dummyFile, m)
		m, _ = resMsg[0].MetaGet("mod_time")
		assert.Equal(t, dummyTimeUTC.Format(time.RFC3339), m)
		m, _ = resMsg[0].MetaGet("mod_time_unix")
		assert.Equal(t, strconv.Itoa(int(dummyTimeUTC.Unix())), m)
	}

	_, _, err = f.ReadBatch(context.Background())
	assert.Equal(t, service.ErrNotConnected, err)

	err = f.Connect(context.Background())
	assert.Equal(t, service.ErrEndOfInput, err)
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
		handle.WriteString(msg)
		handle.WriteString("\n")
	}

	ctored := false
	f, err := newCSVReader(
		func(ctx context.Context) (csvScannerInfo, error) {
			if ctored {
				return csvScannerInfo{}, io.EOF
			}
			ctored = true
			return csvScannerInfo{handle: &handle}, nil
		},
		func(ctx context.Context) {},
		optCSVSetGroupCount(3),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		require.NoError(t, f.Close(ctx))
		done()
	})

	require.NoError(t, f.Connect(context.Background()))

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
		var resMsg service.MessageBatch
		resMsg, _, err = f.ReadBatch(context.Background())
		require.NoError(t, err)

		require.Equal(t, len(exp), len(resMsg))
		for i := 0; i < len(exp); i++ {
			mBytes, err := resMsg[i].AsBytes()
			require.NoError(t, err)
			assert.Equal(t, exp[i], string(mBytes))
		}
	}

	_, _, err = f.ReadBatch(context.Background())
	assert.Equal(t, service.ErrNotConnected, err)

	err = f.Connect(context.Background())
	assert.Equal(t, service.ErrEndOfInput, err)
}

func TestCSVReadersTwoFiles(t *testing.T) {
	var handleOne, handleTwo bytes.Buffer

	for _, msg := range []string{
		"header1,header2,header3",
		"foo1,foo2,foo3",
		"bar1,bar2,bar3",
		"baz1,baz2,baz3",
	} {
		handleOne.WriteString(msg)
		handleOne.WriteString("\n")
	}

	for _, msg := range []string{
		"header4,header5,header6",
		"foo1,foo2,foo3",
		"bar1,bar2,bar3",
		"baz1,baz2,baz3",
	} {
		handleTwo.WriteString(msg)
		handleTwo.WriteString("\n")
	}

	consumedFirst, consumedSecond := false, false

	f, err := newCSVReader(
		func(ctx context.Context) (csvScannerInfo, error) {
			if !consumedFirst {
				consumedFirst = true
				return csvScannerInfo{handle: &handleOne}, nil
			} else if !consumedSecond {
				consumedSecond = true
				return csvScannerInfo{handle: &handleTwo}, nil
			}
			return csvScannerInfo{}, io.EOF
		},
		func(ctx context.Context) {},
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		require.NoError(t, f.Close(ctx))
		done()
	})

	require.NoError(t, f.Connect(context.Background()))

	for i, exp := range []string{
		`{"header1":"foo1","header2":"foo2","header3":"foo3"}`,
		`{"header1":"bar1","header2":"bar2","header3":"bar3"}`,
		`{"header1":"baz1","header2":"baz2","header3":"baz3"}`,
		`{"header4":"foo1","header5":"foo2","header6":"foo3"}`,
		`{"header4":"bar1","header5":"bar2","header6":"bar3"}`,
		`{"header4":"baz1","header5":"baz2","header6":"baz3"}`,
	} {
		var resMsg service.MessageBatch
		var ackFn service.AckFunc
		resMsg, ackFn, err = f.ReadBatch(context.Background())
		if err == service.ErrNotConnected {
			require.NoError(t, f.Connect(context.Background()))
			resMsg, ackFn, err = f.ReadBatch(context.Background())
		}
		require.NoError(t, err, i)

		mBytes, err := resMsg[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, exp, string(mBytes), i)
		_ = ackFn(context.Background(), nil)
	}

	_, _, err = f.ReadBatch(context.Background())
	assert.Equal(t, service.ErrNotConnected, err)

	err = f.Connect(context.Background())
	assert.Equal(t, service.ErrEndOfInput, err)
}

func TestCSVReaderCustomComma(t *testing.T) {
	var handle bytes.Buffer

	for _, msg := range []string{
		"header1|header2|header3",
		"foo1|foo2|foo3",
		"bar1|bar2|bar3",
		"baz1|baz2|baz3",
	} {
		handle.WriteString(msg)
		handle.WriteString("\n")
	}

	ctored := false
	f, err := newCSVReader(
		func(ctx context.Context) (csvScannerInfo, error) {
			if ctored {
				return csvScannerInfo{}, io.EOF
			}
			ctored = true
			return csvScannerInfo{handle: &handle}, nil
		},
		func(ctx context.Context) {},
		optCSVSetComma('|'),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		require.NoError(t, f.Close(ctx))
		done()
	})

	require.NoError(t, f.Connect(context.Background()))

	for _, exp := range []string{
		`{"header1":"foo1","header2":"foo2","header3":"foo3"}`,
		`{"header1":"bar1","header2":"bar2","header3":"bar3"}`,
		`{"header1":"baz1","header2":"baz2","header3":"baz3"}`,
	} {
		var resMsg service.MessageBatch
		resMsg, _, err = f.ReadBatch(context.Background())
		require.NoError(t, err)

		mBytes, err := resMsg[0].AsBytes()
		require.NoError(t, err)

		assert.Equal(t, exp, string(mBytes))
	}

	_, _, err = f.ReadBatch(context.Background())
	assert.Equal(t, service.ErrNotConnected, err)

	err = f.Connect(context.Background())
	assert.Equal(t, service.ErrEndOfInput, err)
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
		handle.WriteString(msg)
		handle.WriteString("\n")
	}

	ctored := false
	f, err := newCSVReader(
		func(ctx context.Context) (csvScannerInfo, error) {
			if ctored {
				return csvScannerInfo{}, io.EOF
			}
			ctored = true
			return csvScannerInfo{handle: &handle}, nil
		},
		func(ctx context.Context) {},
		optCSVSetStrict(false),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		require.NoError(t, f.Close(ctx))
		done()
	})

	require.NoError(t, f.Connect(context.Background()))

	for _, exp := range []string{
		`{"header1":"foo1","header2":"foo2","header3":"foo3"}`,
		`["bar1","bar2","bar3","bar4"]`,
		`{"header1":"baz1","header2":"baz2","header3":"baz3"}`,
		`{"header1":"buz1","header2":"buz2"}`,
	} {
		var resMsg service.MessageBatch
		resMsg, _, err = f.ReadBatch(context.Background())
		require.NoError(t, err)

		mBytes, err := resMsg[0].AsBytes()
		require.NoError(t, err)

		assert.Equal(t, exp, string(mBytes))
	}

	_, _, err = f.ReadBatch(context.Background())
	assert.Equal(t, service.ErrNotConnected, err)

	err = f.Connect(context.Background())
	assert.Equal(t, service.ErrEndOfInput, err)
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
		handle.WriteString(msg)
		handle.WriteString("\n")
	}

	ctored := false
	f, err := newCSVReader(
		func(ctx context.Context) (csvScannerInfo, error) {
			if ctored {
				return csvScannerInfo{}, io.EOF
			}
			ctored = true
			return csvScannerInfo{handle: &handle}, nil
		},
		func(ctx context.Context) {},
		optCSVSetStrict(true),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		require.NoError(t, f.Close(ctx))
		done()
	})

	require.NoError(t, f.Connect(context.Background()))

	for _, exp := range []any{
		`{"header1":"foo1","header2":"foo2","header3":"foo3"}`,
		errors.New("record on line 3: wrong number of fields"),
		`{"header1":"baz1","header2":"baz2","header3":"baz3"}`,
		errors.New("record on line 5: wrong number of fields"),
	} {
		var resMsg service.MessageBatch
		resMsg, _, err = f.ReadBatch(context.Background())

		switch expT := exp.(type) {
		case string:
			require.NoError(t, err)

			mBytes, err := resMsg[0].AsBytes()
			require.NoError(t, err)

			assert.Equal(t, expT, string(mBytes))

		case error:
			assert.EqualError(t, err, expT.Error())
		}
	}

	_, _, err = f.ReadBatch(context.Background())
	assert.Equal(t, service.ErrNotConnected, err)

	err = f.Connect(context.Background())
	assert.Equal(t, service.ErrEndOfInput, err)
}

func TestCSVReaderLazyQuotes(t *testing.T) {
	tests := []struct {
		name        string
		lazyQuotes  bool
		input       string
		expected    string
		errContains string
	}{
		{
			name:       "quotes in unquoted field w/ LazyQuotes = true",
			input:      `f"oo"1,f"oo"2,f"oo"3`,
			expected:   `["f\"oo\"1","f\"oo\"2","f\"oo\"3"]`,
			lazyQuotes: true,
		},
		{
			name:        "quotes in unquoted field w/ LazyQuotes = false",
			input:       `f"oo"1,f"oo"2,f"oo"3`,
			errContains: `bare " in non-quoted-field`,
			lazyQuotes:  false,
		},
		{
			name:       "non-doubled quote in quoted field w/ LazyQuotes = true",
			input:      `"f"oo1","f"oo2","f"oo3"`,
			expected:   `["f\"oo1","f\"oo2","f\"oo3"]`,
			lazyQuotes: true,
		},
		{
			name:        "non-doubled quote in quoted field w/ LazyQuotes = false",
			input:       `f"oo1,"f'oo'2","f'oo'3"`,
			errContains: `bare " in non-quoted-field`,
			lazyQuotes:  false,
		},
		{
			name:       "quotes in unquoted field AND non-doubled quote in quoted field w/ LazyQuotes = true",
			input:      `f"oo"1,"f"oo2",f"oo"3`,
			expected:   `[\"f"oo"1\","f"oo2",\"f"oo"3\"]`,
			lazyQuotes: true,
		},
		{
			name:        "quotes in unquoted field AND non-doubled quote in quoted field w/ LazyQuotes = false",
			input:       `f"oo"1,"f"oo2",f"oo"3`,
			errContains: `bare " in non-quoted-field`,
			lazyQuotes:  false,
		},
	}
	for _, test := range tests {
		var handle bytes.Buffer

		handle.WriteString(test.input)

		f, err := newCSVReader(
			func(ctx context.Context) (csvScannerInfo, error) {
				return csvScannerInfo{handle: &handle}, nil
			},
			func(ctx context.Context) {},
			optCSVSetExpectHeader(false),
			optCSVSetLazyQuotes(test.lazyQuotes),
		)
		require.NoError(t, err, test.name)
		t.Cleanup(func() {
			ctx, done := context.WithTimeout(context.Background(), time.Second*30)
			require.NoError(t, f.Close(ctx))
			done()
		})

		require.NoError(t, f.Connect(context.Background()), test.name)

		resMsg, _, err := f.ReadBatch(context.Background())
		if test.errContains != "" {
			require.Contains(t, err.Error(), test.errContains, test.name)
			return
		}
		require.NoError(t, err, test.name)

		mBytes, err := resMsg[0].AsBytes()
		require.NoError(t, err)

		assert.Equal(t, test.expected, string(mBytes), test.name)
	}
}
