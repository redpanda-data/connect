package io_test

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"

	_ "github.com/benthosdev/benthos/v4/internal/impl/io"
)

func csvInput(t testing.TB, confPattern string, args ...any) input.Streamed {
	iConf, err := testutil.InputFromYAML(fmt.Sprintf(confPattern, args...))
	require.NoError(t, err)

	i, err := mock.NewManager().NewInput(iConf)
	require.NoError(t, err)

	return i
}

func TestCSVInputGPaths(t *testing.T) {
	dir := t.TempDir()

	dummyFileA := filepath.Join(dir, "a.csv")
	dummyFileB := filepath.Join(dir, "b.csv")
	require.NoError(t, os.WriteFile(dummyFileA, []byte(`header1,header2,header3
foo1,bar1,baz1
foo2,bar2,baz2
foo3,bar3,baz3
`), 0o777))
	require.NoError(t, os.WriteFile(dummyFileB, []byte(`header4,header5,header6
foo4,bar4,baz4
foo5,bar5,baz5
foo6,bar6,baz6
`), 0o777))

	f := csvInput(t, `
csv:
  paths: [ "%v", "%v" ]
  delete_on_finish: false
`, dummyFileA, dummyFileB)

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second)
		require.NoError(t, f.WaitForClose(ctx))
		done()
	})

	for _, exp := range []string{
		`{"header1":"foo1","header2":"bar1","header3":"baz1"}`,
		`{"header1":"foo2","header2":"bar2","header3":"baz2"}`,
		`{"header1":"foo3","header2":"bar3","header3":"baz3"}`,
		`{"header4":"foo4","header5":"bar4","header6":"baz4"}`,
		`{"header4":"foo5","header5":"bar5","header6":"baz5"}`,
		`{"header4":"foo6","header5":"bar6","header6":"baz6"}`,
	} {
		m := readMsg(t, f.TransactionChan())
		assert.Equal(t, exp, string(m.Get(0).AsBytes()))
	}

	_, err := os.Stat(dummyFileA)
	require.NoError(t, err)

	_, err = os.Stat(dummyFileB)
	require.NoError(t, err)
}

func TestCSVInputDeleteOnFinish(t *testing.T) {
	dummyCSVFile := filepath.Join(t.TempDir(), "dummy.csv")
	require.NoError(t, os.WriteFile(dummyCSVFile, []byte(`header1,header2,header3
foo1,bar1,baz1
`), 0o777))

	f := csvInput(t, `
csv:
  paths: [ "%v" ]
  delete_on_finish: true
`, dummyCSVFile)

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second)
		require.NoError(t, f.WaitForClose(ctx))
		done()
	})

	for _, exp := range []string{
		`{"header1":"foo1","header2":"bar1","header3":"baz1"}`,
	} {
		m := readMsg(t, f.TransactionChan())
		assert.Equal(t, exp, string(m.Get(0).AsBytes()))
	}

	// Make sure the input shut down after reading the file
	select {
	case _, ok := <-f.TransactionChan():
		require.False(t, ok)
	case <-time.After(time.Second * 2):
		require.FailNow(t, "failed to read after input is closed")
	}

	_, err := os.Stat(dummyCSVFile)
	require.ErrorIs(t, err, fs.ErrNotExist)
}

func TestCSVInputGlobPaths(t *testing.T) {
	dir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.csv"), []byte(`header1,header2,header3
foo1,bar1,baz1
foo2,bar2,baz2
foo3,bar3,baz3
`), 0o777))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.csv"), []byte(`header4,header5,header6
foo4,bar4,baz4
foo5,bar5,baz5
foo6,bar6,baz6
`), 0o777))

	f := csvInput(t, `
csv:
  paths: [ "%v/*.csv" ]
`, dir)

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second)
		require.NoError(t, f.WaitForClose(ctx))
		done()
	})

	for _, exp := range []string{
		`{"header1":"foo1","header2":"bar1","header3":"baz1"}`,
		`{"header1":"foo2","header2":"bar2","header3":"baz2"}`,
		`{"header1":"foo3","header2":"bar3","header3":"baz3"}`,
		`{"header4":"foo4","header5":"bar4","header6":"baz4"}`,
		`{"header4":"foo5","header5":"bar5","header6":"baz5"}`,
		`{"header4":"foo6","header5":"bar6","header6":"baz6"}`,
	} {
		m := readMsg(t, f.TransactionChan())
		assert.Equal(t, exp, string(m.Get(0).AsBytes()))
	}
}
