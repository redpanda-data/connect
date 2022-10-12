package io_test

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestFileDirectory(t *testing.T) {
	tmpDir := t.TempDir()

	tmpInnerDir, err := os.MkdirTemp(tmpDir, "benthos_inner")
	require.NoError(t, err)

	tmpFile, err := os.CreateTemp(tmpDir, "f1*.txt")
	require.NoError(t, err)

	_, err = tmpFile.WriteString("foo")
	require.NoError(t, err)

	err = tmpFile.Close()
	require.NoError(t, err)

	err = os.Chtimes(tmpFile.Name(), mockTime(), mockTime())
	require.NoError(t, err)

	tmpFileTwo, err := os.CreateTemp(tmpInnerDir, "f2*.txt")
	require.NoError(t, err)

	_, err = tmpFileTwo.WriteString("bar")
	require.NoError(t, err)

	err = tmpFileTwo.Close()
	require.NoError(t, err)

	err = os.Chtimes(tmpFileTwo.Name(), mockTime(), mockTime())
	require.NoError(t, err)

	expFiles := map[string]*os.File{
		"foo": tmpFile,
		"bar": tmpFileTwo,
	}
	exp := map[string]struct{}{
		"foo": {},
		"bar": {},
	}
	act := map[string]struct{}{}

	conf := input.NewConfig()
	conf.Type = "file"
	conf.File.Paths = []string{
		fmt.Sprintf("%v/*.txt", tmpDir),
		fmt.Sprintf("%v/**/*.txt", tmpDir),
	}
	conf.File.Codec = "all-bytes"

	i, err := mock.NewManager().NewInput(conf)
	require.NoError(t, err)

	for range exp {
		var tran message.Transaction
		var open bool
		select {
		case tran, open = <-i.TransactionChan():
			assert.True(t, open)
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		res := tran.Payload.Get(0)
		resStr := string(res.AsBytes())
		if _, exists := act[resStr]; exists {
			t.Errorf("Received duplicate message: %v", resStr)
		}
		assertValidMetaData(t, res, expFiles[resStr])
		act[resStr] = struct{}{}

		require.NoError(t, tran.Ack(context.Background(), nil))
	}

	var open bool
	select {
	case _, open = <-i.TransactionChan():
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	assert.False(t, open)

	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func assertValidMetaData(t *testing.T, res *message.Part, tmpFile *os.File) {
	assert.Equal(t, tmpFile.Name(), res.MetaGetStr("path"))
	assert.Equal(t, mockTime().Format(time.RFC3339), res.MetaGetStr("mod_time"))
	assert.Equal(t, strconv.Itoa(int(mockTime().Unix())), res.MetaGetStr("mod_time_unix"))
}

func mockTime() time.Time {
	return time.Date(2015, 8, 25, 23, 23, 0, 0, time.UTC)
}
