package input

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

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/log"
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

	exp := map[string]struct{}{
		"foo": {},
		"bar": {},
	}
	act := map[string]struct{}{}

	conf := NewFileConfig()
	conf.Paths = []string{
		fmt.Sprintf("%v/*.txt", tmpDir),
		fmt.Sprintf("%v/**/*.txt", tmpDir),
	}
	conf.Codec = "all-bytes"

	f, err := newFileConsumer(conf, log.Noop())
	require.NoError(t, err)

	err = f.ConnectWithContext(context.Background())
	require.NoError(t, err)

	msg, aFn, err := f.ReadWithContext(context.Background())
	require.NoError(t, err)

	res := msg.Get(0)
	resStr := string(res.Get())
	if _, exists := act[resStr]; exists {
		t.Errorf("Received duplicate message: %v", resStr)
	}
	assertValidMetaData(t, res, tmpFile)
	act[resStr] = struct{}{}
	require.NoError(t, aFn(context.Background(), nil))

	msg, aFn, err = f.ReadWithContext(context.Background())
	require.NoError(t, err)

	res = msg.Get(0)
	resStr = string(res.Get())
	if _, exists := act[resStr]; exists {
		t.Errorf("Received duplicate message: %v", resStr)
	}
	assertValidMetaData(t, res, tmpFileTwo)
	act[resStr] = struct{}{}
	require.NoError(t, aFn(context.Background(), nil))

	_, _, err = f.ReadWithContext(context.Background())
	assert.Equal(t, component.ErrTypeClosed, err)

	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func assertValidMetaData(t *testing.T, res *message.Part, tmpFile *os.File) {
	assert.Equal(t, tmpFile.Name(), res.MetaGet("path"))
	assert.Equal(t, mockTime().Format(time.RFC3339), res.MetaGet("mod_time"))
	assert.Equal(t, strconv.Itoa(int(mockTime().Unix())), res.MetaGet("mod_time_unix"))
}

func mockTime() time.Time {
	return time.Date(2015, 8, 25, 23, 23, 0, 0, time.UTC)
}
