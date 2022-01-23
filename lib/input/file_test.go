package input

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileDirectory(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "benthos_file_input_test")
	require.NoError(t, err)

	tmpInnerDir, err := os.MkdirTemp(tmpDir, "benthos_inner")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.RemoveAll(tmpDir)
	})

	tmpFile, err := os.CreateTemp(tmpDir, "f1*.txt")
	require.NoError(t, err)

	_, err = tmpFile.WriteString("foo")
	require.NoError(t, err)

	err = tmpFile.Close()
	require.NoError(t, err)

	tmpFileTwo, err := os.CreateTemp(tmpInnerDir, "f2*.txt")
	require.NoError(t, err)

	_, err = tmpFileTwo.WriteString("bar")
	require.NoError(t, err)

	err = tmpFileTwo.Close()
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

	resStr := string(msg.Get(0).Get())
	if _, exists := act[resStr]; exists {
		t.Errorf("Received duplicate message: %v", resStr)
	}
	act[resStr] = struct{}{}
	require.NoError(t, aFn(context.Background(), response.NewAck()))

	msg, aFn, err = f.ReadWithContext(context.Background())
	require.NoError(t, err)

	resStr = string(msg.Get(0).Get())
	if _, exists := act[resStr]; exists {
		t.Errorf("Received duplicate message: %v", resStr)
	}
	act[resStr] = struct{}{}
	require.NoError(t, aFn(context.Background(), response.NewAck()))

	_, _, err = f.ReadWithContext(context.Background())
	assert.Equal(t, types.ErrTypeClosed, err)

	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}
