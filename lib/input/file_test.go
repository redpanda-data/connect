package input

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSinglePartDeprecated(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "benthos_file_test")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.Remove(tmpfile.Name())
	})

	messages := []string{
		"first message",
		"second message",
		"third message",
	}

	for _, msg := range messages {
		_, _ = tmpfile.WriteString(msg)
		_, _ = tmpfile.WriteString("\n")
		_, _ = tmpfile.WriteString("\n") // Try some empty messages
	}

	conf := NewConfig()
	conf.File.Path = tmpfile.Name()

	f, err := NewFile(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	defer func() {
		f.CloseAsync()
		assert.NoError(t, f.WaitForClose(time.Second))
	}()

	for _, exp := range messages {
		var ts types.Transaction
		var open bool
		select {
		case ts, open = <-f.TransactionChan():
			require.True(t, open)
			assert.Equal(t, exp, string(ts.Payload.Get(0).Get()))
			assert.Equal(t, tmpfile.Name(), ts.Payload.Get(0).Metadata().Get("path"))
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}
	}

	select {
	case _, open := <-f.TransactionChan():
		require.False(t, open)
	case <-time.After(time.Second):
		t.Error("Timed out waiting for channel close")
	}
}

func TestFileMultiPartDeprecated(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "benthos_file_test")
	require.NoError(t, err)

	t.Cleanup(func() {
		os.Remove(tmpfile.Name())
	})

	messages := [][]string{
		{
			"first message",
			"1",
			"2",
		},
		{
			"second message",
			"1",
			"2",
		},
		{
			"third message",
			"1",
			"2",
		},
	}

	for _, msg := range messages {
		for _, part := range msg {
			_, _ = tmpfile.WriteString(part)
			_, _ = tmpfile.WriteString("\n")
		}
		_, _ = tmpfile.WriteString("\n")
	}

	conf := NewConfig()
	conf.File.Path = tmpfile.Name()
	conf.File.Multipart = true

	f, err := NewFile(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	defer func() {
		f.CloseAsync()
		assert.NoError(t, f.WaitForClose(time.Second))
	}()

	for _, msg := range messages {
		var ts types.Transaction
		var open bool
		select {
		case ts, open = <-f.TransactionChan():
			require.True(t, open)
			for i, exp := range msg {
				assert.Equal(t, exp, string(ts.Payload.Get(i).Get()), strconv.Itoa(i))
			}
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}
	}

	select {
	case _, open := <-f.TransactionChan():
		require.False(t, open)
	case <-time.After(time.Second):
		t.Error("Timed out waiting for channel close")
	}
}

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
