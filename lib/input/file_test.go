package input

import (
	"io/ioutil"
	"os"
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
	tmpfile, err := ioutil.TempFile("", "benthos_file_test")
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
		tmpfile.Write([]byte(msg))
		tmpfile.Write([]byte("\n"))
		tmpfile.Write([]byte("\n")) // Try some empty messages
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
	tmpfile, err := ioutil.TempFile("", "benthos_file_test")
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
			tmpfile.Write([]byte(part))
			tmpfile.Write([]byte("\n"))
		}
		tmpfile.Write([]byte("\n"))
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
