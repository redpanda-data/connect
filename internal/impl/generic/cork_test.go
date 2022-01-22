package generic

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func createSignalSocket(t *testing.T) string {
	tmpDir, err := os.MkdirTemp("", "benthos_cork_test")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(tmpDir))
	})

	return filepath.Join(tmpDir, "signal.sock")
}

func testCork(t *testing.T, cork *corkInput, conn net.Conn) {
	// send a message to uncork
	bs := []byte("cork\n")
	n, err := conn.Write(bs)
	require.NoError(t, err)
	require.Equal(t, len(bs), n)
	require.Eventually(t, func() bool {
		return cork.Corked()
	}, 1*time.Second, 100*time.Millisecond, "input was not corked in time")
}

func testUncork(t *testing.T, cork *corkInput, conn net.Conn) {
	// send a message to uncork
	bs := []byte("uncork\n")
	n, err := conn.Write(bs)
	require.NoError(t, err)
	require.Equal(t, len(bs), n)
	require.Eventually(t, func() bool {
		return !cork.Corked()
	}, 1*time.Second, 100*time.Millisecond, "input was not uncorked in time")
}

func testNilRead(t *testing.T, cork *corkInput) {
	ctx := context.Background()

	batch, ack, err := cork.ReadBatch(ctx)
	require.NoError(t, err)
	require.Nil(t, batch)
	err = ack(ctx, nil)
	require.NoError(t, err)
}

// read a batch from the input and test its contents
// we assume test inputs yield batches with a single message
func testRealRead(t *testing.T, cork *corkInput, expectedValue string) {
	ctx := context.Background()

	batch, ack, err := cork.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	err = ack(ctx, nil)
	require.NoError(t, err)

	// assert message contents
	msg := batch[0]
	bs, err := msg.AsBytes()
	require.NoError(t, err)
	require.Equal(t, string(bs), expectedValue)
}

func TestCork(t *testing.T) {
	socketFile := createSignalSocket(t)
	ctx := context.Background()
	spec := newCorkConfigSpec()

	cfg, err := spec.ParseYAML(`
signal:
  socket_server:
    network: unix
    address: `+socketFile+`
    codec: lines
input:
  generate:
    mapping: root = "testing"
    count: 10
    interval: ""
`, nil)
	require.NoError(t, err)

	cork, err := newCorkFromConfig(cfg, nil)
	require.NoError(t, err)
	defer func() {
		err := cork.Close(ctx)
		require.NoError(t, err)
	}()

	err = cork.Connect(ctx)
	require.NoError(t, err)

	// ensure the input is initially corked
	testNilRead(t, cork)

	conn, err := net.Dial("unix", socketFile)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	err = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	require.NoError(t, err)

	testUncork(t, cork, conn)

	// try to read again and hopefully the input is uncorked and produces a value
	testRealRead(t, cork, "testing")
}

func TestCork_Recork(t *testing.T) {
	socketFile := createSignalSocket(t)
	ctx := context.Background()
	spec := newCorkConfigSpec()

	cfg, err := spec.ParseYAML(`
signal:
  socket_server:
    network: unix
    address: `+socketFile+`
    codec: lines
input:
  generate:
    mapping: root = "testing"
    count: 10
    interval: ""
`, nil)
	require.NoError(t, err)

	cork, err := newCorkFromConfig(cfg, nil)
	require.NoError(t, err)
	defer func() {
		err := cork.Close(ctx)
		require.NoError(t, err)
	}()

	err = cork.Connect(ctx)
	require.NoError(t, err)

	conn, err := net.Dial("unix", socketFile)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, conn.Close())
	})

	testUncork(t, cork, conn)

	testRealRead(t, cork, "testing")

	testCork(t, cork, conn)

	testNilRead(t, cork)
}

func TestCork_InitiallyUncorked(t *testing.T) {
	socketFile := createSignalSocket(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	spec := newCorkConfigSpec()

	cfg, err := spec.ParseYAML(`
initially_corked: false
signal:
  socket_server:
    network: unix
    address: `+socketFile+`
    codec: lines
input:
  generate:
    mapping: root = "testing"
    count: 10
    interval: ""
`, nil)
	require.NoError(t, err)

	cork, err := newCorkFromConfig(cfg, nil)
	require.NoError(t, err)
	defer func() {
		err := cork.Close(ctx)
		require.NoError(t, err)
	}()

	err = cork.Connect(ctx)
	require.NoError(t, err)

	testRealRead(t, cork, "testing")
}
