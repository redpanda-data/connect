package input

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFiles(t *testing.T, dir string, nameToContent map[string]string) {
	t.Helper()

	for k, v := range nameToContent {
		require.NoError(t, ioutil.WriteFile(filepath.Join(dir, k), []byte(v), 777))
	}
}

func TestSequenceHappy(t *testing.T) {
	t.Parallel()

	tmpDir, err := ioutil.TempDir("", "benthos_sequence_input_test")
	require.NoError(t, err)

	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	files := map[string]string{
		"f1": "foo\nbar\nbaz",
		"f2": "buz\nbev\nbif\n",
		"f3": "qux\nquz\nqev",
	}

	writeFiles(t, tmpDir, files)

	conf := NewConfig()
	conf.Type = TypeSequence

	for _, k := range []string{"f1", "f2", "f3"} {
		inConf := NewConfig()
		inConf.Type = TypeFile
		inConf.File.Path = filepath.Join(tmpDir, k)
		conf.Sequence.Inputs = append(conf.Sequence.Inputs, inConf)
	}

	rdr, err := New(conf, types.NoopMgr(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	exp, act := []string{
		"foo", "bar", "baz", "buz", "bev", "bif", "qux", "quz", "qev",
	}, []string{}

consumeLoop:
	for {
		select {
		case tran, open := <-rdr.TransactionChan():
			if !open {
				break consumeLoop
			}
			assert.Equal(t, 1, tran.Payload.Len())
			act = append(act, string(tran.Payload.Get(0).Get()))
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				t.Fatalf("failed to ack after: %v", act)
			}
		case <-time.After(time.Second):
			t.Fatalf("Failed to consume message after: %v", act)
		}
	}

	assert.Equal(t, exp, act)

	rdr.CloseAsync()
	assert.NoError(t, rdr.WaitForClose(time.Second))
}

func TestSequenceSad(t *testing.T) {
	t.Parallel()

	tmpDir, err := ioutil.TempDir("", "benthos_sequence_input_test")
	require.NoError(t, err)

	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	files := map[string]string{
		"f1": "foo\nbar\nbaz",
	}

	writeFiles(t, tmpDir, files)

	conf := NewConfig()
	conf.Type = TypeSequence

	for _, k := range []string{"f1", "f2", "f3"} {
		inConf := NewConfig()
		inConf.Type = TypeFile
		inConf.File.Path = filepath.Join(tmpDir, k)
		conf.Sequence.Inputs = append(conf.Sequence.Inputs, inConf)
	}

	rdr, err := New(conf, types.NoopMgr(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	exp := []string{
		"foo", "bar", "baz",
	}

	for i, str := range exp {
		select {
		case tran, open := <-rdr.TransactionChan():
			if !open {
				t.Fatal("closed earlier than expected")
			}
			assert.Equal(t, 1, tran.Payload.Len())
			assert.Equal(t, str, string(tran.Payload.Get(0).Get()))
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				t.Fatalf("failed to ack after: %v", str)
			}
		case <-time.After(time.Second):
			t.Fatalf("Failed to consume message %v", i)
		}
	}

	select {
	case <-rdr.TransactionChan():
		t.Fatal("unexpected transaction")
	case <-time.After(100 * time.Millisecond):
	}

	exp = []string{
		"buz", "bev", "bif",
	}

	writeFiles(t, tmpDir, map[string]string{
		"f2": "buz\nbev\nbif\n",
	})

	for i, str := range exp {
		select {
		case tran, open := <-rdr.TransactionChan():
			if !open {
				t.Fatal("closed earlier than expected")
			}
			assert.Equal(t, 1, tran.Payload.Len())
			assert.Equal(t, str, string(tran.Payload.Get(0).Get()))
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				t.Fatalf("failed to ack after: %v", str)
			}
		case <-time.After(time.Second):
			t.Fatalf("Failed to consume message %v", i)
		}
	}

	rdr.CloseAsync()
	assert.NoError(t, rdr.WaitForClose(time.Second))
}
