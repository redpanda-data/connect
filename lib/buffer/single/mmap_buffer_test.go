package single

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

func TestMmapBufferBasic(t *testing.T) {
	t.Skip("DEPRECATED")

	dir := t.TempDir()

	n := 100

	conf := NewMmapBufferConfig()
	conf.FileSize = 100000
	conf.Path = dir

	block, err := NewMmapBuffer(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}
	defer block.Close()

	for i := 0; i < n; i++ {
		if _, err := block.PushMessage(message.New(
			[][]byte{
				[]byte("hello"),
				[]byte("world"),
				[]byte("12345"),
				[]byte(fmt.Sprintf("test%v", i)),
			},
		)); err != nil {
			t.Error(err)
			return
		}
	}

	for i := 0; i < n; i++ {
		m, err := block.NextMessage()
		if err != nil {
			t.Error(err)
			return
		}
		if m.Len() != 4 {
			t.Errorf("Wrong # parts, %v != %v", m.Len(), 4)
		} else if expected, actual := fmt.Sprintf("test%v", i), string(m.Get(3).Get()); expected != actual {
			t.Errorf("Wrong order of messages, %v != %v", expected, actual)
		}
		if _, err := block.ShiftMessage(); err != nil {
			t.Error(err)
			return
		}
	}
}

func TestMmapBufferBacklogCounter(t *testing.T) {
	t.Skip("DEPRECATED")

	dir := t.TempDir()

	conf := NewMmapBufferConfig()
	conf.FileSize = 100000
	conf.Path = dir

	block, err := NewMmapBuffer(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}
	defer block.Close()

	if _, err := block.PushMessage(message.New(
		[][]byte{[]byte("1234")}, // 4 bytes + 4 bytes
	)); err != nil {
		t.Error(err)
		return
	}

	if expected, actual := 16, block.backlog(); expected != actual {
		t.Errorf("Wrong backlog count: %v != %v", expected, actual)
	}

	if _, err := block.PushMessage(message.New(
		[][]byte{
			[]byte("1234"),
			[]byte("1234"),
		}, // ( 4 bytes + 4 bytes ) * 2
	)); err != nil {
		t.Error(err)
		return
	}

	if expected, actual := 40, block.backlog(); expected != actual {
		t.Errorf("Wrong backlog count: %v != %v", expected, actual)
	}

	if _, err := block.ShiftMessage(); err != nil {
		t.Error(err)
		return
	}

	if expected, actual := 24, block.backlog(); expected != actual {
		t.Errorf("Wrong backlog count: %v != %v", expected, actual)
	}

	if _, err := block.ShiftMessage(); err != nil {
		t.Error(err)
		return
	}

	if expected, actual := 0, block.backlog(); expected != actual {
		t.Errorf("Wrong backlog count: %v != %v", expected, actual)
	}
}

func TestMmapBufferLoopingRandom(t *testing.T) {
	t.Skip("DEPRECATED")

	dir := t.TempDir()

	conf := NewMmapBufferConfig()
	conf.FileSize = 8000
	conf.Path = dir

	block, err := NewMmapBuffer(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}
	defer block.Close()

	n, iter := 50, 5

	for j := 0; j < iter; j++ {
		for i := 0; i < n; i++ {
			b := make([]byte, rand.Int()%100)
			for k := range b {
				b[k] = '0'
			}
			if _, err := block.PushMessage(message.New(
				[][]byte{
					b,
					[]byte(fmt.Sprintf("test%v", i)),
				},
			)); err != nil {
				t.Error(err)
				return
			}
		}

		for i := 0; i < n; i++ {
			m, err := block.NextMessage()
			if err != nil {
				t.Error(err)
				return
			}
			if m.Len() != 2 {
				t.Errorf("Wrong # parts, %v != %v", m.Len(), 4)
			} else if expected, actual := fmt.Sprintf("test%v", i), string(m.Get(1).Get()); expected != actual {
				t.Errorf("Wrong order of messages, %v != %v", expected, actual)
			}
			if _, err := block.ShiftMessage(); err != nil {
				t.Error(err)
				return
			}
		}
	}
}

func TestMmapBufferMultiFiles(t *testing.T) {
	t.Skip("DEPRECATED")

	dir := t.TempDir()

	n := 100

	conf := NewMmapBufferConfig()
	conf.FileSize = 1000
	conf.Path = dir

	block, err := NewMmapBuffer(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}
	defer block.Close()

	for i := 0; i < n; i++ {
		if _, err := block.PushMessage(message.New(
			[][]byte{
				[]byte("hello"),
				[]byte("world"),
				[]byte("12345"),
				[]byte(fmt.Sprintf("test%v", i)),
			},
		)); err != nil {
			t.Error(err)
			return
		}
	}

	for i := 0; i < n; i++ {
		m, err := block.NextMessage()
		if err != nil {
			t.Error(err)
			return
		}
		if m.Len() != 4 {
			t.Errorf("Wrong # parts, %v != %v", m.Len(), 4)
		} else if expected, actual := fmt.Sprintf("test%v", i), string(m.Get(3).Get()); expected != actual {
			t.Errorf("Wrong order of messages, %v != %v", expected, actual)
		}
		if _, err := block.ShiftMessage(); err != nil {
			t.Error(err)
			return
		}
	}
}

func TestMmapBufferRecoverFiles(t *testing.T) {
	t.Skip("DEPRECATED")

	dir := t.TempDir()

	n := 100

	conf := NewMmapBufferConfig()
	conf.FileSize = 1000
	conf.Path = dir

	// Write a load of data
	block, err := NewMmapBuffer(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < n; i++ {
		if _, err := block.PushMessage(message.New(
			[][]byte{
				[]byte("hello"),
				[]byte("world"),
				[]byte("12345"),
				[]byte(fmt.Sprintf("test%v", i)),
			},
		)); err != nil {
			t.Error(err)
			return
		}
	}

	// Close down any handlers we have.
	block.Close()

	// Read the data back
	block, err = NewMmapBuffer(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < n; i++ {
		m, err := block.NextMessage()
		if err != nil {
			t.Error(err)
			return
		}
		if m.Len() != 4 {
			t.Errorf("Wrong # parts, %v != %v", m.Len(), 4)
		} else if expected, actual := fmt.Sprintf("test%v", i), string(m.Get(3).Get()); expected != actual {
			t.Errorf("Wrong order of messages, %v != %v", expected, actual)
		}
		if _, err := block.ShiftMessage(); err != nil {
			t.Error(err)
			return
		}
	}

	block.Close()
}

func TestMmapBufferRejectLargeMessage(t *testing.T) {
	t.Skip("DEPRECATED")

	dir := t.TempDir()

	tMsg := message.New(make([][]byte, 1))
	tMsg.Get(0).Set([]byte("hello world this message is too long!"))

	conf := NewMmapBufferConfig()
	conf.FileSize = 10
	conf.Path = dir

	// Write a load of data
	block, err := NewMmapBuffer(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}
	_, err = block.PushMessage(tMsg)
	if exp, actual := types.ErrMessageTooLarge, err; exp != actual {
		t.Errorf("Unexpected error: %v != %v", exp, actual)
	}
}

func BenchmarkMmapBufferBasic(b *testing.B) {
	dir := b.TempDir()

	conf := NewMmapBufferConfig()
	conf.FileSize = 1000
	conf.Path = dir

	block, err := NewMmapBuffer(conf, log.Noop(), metrics.Noop())
	if err != nil {
		b.Error(err)
		return
	}
	defer block.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := block.PushMessage(message.New(
			[][]byte{
				[]byte("hello"),
				[]byte("world"),
				[]byte("12345"),
				[]byte(fmt.Sprintf("test%v", i)),
			},
		)); err != nil {
			b.Error(err)
			return
		}
	}

	for i := 0; i < b.N; i++ {
		m, err := block.NextMessage()
		if err != nil {
			b.Error(err)
			return
		}
		if m.Len() != 4 {
			b.Errorf("Wrong # parts, %v != %v", m.Len(), 4)
		} else if expected, actual := fmt.Sprintf("test%v", i), string(m.Get(3).Get()); expected != actual {
			b.Errorf("Wrong order of messages, %v != %v", expected, actual)
		}
		if _, err := block.ShiftMessage(); err != nil {
			b.Error(err)
			return
		}
	}
}
