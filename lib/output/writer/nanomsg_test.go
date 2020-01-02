package writer

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"nanomsg.org/go-mangos"
	"nanomsg.org/go-mangos/protocol/pull"
	"nanomsg.org/go-mangos/transport/tcp"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

//------------------------------------------------------------------------------

func TestNanomsgBasic(t *testing.T) {
	nTestLoops := 1000

	conf := NewNanomsgConfig()
	conf.URLs = []string{"tcp://localhost:1324"}
	conf.Bind = true
	conf.PollTimeout = "100ms"
	conf.SocketType = "PUSH"

	s, err := NewNanomsg(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = s.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		s.CloseAsync()
		if err = s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	socket, err := pull.NewSocket()
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()

	socket.AddTransport(tcp.NewTransport())
	socket.SetOption(mangos.OptionRecvDeadline, time.Second)

	if err = socket.Dial("tcp://localhost:1324"); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.New([][]byte{[]byte(testStr)})

		go func() {
			if serr := s.Write(testMsg); serr != nil {
				t.Error(serr)
			}
		}()

		data, err := socket.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if res := string(data); res != testStr {
			t.Errorf("Wrong value on output: %v != %v", res, testStr)
		}
	}
}

func TestNanomsgParallelWrites(t *testing.T) {
	conf := NewNanomsgConfig()
	conf.URLs = []string{"tcp://localhost:1324"}
	conf.Bind = true
	conf.PollTimeout = "100ms"
	conf.SocketType = "PUSH"

	s, err := NewNanomsg(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = s.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		s.CloseAsync()
		if err = s.WaitForClose(time.Second); err != nil {
			t.Error(err)
		}
	}()

	socket, err := pull.NewSocket()
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()

	socket.AddTransport(tcp.NewTransport())
	socket.SetOption(mangos.OptionRecvDeadline, time.Second)

	if err = socket.Dial("tcp://localhost:1324"); err != nil {
		t.Fatal(err)
	}

	N := 100

	startChan := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(N + 1)

	testMessages := map[string]struct{}{}
	for i := 0; i < N; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMessages[testStr] = struct{}{}
		go func(str string) {
			<-startChan
			if serr := s.Write(message.New([][]byte{[]byte(str)})); serr != nil {
				t.Error(serr)
			}
			wg.Done()
		}(testStr)
	}

	go func() {
		for len(testMessages) > 0 {
			data, err := socket.Recv()
			if err != nil {
				t.Fatal(err)
			}
			resStr := string(data)
			if _, exists := testMessages[resStr]; exists {
				delete(testMessages, resStr)
			} else {
				t.Fatalf("Unexpected message received: %v", resStr)
			}
		}
		wg.Done()
	}()

	close(startChan)
	wg.Wait()
}

//------------------------------------------------------------------------------
