// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package integration

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/input/reader"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/message"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/output/writer"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/nats-io/go-nats-streaming"
	"github.com/ory/dockertest"
)

func TestNATSStreamIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("nats-streaming", "latest", nil)
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	url := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("4222/tcp"))
	if err = pool.Retry(func() error {
		natsConn, err := stan.Connect("test-cluster", "benthos_test_client", stan.NatsURL(url))
		if err != nil {
			return err
		}
		natsConn.Close()
		return nil
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	t.Run("TestNATSStreamSinglePart", func(te *testing.T) {
		testNATSStreamSinglePart(url, te)
	})
	t.Run("TestNATSStreamMultiplePart", func(te *testing.T) {
		testNATSStreamMultiplePart(url, te)
	})
	t.Run("TestNATSStreamDisconnect", func(te *testing.T) {
		testNATSStreamDisconnect(url, te)
	})
}

func createNATSStreamInputOutput(
	inConf reader.NATSStreamConfig, outConf writer.NATSStreamConfig,
) (mInput reader.Type, mOutput writer.Type, err error) {
	if mInput, err = reader.NewNATSStream(inConf, log.Noop(), metrics.Noop()); err != nil {
		return
	}
	if err = mInput.Connect(); err != nil {
		return
	}
	if mOutput, err = writer.NewNATSStream(outConf, log.Noop(), metrics.Noop()); err != nil {
		return
	}
	if err = mOutput.Connect(); err != nil {
		return
	}
	return
}

func testNATSStreamSinglePart(url string, t *testing.T) {
	subject := "benthos_test_single"

	inConf := reader.NewNATSStreamConfig()
	inConf.ClientID = "benthos_test_single_client"
	inConf.URLs = []string{url}
	inConf.Subject = subject

	outConf := writer.NewNATSStreamConfig()
	outConf.URLs = []string{url}
	outConf.Subject = subject

	mInput, mOutput, err := createNATSStreamInputOutput(inConf, outConf)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		mInput.CloseAsync()
		if cErr := mInput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
		mOutput.CloseAsync()
		if cErr := mOutput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	N := 10

	wg := sync.WaitGroup{}
	wg.Add(N)

	testMsgs := map[string]struct{}{}
	for i := 0; i < N; i++ {
		str := fmt.Sprintf("hello world: %v", i)
		testMsgs[str] = struct{}{}
		go func(testStr string) {
			msg := message.New([][]byte{
				[]byte(testStr),
			})
			msg.Get(0).Metadata().Set("foo", "bar")
			msg.Get(0).Metadata().Set("root_foo", "bar2")
			if gerr := mOutput.Write(msg); gerr != nil {
				t.Fatal(gerr)
			}
			wg.Done()
		}(str)
	}

	lMsgs := len(testMsgs)
	for lMsgs > 0 {
		var actM types.Message
		actM, err = mInput.Read()
		if err != nil {
			t.Error(err)
		} else {
			act := string(actM.Get(0).Get())
			if _, exists := testMsgs[act]; !exists {
				t.Errorf("Unexpected message: %v", act)
			}
			delete(testMsgs, act)
		}
		if err = mInput.Acknowledge(nil); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}

	wg.Wait()
}

func testNATSStreamMultiplePart(url string, t *testing.T) {
	subject := "benthos_test_multi"

	inConf := reader.NewNATSStreamConfig()
	inConf.ClientID = "benthos_test_multi_client"
	inConf.URLs = []string{url}
	inConf.Subject = subject

	outConf := writer.NewNATSStreamConfig()
	outConf.URLs = []string{url}
	outConf.Subject = subject

	mInput, mOutput, err := createNATSStreamInputOutput(inConf, outConf)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		mInput.CloseAsync()
		if cErr := mInput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
		mOutput.CloseAsync()
		if cErr := mOutput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	N := 10

	wg := sync.WaitGroup{}
	wg.Add(N)

	testMsgs := map[string]struct{}{}
	for i := 0; i < N; i++ {
		str1 := fmt.Sprintf("hello world: %v part 1", i)
		str2 := fmt.Sprintf("hello world: %v part 2", i)
		str3 := fmt.Sprintf("hello world: %v part 3", i)
		testMsgs[str1] = struct{}{}
		testMsgs[str2] = struct{}{}
		testMsgs[str3] = struct{}{}
		go func(testStr1, testStr2, testStr3 string) {
			msg := message.New([][]byte{
				[]byte(testStr1),
				[]byte(testStr2),
				[]byte(testStr3),
			})
			msg.Get(0).Metadata().Set("foo", "bar")
			msg.Get(1).Metadata().Set("root_foo", "bar2")
			if gerr := mOutput.Write(msg); gerr != nil {
				t.Fatal(gerr)
			}
			wg.Done()
		}(str1, str2, str3)
	}

	lMsgs := len(testMsgs)
	for lMsgs > 0 {
		var actM types.Message
		actM, err = mInput.Read()
		if err != nil {
			t.Error(err)
		} else {
			act := string(actM.Get(0).Get())
			if _, exists := testMsgs[act]; !exists {
				t.Errorf("Unexpected message: %v", act)
			}
			delete(testMsgs, act)
		}
		if err = mInput.Acknowledge(nil); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}

	wg.Wait()
}

func testNATSStreamDisconnect(url string, t *testing.T) {
	subject := "benthos_test_disconnect"

	inConf := reader.NewNATSStreamConfig()
	inConf.ClientID = "benthos_test_disconn_client"
	inConf.URLs = []string{url}
	inConf.Subject = subject

	outConf := writer.NewNATSStreamConfig()
	outConf.URLs = []string{url}
	outConf.Subject = subject

	mInput, mOutput, err := createNATSStreamInputOutput(inConf, outConf)
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		mInput.CloseAsync()
		if cErr := mInput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
		mOutput.CloseAsync()
		if cErr := mOutput.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
		wg.Done()
	}()

	if _, err = mInput.Read(); err != types.ErrTypeClosed && err != types.ErrNotConnected {
		t.Errorf("Wrong error: %v != %v", err, types.ErrTypeClosed)
	}

	wg.Wait()
}
