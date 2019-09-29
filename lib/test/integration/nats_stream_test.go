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
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/nats-io/stan.go"
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
	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()
	resource.Expire(900)

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

	t.Run("TestNATSStreamStreamsALO", func(te *testing.T) {
		testNATSStreamStreamsALO(url, te)
	})
	t.Run("TestNATSStreamStreamsALOAsync", func(te *testing.T) {
		testNATSStreamStreamsALOAsync(url, te)
	})
	t.Run("TestNATSStreamSinglePart", func(te *testing.T) {
		testNATSStreamSinglePart(url, te)
	})
	t.Run("TestNATSStreamResumeDurable", func(te *testing.T) {
		testNATSStreamResumeDurable(url, te)
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

func testNATSStreamStreamsALO(url string, t *testing.T) {
	subject := "benthos_test_streams_alo"

	inConf := reader.NewNATSStreamConfig()
	inConf.ClientID = "benthos_test_streams_alo"
	inConf.URLs = []string{url}
	inConf.Subject = subject

	outConf := writer.NewNATSStreamConfig()
	outConf.URLs = []string{url}
	outConf.Subject = subject

	outputCtr := func() (mOutput writer.Type, err error) {
		if mOutput, err = writer.NewNATSStream(outConf, log.Noop(), metrics.Noop()); err != nil {
			return
		}
		err = mOutput.Connect()
		return
	}
	inputCtr := func() (mInput reader.Type, err error) {
		if mInput, err = reader.NewNATSStream(inConf, log.Noop(), metrics.Noop()); err != nil {
			return
		}
		err = mInput.Connect()
		return
	}

	checkALOSynchronous(outputCtr, inputCtr, t)

	inConf.Subject = "benthos_test_streams_alo_with_dc"
	outConf.Subject = "benthos_test_streams_alo_with_dc"

	checkALOSynchronousAndDie(outputCtr, inputCtr, t)
}

func testNATSStreamStreamsALOAsync(url string, t *testing.T) {
	subject := "benthos_test_streams_alo_async"

	inConf := reader.NewNATSStreamConfig()
	inConf.ClientID = "benthos_test_streams_alo_async"
	inConf.URLs = []string{url}
	inConf.Subject = subject

	outConf := writer.NewNATSStreamConfig()
	outConf.URLs = []string{url}
	outConf.Subject = subject

	outputCtr := func() (mOutput writer.Type, err error) {
		if mOutput, err = writer.NewNATSStream(outConf, log.Noop(), metrics.Noop()); err != nil {
			return
		}
		err = mOutput.Connect()
		return
	}
	inputCtr := func() (mInput reader.Async, err error) {
		ctx, done := context.WithTimeout(context.Background(), time.Second*10)
		defer done()

		if mInput, err = reader.NewNATSStream(inConf, log.Noop(), metrics.Noop()); err != nil {
			return
		}
		err = mInput.ConnectWithContext(ctx)
		return
	}

	checkALOSynchronousAsync(outputCtr, inputCtr, t)

	inConf.Subject = "benthos_test_streams_alo_with_dc_async"
	outConf.Subject = "benthos_test_streams_alo_with_dc_async"

	checkALOSynchronousAndDieAsync(outputCtr, inputCtr, t)

	inConf.Subject = "benthos_test_streams_parallel_async"
	outConf.Subject = "benthos_test_streams_parallel_async"

	checkALOParallelAsync(outputCtr, inputCtr, 100, t)
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

func testNATSStreamResumeDurable(url string, t *testing.T) {
	subject := "benthos_test_resume_durable"

	inConf := reader.NewNATSStreamConfig()
	inConf.ClientID = "benthos_test_durable_client"
	inConf.URLs = []string{url}
	inConf.Subject = subject
	inConf.UnsubOnClose = false

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

	testMsgs := map[string]struct{}{}
	N := 50
	i := 0
	for ; i < (N / 2); i++ {
		str := fmt.Sprintf("hello world: %v", i)
		testMsgs[str] = struct{}{}
		msg := message.New([][]byte{
			[]byte(str),
		})
		msg.Get(0).Metadata().Set("foo", "bar")
		msg.Get(0).Metadata().Set("root_foo", "bar2")
		if gerr := mOutput.Write(msg); gerr != nil {
			t.Fatal(gerr)
		}
	}

	for len(testMsgs) > 0 {
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
	}

	mInput.CloseAsync()
	if cErr := mInput.WaitForClose(time.Second); cErr != nil {
		t.Error(cErr)
	}

	for ; i < N; i++ {
		str := fmt.Sprintf("hello world: %v", i+N)
		testMsgs[str] = struct{}{}
		msg := message.New([][]byte{
			[]byte(str),
		})
		msg.Get(0).Metadata().Set("foo", "bar")
		msg.Get(0).Metadata().Set("root_foo", "bar2")
		if gerr := mOutput.Write(msg); gerr != nil {
			t.Fatal(gerr)
		}
	}

	if mInput, err = reader.NewNATSStream(inConf, log.Noop(), metrics.Noop()); err != nil {
		t.Fatal(err)
	}
	if err = mInput.Connect(); err != nil {
		t.Fatal(err)
	}

	for len(testMsgs) > 1 {
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
	}
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
