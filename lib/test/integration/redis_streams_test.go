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
	"github.com/ory/dockertest"
)

func TestRedisStreamsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("redis", "latest", nil)
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}
	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()
	resource.Expire(900)

	url := fmt.Sprintf("tcp://localhost:%v", resource.GetPort("6379/tcp"))

	if err = pool.Retry(func() error {
		conf := writer.NewRedisStreamsConfig()
		conf.URL = url

		r, cErr := writer.NewRedisStreams(conf, log.Noop(), metrics.Noop())
		if cErr != nil {
			return cErr
		}
		cErr = r.Connect()

		r.CloseAsync()
		return cErr
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	t.Run("TestRedisStreamsSinglePart", func(te *testing.T) {
		testRedisStreamsSinglePart(url, te)
	})
	t.Run("TestRedisStreamsALO", func(te *testing.T) {
		testRedisStreamsALO(url, te)
	})
	t.Run("TestRedisStreamsAsyncALO", func(te *testing.T) {
		testRedisStreamsAsyncALO(url, te)
	})
	t.Run("TestRedisStreamsMultiplePart", func(te *testing.T) {
		testRedisStreamsMultiplePart(url, te)
	})
	t.Run("TestRedisStreamsDisconnect", func(te *testing.T) {
		testRedisStreamsDisconnect(url, te)
	})
}

func createRedisStreamsInputOutput(
	inConf reader.RedisStreamsConfig, outConf writer.RedisStreamsConfig,
) (mInput reader.Type, mOutput writer.Type, err error) {
	if mOutput, err = writer.NewRedisStreams(outConf, log.Noop(), metrics.Noop()); err != nil {
		return
	}
	if err = mOutput.Connect(); err != nil {
		return
	}
	if err = mOutput.Write(message.New([][]byte{[]byte(`IGNORE ME`)})); err != nil {
		return
	}
	if mInput, err = reader.NewRedisStreams(inConf, log.Noop(), metrics.Noop()); err != nil {
		return
	}
	if err = mInput.Connect(); err != nil {
		return
	}
	return
}

func testRedisStreamsALO(url string, t *testing.T) {
	inConf := reader.NewRedisStreamsConfig()
	inConf.URL = url
	inConf.Streams = []string{"benthos_test_streams_alo"}
	inConf.StartFromOldest = false

	outConf := writer.NewRedisStreamsConfig()
	outConf.URL = url
	outConf.Stream = "benthos_test_streams_alo"

	outputCtr := func() (writer.Type, error) {
		mOutput, err := writer.NewRedisStreams(outConf, log.Noop(), metrics.Noop())
		if err != nil {
			return nil, err
		}
		if err = mOutput.Connect(); err != nil {
			return nil, err
		}
		if err = mOutput.Write(message.New([][]byte{[]byte(`IGNORE ME`)})); err != nil {
			return nil, err
		}
		return mOutput, nil
	}
	inputCtr := func() (reader.Type, error) {
		var err error
		var mInput reader.Type
		if mInput, err = reader.NewRedisStreams(inConf, log.Noop(), metrics.Noop()); err != nil {
			return nil, err
		}
		mInput = reader.NewPreserver(mInput)
		if err = mInput.Connect(); err != nil {
			return nil, err
		}
		return mInput, nil
	}

	checkALOSynchronous(outputCtr, inputCtr, t)

	inConf.Streams = []string{"benthos_test_streams_alo_with_dc"}
	outConf.Stream = "benthos_test_streams_alo_with_dc"

	checkALOSynchronousAndDie(outputCtr, inputCtr, t)
}

func testRedisStreamsAsyncALO(url string, t *testing.T) {
	inConf := reader.NewRedisStreamsConfig()
	inConf.URL = url
	inConf.Streams = []string{"benthos_test_streams_alo_async"}
	inConf.StartFromOldest = false

	outConf := writer.NewRedisStreamsConfig()
	outConf.URL = url
	outConf.Stream = "benthos_test_streams_alo_async"

	outputCtr := func() (writer.Type, error) {
		mOutput, err := writer.NewRedisStreams(outConf, log.Noop(), metrics.Noop())
		if err != nil {
			return nil, err
		}
		if err = mOutput.Connect(); err != nil {
			return nil, err
		}
		if err = mOutput.Write(message.New([][]byte{[]byte(`IGNORE ME`)})); err != nil {
			return nil, err
		}
		return mOutput, nil
	}
	inputCtr := func() (reader.Async, error) {
		ctx, done := context.WithTimeout(context.Background(), time.Second)
		defer done()

		var err error
		var mInput reader.Async
		if mInput, err = reader.NewRedisStreams(inConf, log.Noop(), metrics.Noop()); err != nil {
			return nil, err
		}
		mInput = reader.NewAsyncPreserver(mInput)
		if err = mInput.ConnectWithContext(ctx); err != nil {
			return nil, err
		}
		return mInput, nil
	}

	checkALOSynchronousAsync(outputCtr, inputCtr, t)

	inConf.Streams = []string{"benthos_test_streams_alo_with_dc_async"}
	outConf.Stream = "benthos_test_streams_alo_with_dc_async"

	checkALOSynchronousAndDieAsync(outputCtr, inputCtr, t)

	inConf.Streams = []string{"benthos_test_streams_parallel_async"}
	outConf.Stream = "benthos_test_streams_parallel_async"

	checkALOParallelAsync(outputCtr, inputCtr, 100, t)
}

func testRedisStreamsSinglePart(url string, t *testing.T) {
	inConf := reader.NewRedisStreamsConfig()
	inConf.URL = url
	inConf.Streams = []string{"benthos_test_streams_single_part"}
	inConf.StartFromOldest = false

	outConf := writer.NewRedisStreamsConfig()
	outConf.URL = url
	outConf.Stream = "benthos_test_streams_single_part"

	mInput, mOutput, err := createRedisStreamsInputOutput(inConf, outConf)
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
			actM.Iter(func(i int, part types.Part) error {
				act := string(part.Get())
				if _, exists := testMsgs[act]; !exists {
					t.Errorf("Unexpected message: %v", act)
				}
				delete(testMsgs, act)
				return nil
			})
		}
		if err = mInput.Acknowledge(nil); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}

	wg.Wait()
}

func testRedisStreamsMultiplePart(url string, t *testing.T) {
	inConf := reader.NewRedisStreamsConfig()
	inConf.URL = url
	inConf.Streams = []string{"benthos_test_streams_multiple_part"}
	inConf.StartFromOldest = false

	outConf := writer.NewRedisStreamsConfig()
	outConf.URL = url
	outConf.Stream = "benthos_test_streams_multiple_part"

	mInput, mOutput, err := createRedisStreamsInputOutput(inConf, outConf)
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
			actM.Iter(func(i int, part types.Part) error {
				act := string(part.Get())
				if _, exists := testMsgs[act]; !exists {
					t.Errorf("Unexpected message: %v", act)
				}
				delete(testMsgs, act)
				return nil
			})
		}
		if err = mInput.Acknowledge(nil); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}

	wg.Wait()
}

func testRedisStreamsDisconnect(url string, t *testing.T) {
	inConf := reader.NewRedisStreamsConfig()
	inConf.URL = url
	inConf.Streams = []string{"benthos_test_streams_disconnect"}

	outConf := writer.NewRedisStreamsConfig()
	outConf.URL = url
	outConf.Stream = "benthos_test_streams_disconnect"

	mInput, mOutput, err := createRedisStreamsInputOutput(inConf, outConf)
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
