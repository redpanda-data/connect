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
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/ory/dockertest"
	"github.com/streadway/amqp"
)

func TestAMQPIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("rabbitmq", "latest", nil)
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}
	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()
	resource.Expire(900)

	url := fmt.Sprintf("amqp://guest:guest@localhost:%v/", resource.GetPort("5672/tcp"))

	if err = pool.Retry(func() error {
		client, err := amqp.Dial(url)
		if err == nil {
			client.Close()
		}
		return err
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	t.Run("TestAMQPStreamsALO", func(te *testing.T) {
		testAMQPStreamsALO(url, te)
	})
	t.Run("TestAMQPSinglePart", func(te *testing.T) {
		testAMQPSinglePart(url, te)
	})
	t.Run("TestAMQPMultiPart", func(te *testing.T) {
		testAMQPSinglePart(url, te)
	})
	t.Run("TestAMQPDisconnect", func(te *testing.T) {
		testAMQPDisconnect(url, te)
	})
	t.Run("TestAMQP09StreamsALOAsync", func(te *testing.T) {
		testAMQP09StreamsALOAsync(url, te)
	})
	t.Run("TestAMQP09SinglePart", func(te *testing.T) {
		testAMQP09SinglePart(url, te)
	})
	t.Run("TestAMQP09MultiPart", func(te *testing.T) {
		testAMQP09SinglePart(url, te)
	})
	t.Run("TestAMQP09Disconnect", func(te *testing.T) {
		testAMQP09Disconnect(url, te)
	})
}

func createAMQPInputOutput(
	inConf reader.AMQPConfig, outConf writer.AMQPConfig,
) (mInput reader.Type, mOutput writer.Type, err error) {
	if mOutput, err = writer.NewAMQP(outConf, log.Noop(), metrics.Noop()); err != nil {
		return
	}
	if err = mOutput.Connect(); err != nil {
		return
	}
	if mInput, err = reader.NewAMQP(inConf, log.Noop(), metrics.Noop()); err != nil {
		return
	}
	if err = mInput.Connect(); err != nil {
		return
	}
	return
}

func createAMQP09InputOutput(
	inConf reader.AMQP09Config, outConf writer.AMQPConfig,
) (mInput reader.Async, mOutput writer.Type, err error) {
	if mOutput, err = writer.NewAMQP(outConf, log.Noop(), metrics.Noop()); err != nil {
		return
	}
	if err = mOutput.Connect(); err != nil {
		return
	}
	if mInput, err = reader.NewAMQP09(inConf, log.Noop(), metrics.Noop()); err != nil {
		return
	}
	if err = mInput.ConnectWithContext(context.Background()); err != nil {
		return
	}
	return
}

func testAMQPStreamsALO(url string, t *testing.T) {
	subject := "benthos_test_streams_alo"

	outConf := writer.NewAMQPConfig()
	outConf.Exchange = subject
	outConf.URL = url
	outConf.ExchangeDeclare.Enabled = true

	inConf := reader.NewAMQPConfig()
	inConf.URL = url
	inConf.Queue = subject
	inConf.QueueDeclare.Enabled = true
	inConf.BindingsDeclare = append(inConf.BindingsDeclare, reader.AMQPBindingConfig{
		Exchange:   outConf.Exchange,
		RoutingKey: outConf.BindingKey,
	})

	outputCtr := func() (mOutput writer.Type, err error) {
		if mOutput, err = writer.NewAMQP(outConf, log.Noop(), metrics.Noop()); err != nil {
			return
		}
		err = mOutput.Connect()
		return
	}
	inputCtr := func() (mInput reader.Type, err error) {
		if mInput, err = reader.NewAMQP(inConf, log.Noop(), metrics.Noop()); err != nil {
			return
		}
		err = mInput.Connect()
		return
	}

	checkALOSynchronous(outputCtr, inputCtr, t)

	subject = "benthos_test_streams_alo_with_dc"

	outConf.Exchange = subject
	inConf.Queue = subject
	inConf.BindingsDeclare = []reader.AMQPBindingConfig{
		{
			Exchange:   outConf.Exchange,
			RoutingKey: outConf.BindingKey,
		},
	}

	checkALOSynchronousAndDie(outputCtr, inputCtr, t)
}

func testAMQPSinglePart(url string, t *testing.T) {
	subject := "benthos_test_single_part"

	outConf := writer.NewAMQPConfig()
	outConf.URL = url
	outConf.Exchange = subject
	outConf.ExchangeDeclare.Enabled = true

	inConf := reader.NewAMQPConfig()
	inConf.URL = url
	inConf.Queue = subject
	inConf.QueueDeclare.Enabled = true
	inConf.BindingsDeclare = append(inConf.BindingsDeclare, reader.AMQPBindingConfig{
		Exchange:   outConf.Exchange,
		RoutingKey: outConf.BindingKey,
	})

	mInput, mOutput, err := createAMQPInputOutput(inConf, outConf)
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
			if act = actM.Get(0).Metadata().Get("foo"); act != "bar" {
				t.Errorf("Wrong metadata returned: %v != bar", act)
			}
			if act = actM.Get(0).Metadata().Get("root_foo"); act != "bar2" {
				t.Errorf("Wrong metadata returned: %v != bar2", act)
			}
		}
		if err = mInput.Acknowledge(nil); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}

	wg.Wait()
}

func testAMQPMultiplePart(url string, t *testing.T) {
	subject := "benthos_test_multi_part"

	outConf := writer.NewAMQPConfig()
	outConf.URL = url
	outConf.Exchange = subject
	outConf.ExchangeDeclare.Enabled = true

	inConf := reader.NewAMQPConfig()
	inConf.URL = url
	inConf.Queue = subject
	inConf.QueueDeclare.Enabled = true
	inConf.BindingsDeclare = append(inConf.BindingsDeclare, reader.AMQPBindingConfig{
		Exchange:   outConf.Exchange,
		RoutingKey: outConf.BindingKey,
	})

	mInput, mOutput, err := createAMQPInputOutput(inConf, outConf)
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
			if act = actM.Get(0).Metadata().Get("foo"); act != "bar" {
				t.Errorf("Wrong metadata returned: %v != bar", act)
			}
			if act = actM.Get(1).Metadata().Get("foo"); act != "" {
				t.Errorf("Wrong metadata returned: %v != ''", act)
			}
			if act = actM.Get(1).Metadata().Get("root_foo"); act != "bar2" {
				t.Errorf("Wrong metadata returned: %v != bar2", act)
			}
			if act = actM.Get(0).Metadata().Get("root_foo"); act != "" {
				t.Errorf("Wrong metadata returned: %v != ''", act)
			}
		}
		if err = mInput.Acknowledge(nil); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}

	wg.Wait()
}

func testAMQPDisconnect(url string, t *testing.T) {
	subject := "benthos_test_dc"

	outConf := writer.NewAMQPConfig()
	outConf.URL = url
	outConf.Exchange = subject
	outConf.ExchangeDeclare.Enabled = true

	inConf := reader.NewAMQPConfig()
	inConf.URL = url
	inConf.Queue = subject
	inConf.QueueDeclare.Enabled = true
	inConf.BindingsDeclare = append(inConf.BindingsDeclare, reader.AMQPBindingConfig{
		Exchange:   outConf.Exchange,
		RoutingKey: outConf.BindingKey,
	})

	mInput, mOutput, err := createAMQPInputOutput(inConf, outConf)
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

func testAMQP09StreamsALOAsync(url string, t *testing.T) {
	subject := "benthos_test_streams_alo_async"

	outConf := writer.NewAMQPConfig()
	outConf.Exchange = subject
	outConf.URL = url
	outConf.ExchangeDeclare.Enabled = true

	inConf := reader.NewAMQP09Config()
	inConf.URL = url
	inConf.Queue = subject
	inConf.QueueDeclare.Enabled = true
	inConf.BindingsDeclare = append(inConf.BindingsDeclare, reader.AMQP09BindingConfig{
		Exchange:   outConf.Exchange,
		RoutingKey: outConf.BindingKey,
	})

	outputCtr := func() (mOutput writer.Type, err error) {
		if mOutput, err = writer.NewAMQP(outConf, log.Noop(), metrics.Noop()); err != nil {
			return
		}
		err = mOutput.Connect()
		return
	}
	inputCtr := func() (mInput reader.Async, err error) {
		ctx, done := context.WithTimeout(context.Background(), time.Second*60)
		defer done()

		if mInput, err = reader.NewAMQP09(inConf, log.Noop(), metrics.Noop()); err != nil {
			return
		}
		err = mInput.ConnectWithContext(ctx)
		return
	}

	checkALOSynchronousAsync(outputCtr, inputCtr, t)

	subject = "benthos_test_streams_alo_with_dc_async"

	outConf.Exchange = subject
	inConf.Queue = subject
	inConf.BindingsDeclare = []reader.AMQP09BindingConfig{
		{
			Exchange:   outConf.Exchange,
			RoutingKey: outConf.BindingKey,
		},
	}

	checkALOSynchronousAndDieAsync(outputCtr, inputCtr, t)

	subject = "benthos_test_streams_alo_parallel_async"

	outConf.Exchange = subject
	inConf.Queue = subject
	inConf.PrefetchCount = 100
	inConf.BindingsDeclare = []reader.AMQP09BindingConfig{
		{
			Exchange:   outConf.Exchange,
			RoutingKey: outConf.BindingKey,
		},
	}

	checkALOParallelAsync(outputCtr, inputCtr, 50, t)
}

func testAMQP09SinglePart(url string, t *testing.T) {
	subject := "benthos_test_09_single_part"

	outConf := writer.NewAMQPConfig()
	outConf.URL = url
	outConf.Exchange = subject
	outConf.ExchangeDeclare.Enabled = true

	inConf := reader.NewAMQP09Config()
	inConf.URL = url
	inConf.Queue = subject
	inConf.QueueDeclare.Enabled = true
	inConf.BindingsDeclare = append(inConf.BindingsDeclare, reader.AMQP09BindingConfig{
		Exchange:   outConf.Exchange,
		RoutingKey: outConf.BindingKey,
	})

	mInput, mOutput, err := createAMQP09InputOutput(inConf, outConf)
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

	ctx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	lMsgs := len(testMsgs)
	for lMsgs > 0 {
		var actM types.Message
		var ackFn reader.AsyncAckFn
		actM, ackFn, err = mInput.ReadWithContext(ctx)
		if err != nil {
			t.Error(err)
		} else {
			act := string(actM.Get(0).Get())
			if _, exists := testMsgs[act]; !exists {
				t.Errorf("Unexpected message: %v", act)
			}
			delete(testMsgs, act)
			if act = actM.Get(0).Metadata().Get("foo"); act != "bar" {
				t.Errorf("Wrong metadata returned: %v != bar", act)
			}
			if act = actM.Get(0).Metadata().Get("root_foo"); act != "bar2" {
				t.Errorf("Wrong metadata returned: %v != bar2", act)
			}
		}
		if err = ackFn(ctx, response.NewAck()); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}

	wg.Wait()
}

func testAMQP09MultiplePart(url string, t *testing.T) {
	subject := "benthos_test_09_multi_part"

	outConf := writer.NewAMQPConfig()
	outConf.URL = url
	outConf.Exchange = subject
	outConf.ExchangeDeclare.Enabled = true

	inConf := reader.NewAMQP09Config()
	inConf.URL = url
	inConf.Queue = subject
	inConf.QueueDeclare.Enabled = true
	inConf.BindingsDeclare = append(inConf.BindingsDeclare, reader.AMQP09BindingConfig{
		Exchange:   outConf.Exchange,
		RoutingKey: outConf.BindingKey,
	})

	mInput, mOutput, err := createAMQP09InputOutput(inConf, outConf)
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

	ctx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

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
		var ackFn reader.AsyncAckFn
		actM, ackFn, err = mInput.ReadWithContext(ctx)
		if err != nil {
			t.Error(err)
		} else {
			act := string(actM.Get(0).Get())
			if _, exists := testMsgs[act]; !exists {
				t.Errorf("Unexpected message: %v", act)
			}
			delete(testMsgs, act)
			if act = actM.Get(0).Metadata().Get("foo"); act != "bar" {
				t.Errorf("Wrong metadata returned: %v != bar", act)
			}
			if act = actM.Get(1).Metadata().Get("foo"); act != "" {
				t.Errorf("Wrong metadata returned: %v != ''", act)
			}
			if act = actM.Get(1).Metadata().Get("root_foo"); act != "bar2" {
				t.Errorf("Wrong metadata returned: %v != bar2", act)
			}
			if act = actM.Get(0).Metadata().Get("root_foo"); act != "" {
				t.Errorf("Wrong metadata returned: %v != ''", act)
			}
		}
		if err = ackFn(ctx, response.NewAck()); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}

	wg.Wait()
}

func testAMQP09Disconnect(url string, t *testing.T) {
	subject := "benthos_test_09_disconnect"

	outConf := writer.NewAMQPConfig()
	outConf.URL = url
	outConf.Exchange = subject
	outConf.ExchangeDeclare.Enabled = true

	inConf := reader.NewAMQP09Config()
	inConf.URL = url
	inConf.Queue = subject
	inConf.QueueDeclare.Enabled = true
	inConf.BindingsDeclare = append(inConf.BindingsDeclare, reader.AMQP09BindingConfig{
		Exchange:   outConf.Exchange,
		RoutingKey: outConf.BindingKey,
	})

	mInput, mOutput, err := createAMQP09InputOutput(inConf, outConf)
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

	if _, _, err = mInput.ReadWithContext(context.Background()); err != types.ErrTypeClosed && err != types.ErrNotConnected {
		t.Errorf("Wrong error: %v != %v", err, types.ErrTypeClosed)
	}

	wg.Wait()
}
