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

package reader

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/ory/dockertest"
	"github.com/streadway/amqp"
)

func TestAMQPIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

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

	url := fmt.Sprintf("amqp://guest:guest@localhost:%v/", resource.GetPort("5672/tcp"))

	if err = pool.Retry(func() error {
		client, err := amqp.Dial(url)
		if err == nil {
			var mChan *amqp.Channel
			if mChan, err = client.Channel(); err == nil {
				err = mChan.ExchangeDeclare(
					"test-exchange", // name of the exchange
					"direct",        // type
					true,            // durable
					false,           // delete when complete
					false,           // internal
					false,           // noWait
					nil,             // arguments
				)
			}
			client.Close()
		}

		return err
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	t.Run("TestAMQPConnect", func(te *testing.T) {
		testAMQPConnect(url, te)
	})
	t.Run("TestAMQPDisconnect", func(te *testing.T) {
		testAMQPDisconnect(url, te)
	})
}

func testAMQPConnect(url string, t *testing.T) {
	exchange := "test-exchange"
	key := "benthos-key"

	conf := NewAMQPConfig()
	conf.URL = url
	conf.QueueDeclare.Enabled = true
	conf.BindingsDeclare = append(conf.BindingsDeclare, AMQPBindingConfig{
		Exchange:   exchange,
		RoutingKey: key,
	})

	m, err := NewAMQP(conf, log.New(os.Stdout, log.Config{LogLevel: "NONE"}), metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		m.CloseAsync()
		if cErr := m.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	var mIn *amqp.Connection
	if mIn, err = amqp.Dial(url); err != nil {
		t.Fatal(err)
	}

	var mChan *amqp.Channel
	if mChan, err = mIn.Channel(); err != nil {
		t.Fatalf("AMQP Channel: %s", err)
	}

	defer func() {
		mChan.Close()
		mIn.Close()
	}()

	N := 10

	wg := sync.WaitGroup{}
	wg.Add(N)

	testMsgs := map[string]struct{}{}
	for i := 0; i < N; i++ {
		str := fmt.Sprintf("hello world: %v", i)
		testMsgs[str] = struct{}{}
		go func(testStr string) {
			if pErr := mChan.Publish(exchange, key, false, false, amqp.Publishing{
				Headers: amqp.Table{
					"foo": "bar",
					"root": amqp.Table{
						"foo": "bar2",
					},
				},
				ContentType:     "application/octet-stream",
				ContentEncoding: "",
				Body:            []byte(testStr),
				DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
				Priority:        0,              // 0-9
			}); pErr != nil {
				t.Error(pErr)
			}
			wg.Done()
		}(str)
	}

	lMsgs := len(testMsgs)
	for lMsgs > 0 {
		var actM types.Message
		actM, err = m.Read()
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
		if err = m.Acknowledge(nil); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}

	wg.Wait()
}

func testAMQPBatch(url string, t *testing.T) {
	exchange := "test-exchange"
	key := "benthos-key"

	conf := NewAMQPConfig()
	conf.URL = url
	conf.QueueDeclare.Enabled = true
	conf.MaxBatchCount = 10
	conf.BindingsDeclare = append(conf.BindingsDeclare, AMQPBindingConfig{
		Exchange:   exchange,
		RoutingKey: key,
	})

	m, err := NewAMQP(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		m.CloseAsync()
		if cErr := m.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
	}()

	var mIn *amqp.Connection
	if mIn, err = amqp.Dial(url); err != nil {
		t.Fatal(err)
	}

	var mChan *amqp.Channel
	if mChan, err = mIn.Channel(); err != nil {
		t.Fatalf("AMQP Channel: %s", err)
	}

	defer func() {
		mChan.Close()
		mIn.Close()
	}()

	N := 10

	wg := sync.WaitGroup{}
	wg.Add(N)

	testMsgs := map[string]struct{}{}
	for i := 0; i < N; i++ {
		str := fmt.Sprintf("hello world: %v", i)
		testMsgs[str] = struct{}{}
		go func(testStr string) {
			if pErr := mChan.Publish(exchange, key, false, false, amqp.Publishing{
				Headers: amqp.Table{
					"foo": "bar",
					"root": amqp.Table{
						"foo": "bar2",
					},
				},
				ContentType:     "application/octet-stream",
				ContentEncoding: "",
				Body:            []byte(testStr),
				DeliveryMode:    1, // 1=non-persistent, 2=persistent
				Priority:        0, // 0-9
			}); pErr != nil {
				t.Error(pErr)
			}
			wg.Done()
		}(str)
	}

	wg.Wait()

	lMsgs := len(testMsgs)
	for lMsgs > 0 {
		var actM types.Message
		actM, err = m.Read()
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
		if err = m.Acknowledge(nil); err != nil {
			t.Error(err)
		}
		lMsgs = len(testMsgs)
	}
}

func testAMQPDisconnect(url string, t *testing.T) {
	conf := NewAMQPConfig()
	conf.URL = url
	conf.QueueDeclare.Enabled = true

	m, err := NewAMQP(conf, log.New(os.Stdout, log.Config{LogLevel: "NONE"}), metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		m.CloseAsync()
		if cErr := m.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
		wg.Done()
	}()

	if _, err = m.Read(); err != types.ErrTypeClosed && err != types.ErrNotConnected {
		t.Errorf("Wrong error: %v != %v", err, types.ErrTypeClosed)
	}

	wg.Wait()
}
