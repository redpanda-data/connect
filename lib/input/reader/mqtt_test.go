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
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/ory/dockertest"
)

func getMQTTConn(urls []string) (mqtt.Client, error) {
	inConf := mqtt.NewClientOptions().
		SetClientID("UNIT_TEST")
	for _, u := range urls {
		inConf = inConf.AddBroker(u)
	}

	mIn := mqtt.NewClient(inConf)
	tok := mIn.Connect()
	tok.Wait()
	if cErr := tok.Error(); cErr != nil {
		return nil, cErr
	}

	return mIn, nil
}

func sendMQTTMsg(c mqtt.Client, topic, msg string) error {
	mtok := c.Publish(topic, 2, false, msg)
	mtok.Wait()
	return mtok.Error()
}

func TestMQTTIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	t.Skip("Skipping MQTT tests because the library crashes on shutdown")

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to docker: %s", err)
	}
	pool.MaxWait = time.Second * 30

	resource, err := pool.Run("ncarlier/mqtt", "latest", nil)
	if err != nil {
		t.Fatalf("Could not start resource: %s", err)
	}

	urls := []string{fmt.Sprintf("tcp://localhost:%v", resource.GetPort("1883/tcp"))}

	if err = pool.Retry(func() error {
		client, err := getMQTTConn(urls)
		if err == nil {
			client.Disconnect(0)
		}
		return err
	}); err != nil {
		t.Fatalf("Could not connect to docker resource: %s", err)
	}

	defer func() {
		if err = pool.Purge(resource); err != nil {
			t.Logf("Failed to clean up docker resource: %v", err)
		}
	}()

	t.Run("TestMQTTConnect", func(te *testing.T) {
		testMQTTConnect(urls, te)
	})
	t.Run("TestMQTTDisconnect", func(te *testing.T) {
		testMQTTDisconnect(urls, te)
	})
}

func testMQTTConnect(urls []string, t *testing.T) {
	conf := NewMQTTConfig()
	conf.ClientID = "foo"
	conf.Topics = []string{"test_input_1"}
	conf.URLs = urls

	m, err := NewMQTT(conf, log.New(os.Stdout, log.Config{LogLevel: "NONE"}), metrics.DudType{})
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

	var mIn mqtt.Client
	if mIn, err = getMQTTConn(urls); err != nil {
		t.Fatal(err)
	}

	defer mIn.Disconnect(0)

	N := 10

	wg := sync.WaitGroup{}
	wg.Add(N)

	testMsgs := map[string]struct{}{}
	for i := 0; i < N; i++ {
		str := fmt.Sprintf("hello world: %v", i)
		testMsgs[str] = struct{}{}
		go func(testStr string) {
			if sErr := sendMQTTMsg(mIn, "test_input_1", testStr); sErr != nil {
				t.Error(err)
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
		}
		lMsgs = len(testMsgs)
	}

	wg.Wait()
}

func testMQTTDisconnect(urls []string, t *testing.T) {
	conf := NewMQTTConfig()
	conf.ClientID = "foo"
	conf.Topics = []string{"test_input_1"}
	conf.URLs = urls

	m, err := NewMQTT(conf, log.New(os.Stdout, log.Config{LogLevel: "NONE"}), metrics.DudType{})
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

	if _, err = m.Read(); err != types.ErrTypeClosed {
		t.Errorf("Wrong error: %v != %v", err, types.ErrTypeClosed)
	}

	wg.Wait()
}
