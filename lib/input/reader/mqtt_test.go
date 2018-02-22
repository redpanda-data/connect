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

// +build integration

package reader

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func getMQTTConn(urls []string, t *testing.T) mqtt.Client {
	inConf := mqtt.NewClientOptions().
		SetClientID("UNIT_TEST")
	for _, u := range urls {
		inConf = inConf.AddBroker(u)
	}

	mIn := mqtt.NewClient(inConf)
	tok := mIn.Connect()
	tok.Wait()
	if cErr := tok.Error(); cErr != nil {
		t.Fatal(cErr)
	}

	return mIn
}

func sendMsg(c mqtt.Client, topic, msg string, t *testing.T) {
	mtok := c.Publish(topic, 2, false, msg)
	mtok.Wait()
	if mErr := mtok.Error(); mErr != nil {
		t.Error(mErr)
	}
}

func TestMQTTConnect(t *testing.T) {
	conf := NewMQTTConfig()
	conf.ClientID = "foo"
	conf.Topics = []string{"test_input_1"}

	m, err := NewMQTT(conf, log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"}), metrics.DudType{})
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

	mIn := getMQTTConn(m.urls, t)

	testMsgs := map[string]struct{}{}
	for i := 0; i < 10; i++ {
		str := fmt.Sprintf("hello world: %v", i)
		testMsgs[str] = struct{}{}
		go sendMsg(mIn, "test_input_1", str, t)
	}

	lMsgs := len(testMsgs)
	for lMsgs > 0 {
		var actM types.Message
		actM, err = m.Read()
		if err != nil {
			t.Error(err)
		} else {
			act := string(actM.Parts[0])
			if _, exists := testMsgs[act]; !exists {
				t.Errorf("Unexpected message: %v", act)
			}
			delete(testMsgs, act)
		}
		lMsgs = len(testMsgs)
	}
}

func TestMQTTDisconnect(t *testing.T) {
	conf := NewMQTTConfig()
	conf.ClientID = "foo"
	conf.Topics = []string{"test_input_1"}

	m, err := NewMQTT(conf, log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"}), metrics.DudType{})
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
