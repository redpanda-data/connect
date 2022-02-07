package reader

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/ory/dockertest/v3"
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

	m, err := NewMQTT(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	if err = m.ConnectWithContext(ctx); err != nil {
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
		var actM *message.Batch
		actM, _, err = m.ReadWithContext(ctx)
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

	m, err := NewMQTT(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	if err = m.ConnectWithContext(ctx); err != nil {
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

	if _, _, err = m.ReadWithContext(ctx); err != types.ErrTypeClosed {
		t.Errorf("Wrong error: %v != %v", err, types.ErrTypeClosed)
	}

	wg.Wait()
}
