package io_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func parseYAMLOutputConf(t testing.TB, formatStr string, args ...any) (conf output.Config) {
	t.Helper()
	var err error
	conf, err = output.FromYAML(fmt.Sprintf(formatStr, args...))
	require.NoError(t, err)
	return
}

func TestHTTPServerOutputBasic(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	nTestLoops := 10

	port := getFreePort(t)
	conf := parseYAMLOutputConf(t, `
http_server:
  address: localhost:%v
  path: /testpost
`, port)

	h, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	msgChan := make(chan message.Transaction)
	resChan := make(chan error)

	if err = h.Consume(msgChan); err != nil {
		t.Error(err)
		return
	}
	if err = h.Consume(msgChan); err == nil {
		t.Error("Expected error from double listen")
	}

	<-time.After(time.Millisecond * 100)

	// Test both single and multipart messages.
	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)

		go func() {
			testMsg := message.QuickBatch([][]byte{[]byte(testStr)})
			select {
			case msgChan <- message.NewTransaction(testMsg, resChan):
			case <-time.After(time.Second):
				t.Error("Timed out waiting for message")
				return
			}
			select {
			case resMsg := <-resChan:
				if resMsg != nil {
					t.Error(resMsg)
				}
			case <-time.After(time.Second):
				t.Error("Timed out waiting for response")
			}
		}()

		res, err := http.Get(fmt.Sprintf("http://localhost:%v/testpost", port))
		if err != nil {
			t.Error(err)
			return
		}
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Errorf("Wrong error code returned: %v", res.StatusCode)
			return
		}
	}

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))
}

func TestHTTPServerOutputBadRequests(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	port := getFreePort(t)
	conf := parseYAMLOutputConf(t, `
http_server:
  address: localhost:%v
  path: /testpost
`, port)

	h, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	msgChan := make(chan message.Transaction)

	if err = h.Consume(msgChan); err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 100)

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))

	_, err = http.Get(fmt.Sprintf("http://localhost:%v/testpost", port))
	if err == nil {
		t.Error("request success when service should be closed")
	}
}

func TestHTTPServerOutputTimeout(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	port := getFreePort(t)
	conf := parseYAMLOutputConf(t, `
http_server:
  address: localhost:%v
  path: /testpost
  timeout: 1ms
`, port)

	h, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	msgChan := make(chan message.Transaction)

	if err = h.Consume(msgChan); err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 100)

	var res *http.Response
	res, err = http.Get(fmt.Sprintf("http://localhost:%v/testpost", port))
	if err != nil {
		t.Error(err)
		return
	}
	if exp, act := http.StatusRequestTimeout, res.StatusCode; exp != act {
		t.Errorf("Unexpected status code: %v != %v", exp, act)
	}

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))
}
