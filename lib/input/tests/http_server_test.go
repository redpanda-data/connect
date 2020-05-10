package tests

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/roundtrip"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/gorilla/websocket"
)

type apiRegMutWrapper struct {
	mut *http.ServeMux
}

func (a apiRegMutWrapper) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
	a.mut.HandleFunc(path, h)
}

func TestHTTPBasic(t *testing.T) {
	t.Parallel()

	nTestLoops := 100

	reg := apiRegMutWrapper{mut: &http.ServeMux{}}
	mgr, err := manager.New(manager.NewConfig(), reg, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.HTTPServer.Path = "/testpost"

	h, err := input.NewHTTPServer(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(reg.mut)
	defer server.Close()

	// Test both single and multipart messages.
	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testResponse := fmt.Sprintf("response%v", i)
		// Send it as single part
		go func(input, output string) {
			res, err := http.Post(
				server.URL+"/testpost",
				"application/octet-stream",
				bytes.NewBuffer([]byte(input)),
			)
			if err != nil {
				t.Fatal(err)
			} else if res.StatusCode != 200 {
				t.Fatalf("Wrong error code returned: %v", res.StatusCode)
			}
			resBytes, err := ioutil.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}
			if exp, act := output, string(resBytes); exp != act {
				t.Errorf("Wrong sync response: %v != %v", act, exp)
			}
		}(testStr, testResponse)

		var ts types.Transaction
		select {
		case ts = <-h.TransactionChan():
			if res := string(ts.Payload.Get(0).Get()); res != testStr {
				t.Errorf("Wrong result, %v != %v", ts.Payload, res)
			}
			ts.Payload.Get(0).Set([]byte(testResponse))
			roundtrip.SetAsResponse(ts.Payload)
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}
	}

	// Test MIME multipart parsing, as defined in RFC 2046
	for i := 0; i < nTestLoops; i++ {
		partOne := fmt.Sprintf("test%v part one", i)
		partTwo := fmt.Sprintf("test%v part two", i)

		testStr := fmt.Sprintf(
			"--foo\r\n"+
				"Content-Type: application/octet-stream\r\n\r\n"+
				"%v\r\n"+
				"--foo\r\n"+
				"Content-Type: application/octet-stream\r\n\r\n"+
				"%v\r\n"+
				"--foo--\r\n",
			partOne, partTwo)

		// Send it as multi part
		go func() {
			if res, err := http.Post(
				server.URL+"/testpost",
				"multipart/mixed; boundary=foo",
				bytes.NewBuffer([]byte(testStr)),
			); err != nil {
				t.Fatal(err)
			} else if res.StatusCode != 200 {
				t.Fatalf("Wrong error code returned: %v", res.StatusCode)
			}
		}()

		var ts types.Transaction
		select {
		case ts = <-h.TransactionChan():
			if exp, actual := 2, ts.Payload.Len(); exp != actual {
				t.Errorf("Wrong number of parts: %v != %v", actual, exp)
			} else if exp, actual := partOne, string(ts.Payload.Get(0).Get()); exp != actual {
				t.Errorf("Wrong result, %v != %v", actual, exp)
			} else if exp, actual := partTwo, string(ts.Payload.Get(1).Get()); exp != actual {
				t.Errorf("Wrong result, %v != %v", actual, exp)
			}
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}
	}

	//Test requests without content-type
	client := &http.Client{}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testResponse := fmt.Sprintf("response%v", i)
		// Send it as single part
		go func(input, output string) {
			req, err := http.NewRequest(
				"POST", server.URL+"/testpost", bytes.NewBuffer([]byte(input)))

			res, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			} else if res.StatusCode != 200 {
				t.Fatalf("Wrong error code returned: %v", res.StatusCode)
			}
			resBytes, err := ioutil.ReadAll(res.Body)
			if err != nil {
				t.Fatal(err)
			}
			if exp, act := output, string(resBytes); exp != act {
				t.Errorf("Wrong sync response: %v != %v", act, exp)
			}
		}(testStr, testResponse)

		var ts types.Transaction
		select {
		case ts = <-h.TransactionChan():
			if res := string(ts.Payload.Get(0).Get()); res != testStr {
				t.Errorf("Wrong result, %v != %v", ts.Payload, res)
			}
			ts.Payload.Get(0).Set([]byte(testResponse))
			roundtrip.SetAsResponse(ts.Payload)
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}
	}

	h.CloseAsync()
}

func TestHTTPBadRequests(t *testing.T) {
	t.Parallel()

	reg := apiRegMutWrapper{mut: &http.ServeMux{}}
	mgr, err := manager.New(manager.NewConfig(), reg, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.HTTPServer.Path = "/testpost"

	h, err := input.NewHTTPServer(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(reg.mut)
	defer server.Close()

	res, err := http.Get(server.URL + "/testpost")
	if err != nil {
		t.Error(err)
		return
	}
	if exp, act := http.StatusMethodNotAllowed, res.StatusCode; exp != act {
		t.Errorf("unexpected HTTP response code: %v != %v", exp, act)
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestHTTPTimeout(t *testing.T) {
	t.Parallel()

	reg := apiRegMutWrapper{mut: &http.ServeMux{}}
	mgr, err := manager.New(manager.NewConfig(), reg, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.HTTPServer.Path = "/testpost"
	conf.HTTPServer.Timeout = "1ms"

	h, err := input.NewHTTPServer(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(reg.mut)
	defer server.Close()

	var res *http.Response
	res, err = http.Post(
		server.URL+"/testpost",
		"application/octet-stream",
		bytes.NewBuffer([]byte("hello world")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := http.StatusRequestTimeout, res.StatusCode; exp != act {
		t.Errorf("Unexpected status code: %v != %v", exp, act)
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestHTTPRateLimit(t *testing.T) {
	t.Parallel()

	reg := apiRegMutWrapper{mut: &http.ServeMux{}}

	rlConf := ratelimit.NewConfig()
	rlConf.Type = ratelimit.TypeLocal
	rlConf.Local.Count = 1
	rlConf.Local.Interval = "60s"

	mgrConf := manager.NewConfig()
	mgrConf.RateLimits["foorl"] = rlConf
	mgr, err := manager.New(mgrConf, reg, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.HTTPServer.Path = "/testpost"
	conf.HTTPServer.RateLimit = "foorl"

	h, err := input.NewHTTPServer(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(reg.mut)
	defer server.Close()

	go func() {
		var ts types.Transaction
		select {
		case ts = <-h.TransactionChan():
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}
	}()

	var res *http.Response
	res, err = http.Post(
		server.URL+"/testpost",
		"application/octet-stream",
		bytes.NewBuffer([]byte("hello world")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := http.StatusOK, res.StatusCode; exp != act {
		t.Errorf("Unexpected status code: %v != %v", exp, act)
	}

	res, err = http.Post(
		server.URL+"/testpost",
		"application/octet-stream",
		bytes.NewBuffer([]byte("hello world")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := http.StatusTooManyRequests, res.StatusCode; exp != act {
		t.Errorf("Unexpected status code: %v != %v", exp, act)
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestHTTPServerWebsockets(t *testing.T) {
	t.Parallel()

	reg := apiRegMutWrapper{mut: &http.ServeMux{}}

	mgr, err := manager.New(manager.NewConfig(), reg, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.HTTPServer.WSPath = "/testws"

	h, err := input.NewHTTPServer(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(reg.mut)
	defer server.Close()

	purl, err := url.Parse(server.URL + "/testws")
	if err != nil {
		t.Fatal(err)
	}
	purl.Scheme = "ws"

	var client *websocket.Conn
	if client, _, err = websocket.DefaultDialer.Dial(purl.String(), http.Header{}); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		if clientErr := client.WriteMessage(
			websocket.BinaryMessage, []byte("hello world 1"),
		); clientErr != nil {
			t.Fatal(clientErr)
		}
		wg.Done()
	}()

	var ts types.Transaction
	select {
	case ts = <-h.TransactionChan():
	case <-time.After(time.Second):
		t.Error("Timed out waiting for message")
	}
	if exp, act := `[hello world 1]`, fmt.Sprintf("%s", message.GetAllBytes(ts.Payload)); exp != act {
		t.Errorf("Unexpected message: %v != %v", act, exp)
	}
	select {
	case ts.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Error("Timed out waiting for response")
	}
	wg.Wait()

	wg.Add(1)
	go func() {
		if closeErr := client.WriteMessage(
			websocket.BinaryMessage, []byte("hello world 2"),
		); closeErr != nil {
			t.Fatal(closeErr)
		}
		wg.Done()
	}()

	select {
	case ts = <-h.TransactionChan():
	case <-time.After(time.Second):
		t.Error("Timed out waiting for message")
	}
	if exp, act := `[hello world 2]`, fmt.Sprintf("%s", message.GetAllBytes(ts.Payload)); exp != act {
		t.Errorf("Unexpected message: %v != %v", act, exp)
	}
	select {
	case ts.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Error("Timed out waiting for response")
	}
	wg.Wait()

	h.CloseAsync()
	if err := h.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestHTTPServerWSRateLimit(t *testing.T) {
	t.Parallel()

	reg := apiRegMutWrapper{mut: &http.ServeMux{}}

	rlConf := ratelimit.NewConfig()
	rlConf.Type = ratelimit.TypeLocal
	rlConf.Local.Count = 1
	rlConf.Local.Interval = "60s"

	mgrConf := manager.NewConfig()
	mgrConf.RateLimits["foorl"] = rlConf
	mgr, err := manager.New(mgrConf, reg, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.HTTPServer.WSPath = "/testws"
	conf.HTTPServer.WSWelcomeMessage = "test welcome"
	conf.HTTPServer.WSRateLimitMessage = "test rate limited"
	conf.HTTPServer.RateLimit = "foorl"

	h, err := input.NewHTTPServer(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(reg.mut)
	defer server.Close()

	purl, err := url.Parse(server.URL + "/testws")
	if err != nil {
		t.Fatal(err)
	}
	purl.Scheme = "ws"

	var client *websocket.Conn
	if client, _, err = websocket.DefaultDialer.Dial(purl.String(), http.Header{}); err != nil {
		t.Fatal(err)
	}

	go func() {
		var ts types.Transaction
		select {
		case ts = <-h.TransactionChan():
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Error("Timed out waiting for response")
		}
	}()

	var msgBytes []byte
	if _, msgBytes, err = client.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	if exp, act := "test welcome", string(msgBytes); exp != act {
		t.Errorf("Unexpected welcome message: %v != %v", act, exp)
	}

	if err = client.WriteMessage(
		websocket.BinaryMessage, []byte("hello world"),
	); err != nil {
		t.Fatal(err)
	}

	if err = client.WriteMessage(
		websocket.BinaryMessage, []byte("hello world"),
	); err != nil {
		t.Fatal(err)
	}

	if _, msgBytes, err = client.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	if exp, act := "test rate limited", string(msgBytes); exp != act {
		t.Errorf("Unexpected rate limit message: %v != %v", act, exp)
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestHTTPSyncResponseHeaders(t *testing.T) {
	t.Parallel()

	reg := apiRegMutWrapper{mut: &http.ServeMux{}}
	mgr, err := manager.New(manager.NewConfig(), reg, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.HTTPServer.Path = "/testpost"
	conf.HTTPServer.Response.Headers["Content-Type"] = "application/json"
	conf.HTTPServer.Response.Headers["foo"] = `${!json("field1")}`

	h, err := input.NewHTTPServer(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(reg.mut)
	defer server.Close()

	input := `{"foo":"test message","field1":"bar"}`

	go func() {
		res, err := http.Post(
			server.URL+"/testpost",
			"application/octet-stream",
			bytes.NewBuffer([]byte(input)),
		)
		if err != nil {
			t.Fatal(err)
		} else if res.StatusCode != 200 {
			t.Fatalf("Wrong error code returned: %v", res.StatusCode)
		}
		resBytes, err := ioutil.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if exp, act := input, string(resBytes); exp != act {
			t.Errorf("Wrong sync response: %v != %v", act, exp)
		}
		if exp, act := "application/json", res.Header.Get("Content-Type"); exp != act {
			t.Errorf("Wrong sync response header: %v != %v", act, exp)
		}
		if exp, act := "bar", res.Header.Get("foo"); exp != act {
			t.Errorf("Wrong sync response header: %v != %v", act, exp)
		}
	}()

	var ts types.Transaction
	select {
	case ts = <-h.TransactionChan():
		if res := string(ts.Payload.Get(0).Get()); res != input {
			t.Errorf("Wrong result, %v != %v", ts.Payload, res)
		}
		roundtrip.SetAsResponse(ts.Payload)
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message")
	}
	select {
	case ts.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Error("Timed out waiting for response")
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}
