package io_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/api"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/transaction"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

type apiRegGorillaMutWrapper struct {
	mut *mux.Router
}

func (a apiRegGorillaMutWrapper) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
	a.mut.HandleFunc(path, h)
}

func TestHTTPBasic(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	t.Parallel()

	nTestLoops := 100

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetAPIReg(reg))
	require.NoError(t, err)

	conf := input.NewConfig()
	conf.Type = "http_server"
	conf.HTTPServer.Path = "/testpost"

	h, err := mgr.NewInput(conf)
	require.NoError(t, err)

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
				t.Error(err)
			} else if res.StatusCode != 200 {
				t.Errorf("Wrong error code returned: %v", res.StatusCode)
			}
			resBytes, err := io.ReadAll(res.Body)
			if err != nil {
				t.Error(err)
			}
			if exp, act := output, string(resBytes); exp != act {
				t.Errorf("Wrong sync response: %v != %v", act, exp)
			}
		}(testStr, testResponse)

		var ts message.Transaction
		select {
		case ts = <-h.TransactionChan():
			if res := string(ts.Payload.Get(0).AsBytes()); res != testStr {
				t.Errorf("Wrong result, %v != %v", ts.Payload, res)
			}
			ts.Payload.Get(0).SetBytes([]byte(testResponse))
			require.NoError(t, transaction.SetAsResponse(ts.Payload))
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		require.NoError(t, ts.Ack(tCtx, nil))
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
				t.Error(err)
			} else if res.StatusCode != 200 {
				t.Errorf("Wrong error code returned: %v", res.StatusCode)
			}
		}()

		var ts message.Transaction
		select {
		case ts = <-h.TransactionChan():
			if exp, actual := 2, ts.Payload.Len(); exp != actual {
				t.Errorf("Wrong number of parts: %v != %v", actual, exp)
			} else if exp, actual := partOne, string(ts.Payload.Get(0).AsBytes()); exp != actual {
				t.Errorf("Wrong result, %v != %v", actual, exp)
			} else if exp, actual := partTwo, string(ts.Payload.Get(1).AsBytes()); exp != actual {
				t.Errorf("Wrong result, %v != %v", actual, exp)
			}
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		require.NoError(t, ts.Ack(tCtx, nil))
	}

	// Test requests without content-type
	client := &http.Client{}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testResponse := fmt.Sprintf("response%v", i)
		// Send it as single part
		go func(input, output string) {
			req, err := http.NewRequest(
				"POST", server.URL+"/testpost", bytes.NewBuffer([]byte(input)))
			if err != nil {
				t.Error(err)
			}
			res, err := client.Do(req)
			if err != nil {
				t.Error(err)
			} else if res.StatusCode != 200 {
				t.Errorf("Wrong error code returned: %v", res.StatusCode)
			}
			resBytes, err := io.ReadAll(res.Body)
			if err != nil {
				t.Error(err)
			}
			if exp, act := output, string(resBytes); exp != act {
				t.Errorf("Wrong sync response: %v != %v", act, exp)
			}
		}(testStr, testResponse)

		var ts message.Transaction
		select {
		case ts = <-h.TransactionChan():
			if res := string(ts.Payload.Get(0).AsBytes()); res != testStr {
				t.Errorf("Wrong result, %v != %v", ts.Payload, res)
			}
			ts.Payload.Get(0).SetBytes([]byte(testResponse))
			require.NoError(t, transaction.SetAsResponse(ts.Payload))
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		require.NoError(t, ts.Ack(tCtx, nil))
	}

	h.TriggerStopConsuming()
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func TestHTTPServerLifecycle(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	freePort, err := getFreePort()
	require.NoError(t, err)

	apiConf := api.NewConfig()
	apiConf.Address = fmt.Sprintf("0.0.0.0:%v", freePort)
	apiConf.Enabled = true

	testURL := fmt.Sprintf("http://localhost:%v/foo/bar", freePort)

	apiImpl, err := api.New("", "", apiConf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	go func() {
		_ = apiImpl.ListenAndServe()
	}()
	defer func() {
		_ = apiImpl.Shutdown(context.Background())
	}()

	mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetAPIReg(apiImpl))
	require.NoError(t, err)

	conf := input.NewConfig()
	conf.Type = "http_server"
	conf.HTTPServer.Path = "/foo/bar"

	timeout := time.Second * 5
	readNextMsg := func(in input.Streamed) (message.Batch, error) {
		t.Helper()
		var tran message.Transaction
		select {
		case tran = <-in.TransactionChan():
			require.NoError(t, tran.Ack(tCtx, nil))
		case <-time.After(timeout):
			return nil, errors.New("timed out 2")
		}
		return tran.Payload, nil
	}

	server, err := mgr.NewInput(conf)
	require.NoError(t, err)

	dummyData := []byte("a bunch of jolly leprechauns await")
	go func() {
		resp, cerr := http.Post(testURL, "text/plain", bytes.NewReader(dummyData))
		if assert.NoError(t, cerr) {
			resp.Body.Close()
		}
	}()

	msg, err := readNextMsg(server)
	require.NoError(t, err)
	assert.Equal(t, dummyData, message.GetAllBytes(msg)[0])

	server.TriggerStopConsuming()
	assert.NoError(t, server.WaitForClose(tCtx))

	res, err := http.Post(testURL, "text/plain", bytes.NewReader(dummyData))
	assert.NoError(t, err)
	assert.Equal(t, 404, res.StatusCode)

	serverTwo, err := mgr.NewInput(conf)
	require.NoError(t, err)

	go func() {
		resp, cerr := http.Post(testURL, "text/plain", bytes.NewReader(dummyData))
		if assert.NoError(t, cerr) {
			resp.Body.Close()
		}
	}()

	msg, err = readNextMsg(serverTwo)
	require.NoError(t, err)
	assert.Equal(t, dummyData, message.GetAllBytes(msg)[0])

	serverTwo.TriggerStopConsuming()
	assert.NoError(t, serverTwo.WaitForClose(tCtx))
}

func TestHTTPServerMetadata(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetAPIReg(reg))
	require.NoError(t, err)

	conf := input.NewConfig()
	conf.Type = "http_server"
	conf.HTTPServer.Path = "/across/the/rainbow/bridge"

	server, err := mgr.NewInput(conf)
	require.NoError(t, err)

	defer func() {
		server.TriggerStopConsuming()
		assert.NoError(t, server.WaitForClose(tCtx))
	}()

	testServer := httptest.NewServer(reg.mut)
	defer testServer.Close()

	dummyPath := "/across/the/rainbow/bridge"
	dummyQuery := url.Values{"foo": []string{"bar"}}
	serverURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	serverURL.Path = dummyPath
	serverURL.RawQuery = dummyQuery.Encode()

	dummyData := []byte("a bunch of jolly leprechauns await")
	go func() {
		resp, cerr := http.Post(serverURL.String(), "text/plain", bytes.NewReader(dummyData))
		require.NoError(t, cerr)
		defer resp.Body.Close()
	}()

	timeout := time.Second * 5

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-server.TransactionChan():
			require.NoError(t, tran.Ack(tCtx, nil))
		case <-time.After(timeout):
			return nil, errors.New("timed out 2")
		}
		return tran.Payload, nil
	}

	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, dummyData, message.GetAllBytes(msg)[0])

	part := msg.Get(0)
	assert.Equal(t, dummyPath, part.MetaGetStr("http_server_request_path"))
	assert.Equal(t, "POST", part.MetaGetStr("http_server_verb"))
	assert.Regexp(t, "^Go-http-client/", part.MetaGetStr("http_server_user_agent"))
	assert.Equal(t, "127.0.0.1", part.MetaGetStr("http_server_remote_ip"))
	// Make sure query params are set in the metadata
	assert.Contains(t, "bar", part.MetaGetStr("foo"))
}

func TestHTTPtServerPathParameters(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetAPIReg(reg))
	require.NoError(t, err)

	conf := input.NewConfig()
	conf.Type = "http_server"
	conf.HTTPServer.Path = "/test/{foo}/{bar}"
	conf.HTTPServer.AllowedVerbs = append(conf.HTTPServer.AllowedVerbs, "PUT")

	server, err := mgr.NewInput(conf)
	require.NoError(t, err)

	defer func() {
		server.TriggerStopConsuming()
		assert.NoError(t, server.WaitForClose(tCtx))
	}()

	testServer := httptest.NewServer(reg.mut)
	defer testServer.Close()

	dummyPath := "/test/foo1/bar1"
	dummyQuery := url.Values{"mylove": []string{"will go on"}}
	serverURL, err := url.Parse(testServer.URL)
	require.NoError(t, err)

	serverURL.Path = dummyPath
	serverURL.RawQuery = dummyQuery.Encode()

	dummyData := []byte("a bunch of jolly leprechauns await")
	go func() {
		req, cerr := http.NewRequest("PUT", serverURL.String(), bytes.NewReader(dummyData))
		require.NoError(t, cerr)
		req.Header.Set("Content-Type", "text/plain")
		resp, cerr := http.DefaultClient.Do(req)
		require.NoError(t, cerr)
		defer resp.Body.Close()
	}()

	readNextMsg := func() (message.Batch, error) {
		var tran message.Transaction
		select {
		case tran = <-server.TransactionChan():
			require.NoError(t, tran.Ack(tCtx, nil))
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, dummyData, message.GetAllBytes(msg)[0])

	part := msg.Get(0)

	assert.Equal(t, dummyPath, part.MetaGetStr("http_server_request_path"))
	assert.Equal(t, "PUT", part.MetaGetStr("http_server_verb"))
	assert.Equal(t, "foo1", part.MetaGetStr("foo"))
	assert.Equal(t, "bar1", part.MetaGetStr("bar"))
	assert.Equal(t, "will go on", part.MetaGetStr("mylove"))
}

func TestHTTPBadRequests(t *testing.T) {
	t.Parallel()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetAPIReg(reg))
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.Type = "http_server"
	conf.HTTPServer.Path = "/testpost"

	h, err := mgr.NewInput(conf)
	require.NoError(t, err)

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

	h.TriggerStopConsuming()
	assert.NoError(t, h.WaitForClose(ctx))
}

func TestHTTPTimeout(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	t.Parallel()

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetAPIReg(reg))
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.Type = "http_server"
	conf.HTTPServer.Path = "/testpost"
	conf.HTTPServer.Timeout = "1ms"

	h, err := mgr.NewInput(conf)
	require.NoError(t, err)

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

	h.TriggerStopConsuming()
	if err := h.WaitForClose(tCtx); err != nil {
		t.Error(err)
	}
}

func TestHTTPRateLimit(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	t.Parallel()

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}

	mgrConf := manager.NewResourceConfig()
	require.NoError(t, yaml.Unmarshal([]byte(`
rate_limit_resources:
  - label: foorl
    local:
      count: 1
      interval: 60s
`), &mgrConf))

	mgr, err := manager.New(mgrConf, manager.OptSetAPIReg(reg))
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.Type = "http_server"
	conf.HTTPServer.Path = "/testpost"
	conf.HTTPServer.RateLimit = "foorl"

	h, err := mgr.NewInput(conf)
	require.NoError(t, err)

	server := httptest.NewServer(reg.mut)
	defer server.Close()

	go func() {
		var ts message.Transaction
		select {
		case ts = <-h.TransactionChan():
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		require.NoError(t, ts.Ack(tCtx, nil))
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

	h.TriggerStopConsuming()
	if err := h.WaitForClose(tCtx); err != nil {
		t.Error(err)
	}
}

func TestHTTPServerWebsockets(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	t.Parallel()

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}

	mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetAPIReg(reg))
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.Type = "http_server"
	conf.HTTPServer.WSPath = "/testws"

	h, err := mgr.NewInput(conf)
	require.NoError(t, err)

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
			t.Error(clientErr)
		}
		wg.Done()
	}()

	var ts message.Transaction
	select {
	case ts = <-h.TransactionChan():
	case <-time.After(time.Second):
		t.Error("Timed out waiting for message")
	}
	if exp, act := `[hello world 1]`, fmt.Sprintf("%s", message.GetAllBytes(ts.Payload)); exp != act {
		t.Errorf("Unexpected message: %v != %v", act, exp)
	}
	require.NoError(t, ts.Ack(tCtx, nil))
	wg.Wait()

	wg.Add(1)
	go func() {
		if closeErr := client.WriteMessage(
			websocket.BinaryMessage, []byte("hello world 2"),
		); closeErr != nil {
			t.Error(closeErr)
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
	require.NoError(t, ts.Ack(tCtx, nil))
	wg.Wait()

	h.TriggerStopConsuming()
	if err := h.WaitForClose(tCtx); err != nil {
		t.Error(err)
	}
}

func TestHTTPServerWSRateLimit(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	t.Parallel()

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}

	mgrConf := manager.NewResourceConfig()
	require.NoError(t, yaml.Unmarshal([]byte(`
rate_limit_resources:
  - label: foorl
    local:
      count: 1
      interval: 60s
`), &mgrConf))

	mgr, err := manager.New(mgrConf, manager.OptSetAPIReg(reg))
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.Type = "http_server"
	conf.HTTPServer.WSPath = "/testws"
	conf.HTTPServer.WSWelcomeMessage = "test welcome"
	conf.HTTPServer.WSRateLimitMessage = "test rate limited"
	conf.HTTPServer.RateLimit = "foorl"

	h, err := mgr.NewInput(conf)
	require.NoError(t, err)

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
		var ts message.Transaction
		select {
		case ts = <-h.TransactionChan():
		case <-time.After(time.Second):
			t.Error("Timed out waiting for message")
		}
		require.NoError(t, ts.Ack(tCtx, nil))
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

	h.TriggerStopConsuming()
	if err := h.WaitForClose(tCtx); err != nil {
		t.Error(err)
	}
}

func TestHTTPSyncResponseHeaders(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	t.Parallel()

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetAPIReg(reg))
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.Type = "http_server"
	conf.HTTPServer.Path = "/testpost"
	conf.HTTPServer.Response.Headers["Content-Type"] = "application/json"
	conf.HTTPServer.Response.Headers["foo"] = `${!json("field1")}`
	conf.HTTPServer.Response.ExtractMetadata.IncludePrefixes = []string{"Loca"}
	conf.HTTPServer.Response.ExtractMetadata.IncludePatterns = []string{"name"}

	h, err := mgr.NewInput(conf)
	require.NoError(t, err)

	server := httptest.NewServer(reg.mut)
	defer server.Close()

	input := `{"foo":"test message","field1":"bar"}`

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		req, err := http.NewRequest(http.MethodPost, server.URL+"/testpost", bytes.NewBuffer([]byte(input)))
		if err != nil {
			t.Error(err)
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Location", "Asgard")
		req.Header.Set("Username", "Thor")
		req.Header.Set("Language", "Norse")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Error(err)
		} else if res.StatusCode != 200 {
			t.Errorf("Wrong error code returned: %v", res.StatusCode)
		}
		resBytes, err := io.ReadAll(res.Body)
		if err != nil {
			t.Error(err)
		}
		assert.JSONEq(t, input, string(resBytes))
		if exp, act := "application/json", res.Header.Get("Content-Type"); exp != act {
			t.Errorf("Wrong sync response header: %v != %v", act, exp)
		}
		if exp, act := "bar", res.Header.Get("foo"); exp != act {
			t.Errorf("Wrong sync response header: %v != %v", act, exp)
		}
		if exp, act := "Asgard", res.Header.Get("Location"); exp != act {
			t.Errorf("Wrong sync response header: %v != %v", act, exp)
		}
		if exp, act := "Thor", res.Header.Get("Username"); exp != act {
			t.Errorf("Wrong sync response header: %v != %v", act, exp)
		}
		if exp, act := "", res.Header.Get("Language"); exp != act {
			t.Errorf("Wrong sync response header: %v != %v", act, exp)
		}
	}()

	var ts message.Transaction
	select {
	case ts = <-h.TransactionChan():
		if res := string(ts.Payload.Get(0).AsBytes()); res != input {
			t.Errorf("Wrong result, %v != %v", ts.Payload, res)
		}
		require.NoError(t, transaction.SetAsResponse(ts.Payload))
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message")
	}
	require.NoError(t, ts.Ack(tCtx, nil))

	h.TriggerStopConsuming()
	if err := h.WaitForClose(tCtx); err != nil {
		t.Error(err)
	}

	wg.Wait()
}

func createMultipart(payloads []string, contentType string) (hdr string, bodyBytes []byte, err error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	for i := 0; i < len(payloads) && err == nil; i++ {
		var part io.Writer
		if part, err = writer.CreatePart(textproto.MIMEHeader{
			"Content-Type": []string{contentType},
		}); err == nil {
			_, err = io.Copy(part, bytes.NewReader([]byte(payloads[i])))
		}
	}

	if err != nil {
		return "", nil, err
	}

	writer.Close()
	return writer.FormDataContentType(), body.Bytes(), nil
}

func readMultipart(res *http.Response) ([]string, error) {
	var params map[string]string
	var err error
	if contentType := res.Header.Get("Content-Type"); len(contentType) > 0 {
		if _, params, err = mime.ParseMediaType(contentType); err != nil {
			return nil, err
		}
	}

	var buffer bytes.Buffer
	var output []string

	mr := multipart.NewReader(res.Body, params["boundary"])
	var bufferIndex int64
	for {
		var p *multipart.Part
		if p, err = mr.NextPart(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		var bytesRead int64
		if bytesRead, err = buffer.ReadFrom(p); err != nil {
			return nil, err
		}

		output = append(output, string(buffer.Bytes()[bufferIndex:bufferIndex+bytesRead]))
		bufferIndex += bytesRead
	}

	return output, nil
}

func TestHTTPSyncResponseMultipart(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	t.Parallel()

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetAPIReg(reg))
	require.NoError(t, err)

	conf := input.NewConfig()
	conf.Type = "http_server"
	conf.HTTPServer.Path = "/testpost"
	conf.HTTPServer.Response.Headers["Content-Type"] = "application/json"

	h, err := mgr.NewInput(conf)
	require.NoError(t, err)

	server := httptest.NewServer(reg.mut)
	t.Cleanup(func() {
		server.Close()
	})

	input := []string{
		`{"foo":"test message 1","field1":"bar"}`,
		`{"foo":"test message 2","field1":"baz"}`,
		`{"foo":"test message 3","field1":"buz"}`,
	}
	output := []string{
		`{"foo":"test message 4","field1":"bar"}`,
		`{"foo":"test message 5","field1":"baz"}`,
		`{"foo":"test message 6","field1":"buz"}`,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		hdr, body, err := createMultipart(input, "application/octet-stream")
		require.NoError(t, err)

		res, err := http.Post(server.URL+"/testpost", hdr, bytes.NewReader(body))
		require.NoError(t, err)
		require.Equal(t, 200, res.StatusCode)

		act, err := readMultipart(res)
		require.NoError(t, err)
		assert.Equal(t, output, act)
	}()

	var ts message.Transaction
	select {
	case ts = <-h.TransactionChan():
		for i, in := range input {
			assert.Equal(t, in, string(ts.Payload.Get(i).AsBytes()))
		}
		for i, o := range output {
			ts.Payload.Get(i).SetBytes([]byte(o))
		}
		require.NoError(t, transaction.SetAsResponse(ts.Payload))
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message")
	}
	require.NoError(t, ts.Ack(tCtx, nil))

	h.TriggerStopConsuming()
	err = h.WaitForClose(tCtx)
	require.NoError(t, err)

	wg.Wait()
}

func TestHTTPSyncResponseHeadersStatus(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	t.Parallel()

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetAPIReg(reg))
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.Type = "http_server"
	conf.HTTPServer.Path = "/testpost"
	conf.HTTPServer.Response.Status = `${! meta("status").or("200") }`
	conf.HTTPServer.Response.Headers["Content-Type"] = "application/json"
	conf.HTTPServer.Response.Headers["foo"] = `${!json("field1")}`

	h, err := mgr.NewInput(conf)
	require.NoError(t, err)

	server := httptest.NewServer(reg.mut)
	defer server.Close()

	input := `{"foo":"test message","field1":"bar"}`

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		res, err := http.Post(
			server.URL+"/testpost",
			"application/octet-stream",
			bytes.NewBuffer([]byte(input)),
		)
		if err != nil {
			t.Error(err)
		} else if res.StatusCode != 200 {
			t.Errorf("Wrong error code returned: %v", res.StatusCode)
		}
		resBytes, err := io.ReadAll(res.Body)
		if err != nil {
			t.Error(err)
		}
		assert.JSONEq(t, input, string(resBytes))
		if exp, act := "application/json", res.Header.Get("Content-Type"); exp != act {
			t.Errorf("Wrong sync response header: %v != %v", act, exp)
		}
		if exp, act := "bar", res.Header.Get("foo"); exp != act {
			t.Errorf("Wrong sync response header: %v != %v", act, exp)
		}

		res, err = http.Post(
			server.URL+"/testpost",
			"application/octet-stream",
			bytes.NewBuffer([]byte(input)),
		)
		if err != nil {
			t.Error(err)
		} else if res.StatusCode != 400 {
			t.Errorf("Wrong error code returned: %v", res.StatusCode)
		}
		resBytes, err = io.ReadAll(res.Body)
		if err != nil {
			t.Error(err)
		}
		assert.JSONEq(t, input, string(resBytes))
		if exp, act := "application/json", res.Header.Get("Content-Type"); exp != act {
			t.Errorf("Wrong sync response header: %v != %v", act, exp)
		}
		if exp, act := "bar", res.Header.Get("foo"); exp != act {
			t.Errorf("Wrong sync response header: %v != %v", act, exp)
		}
	}()

	// Non errored message
	var ts message.Transaction
	select {
	case ts = <-h.TransactionChan():
		if res := string(ts.Payload.Get(0).AsBytes()); res != input {
			t.Errorf("Wrong result, %v != %v", ts.Payload, res)
		}
		require.NoError(t, transaction.SetAsResponse(ts.Payload))
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message")
	}
	require.NoError(t, ts.Ack(tCtx, nil))

	// Errored message
	select {
	case ts = <-h.TransactionChan():
		if res := string(ts.Payload.Get(0).AsBytes()); res != input {
			t.Errorf("Wrong result, %v != %v", ts.Payload, res)
		}
		ts.Payload.Get(0).MetaSetMut("status", "400")
		require.NoError(t, transaction.SetAsResponse(ts.Payload))
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for message")
	}
	require.NoError(t, ts.Ack(tCtx, nil))

	h.TriggerStopConsuming()
	if err := h.WaitForClose(tCtx); err != nil {
		t.Error(err)
	}

	wg.Wait()
}
