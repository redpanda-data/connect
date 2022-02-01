package input_test

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

	"github.com/Jeffail/benthos/v3/lib/api"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/roundtrip"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	_ "github.com/Jeffail/benthos/v3/public/components/all"
)

/*
type apiRegGorillaMutWrapper struct {
	mut *http.ServeMux
}

func (a apiRegGorillaMutWrapper) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
	a.mut.HandleFunc(path, h)
}
*/

type apiRegGorillaMutWrapper struct {
	mut *mux.Router
}

func (a apiRegGorillaMutWrapper) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
	a.mut.HandleFunc(path, h)
}

func TestHTTPBasic(t *testing.T) {
	t.Parallel()

	nTestLoops := 100

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.NewV2(manager.NewResourceConfig(), reg, log.Noop(), metrics.Noop())
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
				t.Error(err)
			} else if res.StatusCode != 200 {
				t.Errorf("Wrong error code returned: %v", res.StatusCode)
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
	defer apiImpl.Shutdown(context.Background())

	mgr, err := manager.NewV2(manager.NewResourceConfig(), apiImpl, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	conf := input.NewConfig()
	conf.HTTPServer.Path = "/foo/bar"

	timeout := time.Second * 5
	readNextMsg := func(in input.Type) (types.Message, error) {
		t.Helper()
		var tran types.Transaction
		select {
		case tran = <-in.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(timeout):
				return nil, errors.New("timed out 1")
			}
		case <-time.After(timeout):
			return nil, errors.New("timed out 2")
		}
		return tran.Payload, nil
	}

	server, err := input.NewHTTPServer(conf, mgr, log.Noop(), metrics.Noop())
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

	server.CloseAsync()
	assert.NoError(t, server.WaitForClose(time.Second))

	res, err := http.Post(testURL, "text/plain", bytes.NewReader(dummyData))
	assert.NoError(t, err)
	assert.Equal(t, 404, res.StatusCode)

	serverTwo, err := input.NewHTTPServer(conf, mgr, log.Noop(), metrics.Noop())
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

	serverTwo.CloseAsync()
	assert.NoError(t, serverTwo.WaitForClose(time.Second))
}

func TestHTTPServerMetadata(t *testing.T) {
	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.NewV2(manager.NewResourceConfig(), reg, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	conf := input.NewConfig()
	conf.HTTPServer.Path = "/across/the/rainbow/bridge"

	server, err := input.NewHTTPServer(conf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	defer func() {
		server.CloseAsync()
		assert.NoError(t, server.WaitForClose(time.Second))
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

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-server.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(timeout):
				return nil, errors.New("timed out 1")
			}
		case <-time.After(timeout):
			return nil, errors.New("timed out 2")
		}
		return tran.Payload, nil
	}

	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, dummyData, message.GetAllBytes(msg)[0])

	meta := msg.Get(0).Metadata()
	assert.Equal(t, dummyPath, meta.Get("http_server_request_path"))
	assert.Equal(t, "POST", meta.Get("http_server_verb"))
	assert.Regexp(t, "^Go-http-client/", meta.Get("http_server_user_agent"))
	// Make sure query params are set in the metadata
	assert.Contains(t, "bar", meta.Get("foo"))
}

func TestHTTPtServerPathParameters(t *testing.T) {
	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.NewV2(manager.NewResourceConfig(), reg, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	conf := input.NewConfig()
	conf.HTTPServer.Path = "/test/{foo}/{bar}"
	conf.HTTPServer.AllowedVerbs = append(conf.HTTPServer.AllowedVerbs, "PUT")

	server, err := input.NewHTTPServer(conf, mgr, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	defer func() {
		server.CloseAsync()
		assert.NoError(t, server.WaitForClose(time.Second))
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

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-server.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, dummyData, message.GetAllBytes(msg)[0])

	meta := msg.Get(0).Metadata()

	assert.Equal(t, dummyPath, meta.Get("http_server_request_path"))
	assert.Equal(t, "PUT", meta.Get("http_server_verb"))
	assert.Equal(t, "foo1", meta.Get("foo"))
	assert.Equal(t, "bar1", meta.Get("bar"))
	assert.Equal(t, "will go on", meta.Get("mylove"))
}

func TestHTTPBadRequests(t *testing.T) {
	t.Parallel()

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.NewV2(manager.NewResourceConfig(), reg, log.Noop(), metrics.Noop())
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

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.NewV2(manager.NewResourceConfig(), reg, log.Noop(), metrics.Noop())
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

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}

	mgrConf := manager.NewResourceConfig()
	require.NoError(t, yaml.Unmarshal([]byte(`
rate_limit_resources:
  - label: foorl
    local:
      count: 1
      interval: 60s
`), &mgrConf))

	mgr, err := manager.NewV2(mgrConf, reg, log.Noop(), metrics.Noop())
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

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}

	mgr, err := manager.NewV2(manager.NewResourceConfig(), reg, log.Noop(), metrics.Noop())
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
			t.Error(clientErr)
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

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}

	mgrConf := manager.NewResourceConfig()
	require.NoError(t, yaml.Unmarshal([]byte(`
rate_limit_resources:
  - label: foorl
    local:
      count: 1
      interval: 60s
`), &mgrConf))

	mgr, err := manager.NewV2(mgrConf, reg, log.Noop(), metrics.Noop())
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

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.NewV2(manager.NewResourceConfig(), reg, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.HTTPServer.Path = "/testpost"
	conf.HTTPServer.Response.Headers["Content-Type"] = "application/json"
	conf.HTTPServer.Response.Headers["foo"] = `${!json("field1")}`
	conf.HTTPServer.Response.ExtractMetadata.IncludePrefixes = []string{"Loca"}
	conf.HTTPServer.Response.ExtractMetadata.IncludePatterns = []string{"name"}

	h, err := input.NewHTTPServer(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

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
		if exp, act := input, string(resBytes); exp != act {
			t.Errorf("Wrong sync response: %v != %v", act, exp)
		}
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
	t.Parallel()

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.NewV2(manager.NewResourceConfig(), reg, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	conf := input.NewConfig()
	conf.HTTPServer.Path = "/testpost"
	conf.HTTPServer.Response.Headers["Content-Type"] = "application/json"

	h, err := input.NewHTTPServer(conf, mgr, log.Noop(), metrics.Noop())
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

	var ts types.Transaction
	select {
	case ts = <-h.TransactionChan():
		for i, in := range input {
			assert.Equal(t, in, string(ts.Payload.Get(i).Get()))
		}
		for i, o := range output {
			ts.Payload.Get(i).Set([]byte(o))
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
	err = h.WaitForClose(time.Second * 5)
	require.NoError(t, err)

	wg.Wait()
}

func TestHTTPSyncResponseHeadersStatus(t *testing.T) {
	t.Parallel()

	reg := apiRegGorillaMutWrapper{mut: mux.NewRouter()}
	mgr, err := manager.NewV2(manager.NewResourceConfig(), reg, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	conf := input.NewConfig()
	conf.HTTPServer.Path = "/testpost"
	conf.HTTPServer.Response.Status = `${! meta("status").or("200") }`
	conf.HTTPServer.Response.Headers["Content-Type"] = "application/json"
	conf.HTTPServer.Response.Headers["foo"] = `${!json("field1")}`

	h, err := input.NewHTTPServer(conf, mgr, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

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
		if exp, act := input, string(resBytes); exp != act {
			t.Errorf("Wrong sync response: %v != %v", act, exp)
		}
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

	// Non errored message
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

	// Errored message
	select {
	case ts = <-h.TransactionChan():
		if res := string(ts.Payload.Get(0).Get()); res != input {
			t.Errorf("Wrong result, %v != %v", ts.Payload, res)
		}
		ts.Payload.Get(0).Metadata().Set("status", "400")
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

	wg.Wait()
}
