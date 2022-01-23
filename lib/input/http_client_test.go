package input

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPClientGET(t *testing.T) {
	inputs := []string{
		"foo1",
		"foo2",
		"foo3",
		"foo4",
		"foo5",
	}

	var reqCount uint32
	index := 0

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if exp, act := "GET", r.Method; exp != act {
			t.Errorf("Wrong method: %v != %v", act, exp)
		}
		atomic.AddUint32(&reqCount, 1)
		w.Write([]byte(inputs[index%len(inputs)]))
		index++
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = ts.URL + "/testpost"
	conf.HTTPClient.Retry = "1ms"

	h, err := NewHTTPClient(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	var tr types.Transaction
	var open bool

	for _, expPart := range inputs {
		select {
		case tr, open = <-h.TransactionChan():
			if !open {
				t.Fatal("Chan not open")
			}
			if exp, act := 1, tr.Payload.Len(); exp != act {
				t.Fatalf("Wrong count of parts: %v != %v", act, exp)
			}
			if exp, act := expPart, string(tr.Payload.Get(0).Get()); exp != act {
				t.Errorf("Wrong part: %v != %v", act, exp)
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}

		select {
		case tr.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	if exp, act := uint32(len(inputs)), atomic.LoadUint32(&reqCount); exp != act && exp+1 != act {
		t.Errorf("Wrong count of HTTP attempts: %v != %v", act, exp)
	}
}

func TestHTTPClientPagination(t *testing.T) {
	var paths []string
	var pathsLock sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello%v", len(paths))
		pathsLock.Lock()
		paths = append(paths, r.URL.Path)
		pathsLock.Unlock()
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = ts.URL + "/${!content()}"
	conf.HTTPClient.Retry = "1ms"

	h, err := NewHTTPClient(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	var tr types.Transaction
	var open bool

	for i := 0; i < 10; i++ {
		exp := fmt.Sprintf("hello%v", i)
		select {
		case tr, open = <-h.TransactionChan():
			require.True(t, open)
			require.Equal(t, 1, tr.Payload.Len())
			assert.Equal(t, exp, string(tr.Payload.Get(0).Get()))
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
		select {
		case tr.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}

	h.CloseAsync()
	assert.NoError(t, h.WaitForClose(time.Second))

	pathsLock.Lock()
	defer pathsLock.Unlock()
	for i, url := range paths {
		expURL := "/"
		if i > 0 {
			expURL = fmt.Sprintf("/hello%v", i-1)
		}
		assert.Equal(t, expURL, url)
	}
}

func TestHTTPClientGETError(t *testing.T) {
	t.Parallel()

	requestChan := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nah", http.StatusBadGateway)
		select {
		case requestChan <- struct{}{}:
		default:
		}
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = ts.URL + "/testpost"
	conf.HTTPClient.Retry = "1ms"

	h, err := NewHTTPClient(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 3; i++ {
		select {
		case <-requestChan:
		case <-time.After(time.Second):
			t.Error("Timed out")
		}
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientGETNotExist(t *testing.T) {
	t.Parallel()

	conf := NewConfig()
	conf.HTTPClient.URL = "jgljksdfhjgkldfjglkf"
	conf.HTTPClient.Retry = "1ms"

	h, err := NewHTTPClient(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 500)

	h.CloseAsync()
	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientGETStreamNotExist(t *testing.T) {
	t.Parallel()

	conf := NewConfig()
	conf.HTTPClient.URL = "jgljksdfhjgkldfjglkf"
	conf.HTTPClient.Retry = "1ms"
	conf.HTTPClient.Stream.Enabled = true

	h, err := NewHTTPClient(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	<-time.After(time.Millisecond * 500)

	h.CloseAsync()
	if err := h.WaitForClose(time.Second * 5); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientGETStreamError(t *testing.T) {
	t.Parallel()

	requestChan := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nah", http.StatusBadGateway)
		select {
		case requestChan <- struct{}{}:
		default:
		}
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = ts.URL + "/testpost"
	conf.HTTPClient.Retry = "1ms"
	conf.HTTPClient.Stream.Enabled = true

	h, err := NewHTTPClient(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	select {
	case <-requestChan:
	case <-time.After(time.Second):
		t.Error("Timed out")
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second * 2); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientPOST(t *testing.T) {
	var reqCount uint32
	inputs := []string{
		"foo1",
		"foo2",
		"foo3",
		"foo4",
		"foo5",
	}

	index := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if exp, act := "POST", r.Method; exp != act {
			t.Errorf("Wrong method: %v != %v", act, exp)
		}
		defer r.Body.Close()

		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}

		if exp, act := "foobar", string(bodyBytes); exp != act {
			t.Errorf("Wrong post body: %v != %v", act, exp)
		}

		atomic.AddUint32(&reqCount, 1)
		w.Write([]byte(inputs[index%len(inputs)]))
		index++
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = ts.URL + "/testpost"
	conf.HTTPClient.Verb = "POST"
	conf.HTTPClient.Payload = "foobar"
	conf.HTTPClient.Retry = "1ms"

	h, err := NewHTTPClient(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	for _, expPart := range inputs {
		var ts types.Transaction
		var open bool

		select {
		case ts, open = <-h.TransactionChan():
			if !open {
				t.Fatal("Chan not open")
			}
			if exp, act := 1, ts.Payload.Len(); exp != act {
				t.Fatalf("Wrong count of parts: %v != %v", act, exp)
			}
			if exp, act := expPart, string(ts.Payload.Get(0).Get()); exp != act {
				t.Errorf("Wrong part: %v != %v", act, exp)
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}

		select {
		case ts.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	if exp, act := uint32(len(inputs)), atomic.LoadUint32(&reqCount); exp != act && exp+1 != act {
		t.Errorf("Wrong count of HTTP attempts: %v != %v", act, exp)
	}
}

func TestHTTPClientGETMultipart(t *testing.T) {
	var reqCount uint32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if exp, act := "GET", r.Method; exp != act {
			t.Errorf("Wrong method: %v != %v", act, exp)
		}
		atomic.AddUint32(&reqCount, 1)

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		parts := []string{
			"hello", "http", "world",
		}
		for _, p := range parts {
			var err error
			var part io.Writer
			if part, err = writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{"application/octet-stream"},
			}); err == nil {
				_, err = io.Copy(part, bytes.NewReader([]byte(p)))
			}
			if err != nil {
				t.Fatal(err)
			}
		}

		writer.Close()
		w.Header().Add("Content-Type", writer.FormDataContentType())
		w.Write(body.Bytes())
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = ts.URL + "/testpost"
	conf.HTTPClient.Retry = "1ms"

	h, err := NewHTTPClient(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	var tr types.Transaction
	var open bool

	select {
	case tr, open = <-h.TransactionChan():
		if !open {
			t.Fatal("Chan not open")
		}
		if exp, act := 3, tr.Payload.Len(); exp != act {
			t.Fatalf("Wrong count of parts: %v != %v", act, exp)
		}
		if exp, act := "hello", string(tr.Payload.Get(0).Get()); exp != act {
			t.Errorf("Wrong part: %v != %v", act, exp)
		}
		if exp, act := "http", string(tr.Payload.Get(1).Get()); exp != act {
			t.Errorf("Wrong part: %v != %v", act, exp)
		}
		if exp, act := "world", string(tr.Payload.Get(2).Get()); exp != act {
			t.Errorf("Wrong part: %v != %v", act, exp)
		}
	case <-time.After(time.Second):
		t.Errorf("Action timed out")
	}

	select {
	case tr.ResponseChan <- response.NewAck():
	case <-time.After(time.Second):
		t.Errorf("Action timed out")
	}
	h.CloseAsync()

	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}

	if exp, act := uint32(1), atomic.LoadUint32(&reqCount); exp != act && exp+1 != act {
		t.Errorf("Wrong count of HTTP attempts: %v != %v", act, exp)
	}
}

func TestHTTPClientGETMultipartLoop(t *testing.T) {
	tests := [][]string{
		{
			"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
			"Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
			"Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
		},
		{
			"Tristique et egestas quis ipsum suspendisse ultrices. Quis enim lobortis scelerisque fermentum dui faucibus.",
		},
		{
			"Lorem donec massa sapien faucibus et molestie ac. Lectus proin nibh nisl condimentum id venenatis a.",
			"Ultricies mi eget mauris pharetra et ultrices neque ornare aenean.",
		},
		{
			"Amet tellus cras adipiscing enim. Non pulvinar neque laoreet suspendisse interdum consectetur. Venenatis cras sed felis eget velit aliquet sagittis.",
			"Ac feugiat sed lectus vestibulum mattis ullamcorper velit. Phasellus vestibulum lorem sed risus ultricies tristique nulla aliquet.",
			"Odio ut sem nulla pharetra diam sit. Neque vitae tempus quam pellentesque nec nam aliquam sem.",
			"Scelerisque eu ultrices vitae auctor eu augue. Ut eu sem integer vitae justo eget. Purus in massa tempor nec feugiat nisl pretium fusce id.",
		},
	}

	var reqMut sync.Mutex

	var index int
	tserve := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqMut.Lock()
		defer reqMut.Unlock()

		if exp, act := "GET", r.Method; exp != act {
			t.Errorf("Wrong method: %v != %v", act, exp)
		}

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		parts := tests[index%len(tests)]
		for _, p := range parts {
			var err error
			var part io.Writer
			if part, err = writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{"application/octet-stream"},
			}); err == nil {
				_, err = io.Copy(part, bytes.NewReader([]byte(p)))
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		index++

		writer.Close()
		w.Header().Add("Content-Type", writer.FormDataContentType())
		w.Write(body.Bytes())
	}))
	defer tserve.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = tserve.URL + "/testpost"
	conf.HTTPClient.Retry = "1ms"

	h, err := NewHTTPClient(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	reqMut.Lock()
	for _, test := range tests {
		var ts types.Transaction
		var open bool

		reqMut.Unlock()
		select {
		case ts, open = <-h.TransactionChan():
			if !open {
				t.Fatal("Chan not open")
			}
			if exp, act := len(test), ts.Payload.Len(); exp != act {
				t.Fatalf("Wrong count of parts: %v != %v", act, exp)
			}
			for i, part := range test {
				if exp, act := part, string(ts.Payload.Get(i).Get()); exp != act {
					t.Errorf("Wrong part: %v != %v", act, exp)
				}
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}

		reqMut.Lock()
		select {
		case ts.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}
	}

	h.CloseAsync()
	reqMut.Unlock()

	select {
	case <-h.TransactionChan():
	case <-time.After(time.Second):
		t.Errorf("Action timed out")
	}

	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientStreamGETMultipartLoop(t *testing.T) {
	tests := [][]string{
		{
			"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
			"Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
			"Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
		},
		{
			"Tristique et egestas quis ipsum suspendisse ultrices. Quis enim lobortis scelerisque fermentum dui faucibus.",
		},
		{
			"Lorem donec massa sapien faucibus et molestie ac. Lectus proin nibh nisl condimentum id venenatis a.",
			"Ultricies mi eget mauris pharetra et ultrices neque ornare aenean.",
		},
		{
			"Amet tellus cras adipiscing enim. Non pulvinar neque laoreet suspendisse interdum consectetur. Venenatis cras sed felis eget velit aliquet sagittis.",
			"Ac feugiat sed lectus vestibulum mattis ullamcorper velit. Phasellus vestibulum lorem sed risus ultricies tristique nulla aliquet.",
			"Odio ut sem nulla pharetra diam sit. Neque vitae tempus quam pellentesque nec nam aliquam sem.",
			"Scelerisque eu ultrices vitae auctor eu augue. Ut eu sem integer vitae justo eget. Purus in massa tempor nec feugiat nisl pretium fusce id.",
		},
	}

	tserve := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if exp, act := "GET", r.Method; exp != act {
			t.Errorf("Wrong method: %v != %v", act, exp)
		}

		body := &bytes.Buffer{}

		for _, test := range tests {
			for _, part := range test {
				body.WriteString(part)
				body.WriteByte('\n')
			}
			body.WriteByte('\n')
		}
		body.WriteString("A msg that we won't read\nsecond part\n\n")

		w.Header().Add("Content-Type", "application/octet-stream")
		w.Write(body.Bytes())
	}))
	defer tserve.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = tserve.URL + "/testpost"
	conf.HTTPClient.Retry = "1ms"
	conf.HTTPClient.Stream.Codec = "lines/multipart"
	conf.HTTPClient.Stream.Enabled = true

	h, err := NewHTTPClient(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	for _, test := range tests {
		var ts types.Transaction
		var open bool

		select {
		case ts, open = <-h.TransactionChan():
			if !open {
				t.Fatal("Chan not open")
			}
			if exp, act := len(test), ts.Payload.Len(); exp != act {
				t.Fatalf("Wrong count of parts: %v != %v", act, exp)
			}
			for i, part := range test {
				if exp, act := part, string(ts.Payload.Get(i).Get()); exp != act {
					t.Errorf("Wrong part: %v != %v", act, exp)
				}
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}

		select {
		case ts.ResponseChan <- response.NewAck():
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
		}
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientStreamGETMultiRecover(t *testing.T) {
	msgs := [][]string{
		{"foo", "bar"},
		{"foo", "baz"},
	}

	tserve := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if exp, act := "GET", r.Method; exp != act {
			t.Errorf("Wrong method: %v != %v", act, exp)
		}

		body := &bytes.Buffer{}
		for _, msg := range msgs {
			for _, part := range msg {
				body.WriteString(part)
				body.WriteByte('\n')
			}
			body.WriteByte('\n')
		}

		w.Header().Add("Content-Type", "application/octet-stream")
		w.Write(body.Bytes())
	}))
	defer tserve.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = tserve.URL + "/testpost"
	conf.HTTPClient.Retry = "1ms"
	conf.HTTPClient.Stream.Enabled = true
	conf.HTTPClient.Stream.Codec = "lines/multipart"

	h, err := NewHTTPClient(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 10; i++ {
		for _, testMsg := range msgs {
			var ts types.Transaction
			var open bool
			select {
			case ts, open = <-h.TransactionChan():
				if !open {
					t.Fatal("Chan not open")
				}
				if exp, act := len(testMsg), ts.Payload.Len(); exp != act {
					t.Fatalf("Wrong count of parts: %v != %v", act, exp)
				}
				for j, part := range testMsg {
					if exp, act := part, string(ts.Payload.Get(j).Get()); exp != act {
						t.Errorf("Wrong part: %v != %v", act, exp)
					}
				}
			case <-time.After(time.Second):
				t.Errorf("Action timed out")
			}

			select {
			case ts.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				t.Errorf("Action timed out")
			}
		}
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientStreamGETRecover(t *testing.T) {
	msgs := []string{"foo", "bar"}

	tserve := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if exp, act := "GET", r.Method; exp != act {
			t.Errorf("Wrong method: %v != %v", act, exp)
		}

		body := &bytes.Buffer{}
		for _, msg := range msgs {
			body.WriteString(msg)
			body.WriteByte('\n')
		}

		w.Header().Add("Content-Type", "application/octet-stream")
		w.Write(body.Bytes())
	}))
	defer tserve.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = tserve.URL + "/testpost"
	conf.HTTPClient.Retry = "1ms"
	conf.HTTPClient.Stream.Enabled = true

	h, err := NewHTTPClient(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 10; i++ {
		for _, testMsg := range msgs {
			var ts types.Transaction
			var open bool
			select {
			case ts, open = <-h.TransactionChan():
				if !open {
					t.Fatal("Chan not open")
				}
				if exp, act := 1, ts.Payload.Len(); exp != act {
					t.Fatalf("Wrong count of parts: %v != %v", act, exp)
				}
				if exp, act := testMsg, string(ts.Payload.Get(0).Get()); exp != act {
					t.Errorf("Wrong part: %v != %v", act, exp)
				}
			case <-time.After(time.Second):
				t.Errorf("Action timed out")
			}

			select {
			case ts.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				t.Errorf("Action timed out")
			}
		}
	}

	h.CloseAsync()
	if err := h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientStreamGETTokenization(t *testing.T) {
	msgs := []string{`{"token":"foo"}`, `{"token":"bar"}`}

	var tokensLock sync.Mutex
	updateTokens := true
	tokens := []string{}

	tserve := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET", r.Method)

		tokensLock.Lock()
		if updateTokens {
			tokens = append(tokens, r.URL.Query().Get("token"))
		}
		tokensLock.Unlock()

		body := &bytes.Buffer{}
		for _, msg := range msgs {
			body.WriteString(msg)
			body.WriteByte('\n')
		}

		w.Header().Add("Content-Type", "application/octet-stream")
		w.Write(body.Bytes())
	}))
	defer tserve.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = tserve.URL + `/testpost?token=${!json("token")}`
	conf.HTTPClient.Retry = "1ms"
	conf.HTTPClient.Stream.Enabled = true

	h, err := NewHTTPClient(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		if i == 9 {
			tokensLock.Lock()
			updateTokens = false
			tokensLock.Unlock()
		}

		for _, testMsg := range msgs {
			var ts types.Transaction
			var open bool
			select {
			case ts, open = <-h.TransactionChan():
				require.True(t, open)
				require.Equal(t, 1, ts.Payload.Len())
				assert.Equal(t, testMsg, string(ts.Payload.Get(0).Get()))
			case <-time.After(time.Second):
				t.Errorf("Action timed out")
			}

			select {
			case ts.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				t.Errorf("Action timed out")
			}
		}
	}

	tokensLock.Lock()
	assert.Equal(t, []string{
		"null", "bar", "bar", "bar", "bar", "bar", "bar", "bar", "bar",
	}, tokens)
	tokensLock.Unlock()

	h.CloseAsync()
	require.NoError(t, h.WaitForClose(time.Second))
}

func BenchmarkHTTPClientGETMultipart(b *testing.B) {
	parts := []string{
		"Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
		"Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
		"Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
	}

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	for _, p := range parts {
		var err error
		var part io.Writer
		if part, err = writer.CreatePart(textproto.MIMEHeader{
			"Content-Type": []string{"application/octet-stream"},
		}); err == nil {
			_, err = io.Copy(part, bytes.NewReader([]byte(p)))
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	writer.Close()
	header := writer.FormDataContentType()

	tserve := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if exp, act := "GET", r.Method; exp != act {
			b.Errorf("Wrong method: %v != %v", act, exp)
		}

		w.Header().Add("Content-Type", header)
		w.Write(body.Bytes())
	}))
	defer tserve.Close()

	conf := NewConfig()
	conf.HTTPClient.URL = tserve.URL + "/testpost"
	conf.HTTPClient.Retry = "1ms"

	h, err := NewHTTPClient(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		b.Error(err)
		return
	}

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		ts, open := <-h.TransactionChan()
		if !open {
			b.Fatal("Chan not open")
		}
		if exp, act := 3, ts.Payload.Len(); exp != act {
			b.Fatalf("Wrong count of parts: %v != %v", act, exp)
		}
		for i, part := range parts {
			if exp, act := part, string(ts.Payload.Get(i).Get()); exp != act {
				b.Errorf("Wrong part: %v != %v", act, exp)
			}
		}
		ts.ResponseChan <- response.NewAck()
	}

	b.StopTimer()

	h.CloseAsync()
	if err := h.WaitForClose(time.Second); err != nil {
		b.Error(err)
	}
}
