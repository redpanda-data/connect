package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/http/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//------------------------------------------------------------------------------

func TestHTTPClientRetries(t *testing.T) {
	var reqCount uint32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint32(&reqCount, 1)
		http.Error(w, "test error", http.StatusForbidden)
	}))
	defer ts.Close()

	conf := client.NewConfig()
	conf.URL = ts.URL + "/testpost"
	conf.Retry = "1ms"
	conf.NumRetries = 3

	h, err := NewClient(conf)
	require.NoError(t, err)
	defer h.Close(context.Background())

	out := message.New([][]byte{[]byte("test")})
	_, err = h.Send(context.Background(), out, out)
	assert.Error(t, err)
	assert.Equal(t, uint32(4), atomic.LoadUint32(&reqCount))
}

func TestHTTPClientBadRequest(t *testing.T) {
	conf := client.NewConfig()
	conf.URL = "htp://notvalid:1111"
	conf.Verb = "notvalid\n"
	conf.NumRetries = 3

	h, err := NewClient(conf)
	require.NoError(t, err)

	out := message.New([][]byte{[]byte("test")})
	_, err = h.Send(context.Background(), out, out)
	assert.Error(t, err)
}

func TestHTTPClientSendBasic(t *testing.T) {
	nTestLoops := 1000

	resultChan := make(chan types.Message, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := message.New(nil)
		defer func() {
			resultChan <- msg
		}()

		b, err := ioutil.ReadAll(r.Body)
		require.NoError(t, err)

		msg.Append(message.NewPart(b))
	}))
	defer ts.Close()

	conf := client.NewConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := NewClient(conf)
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.New([][]byte{[]byte(testStr)})

		_, err = h.Send(context.Background(), testMsg, testMsg)
		require.NoError(t, err)

		select {
		case resMsg := <-resultChan:
			require.Equal(t, 1, resMsg.Len())
			assert.Equal(t, testStr, string(resMsg.Get(0).Get()))
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}
}

func TestHTTPClientBadContentType(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
		require.NoError(t, err)

		_, err = w.Write(bytes.ToUpper(b))
		require.NoError(t, err)
	}))
	t.Cleanup(ts.Close)

	conf := client.NewConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := NewClient(conf)
	require.NoError(t, err)

	testMsg := message.New([][]byte{[]byte("hello world")})

	res, err := h.Send(context.Background(), testMsg, testMsg)
	require.NoError(t, err)

	require.Equal(t, 1, res.Len())
	assert.Equal(t, "HELLO WORLD", string(res.Get(0).Get()))
}

func TestHTTPClientDropOn(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"foo":"bar"}`))
	}))
	defer ts.Close()

	conf := client.NewConfig()
	conf.URL = ts.URL + "/testpost"
	conf.DropOn = []int{400}

	h, err := NewClient(conf)
	require.NoError(t, err)

	testMsg := message.New([][]byte{[]byte(`{"bar":"baz"}`)})

	_, err = h.Send(context.Background(), testMsg, testMsg)
	require.Error(t, err)
}

func TestHTTPClientSuccessfulOn(t *testing.T) {
	var reqs int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"foo":"bar"}`))
		atomic.AddInt32(&reqs, 1)
	}))
	defer ts.Close()

	conf := client.NewConfig()
	conf.URL = ts.URL + "/testpost"
	conf.SuccessfulOn = []int{400}

	h, err := NewClient(conf)
	require.NoError(t, err)

	testMsg := message.New([][]byte{[]byte(`{"bar":"baz"}`)})
	resMsg, err := h.Send(context.Background(), testMsg, testMsg)
	require.NoError(t, err)

	assert.Equal(t, `{"foo":"bar"}`, string(resMsg.Get(0).Get()))
	assert.Equal(t, int32(1), atomic.LoadInt32(&reqs))
}

func TestHTTPClientSendInterpolate(t *testing.T) {
	nTestLoops := 1000

	resultChan := make(chan types.Message, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/firstvar", r.URL.Path)
		assert.Equal(t, "hdr-secondvar", r.Header.Get("dynamic"))
		assert.Equal(t, "foo", r.Header.Get("static"))
		assert.Equal(t, "simpleHost.com", r.Host)

		msg := message.New(nil)
		defer func() {
			resultChan <- msg
		}()

		b, err := ioutil.ReadAll(r.Body)
		require.NoError(t, err)

		msg.Append(message.NewPart(b))
	}))
	defer ts.Close()

	conf := client.NewConfig()
	conf.URL = ts.URL + `/${! json("foo.bar") }`
	conf.Headers["static"] = "foo"
	conf.Headers["dynamic"] = `hdr-${!json("foo.baz")}`
	conf.Headers["Host"] = "simpleHost.com"

	h, err := NewClient(conf, OptSetLogger(log.Noop()), OptSetStats(metrics.Noop()))
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf(`{"test":%v,"foo":{"bar":"firstvar","baz":"secondvar"}}`, i)
		testMsg := message.New([][]byte{[]byte(testStr)})

		_, err = h.Send(context.Background(), testMsg, testMsg)
		require.NoError(t, err)

		select {
		case resMsg := <-resultChan:
			require.Equal(t, 1, resMsg.Len())
			assert.Equal(t, testStr, string(resMsg.Get(0).Get()))
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}
}

func TestHTTPClientSendMultipart(t *testing.T) {
	nTestLoops := 1000

	resultChan := make(chan types.Message, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := message.New(nil)
		defer func() {
			resultChan <- msg
		}()

		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		require.NoError(t, err)

		if strings.HasPrefix(mediaType, "multipart/") {
			mr := multipart.NewReader(r.Body, params["boundary"])
			for {
				p, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)

				msgBytes, err := ioutil.ReadAll(p)
				require.NoError(t, err)

				msg.Append(message.NewPart(msgBytes))
			}
		} else {
			b, err := ioutil.ReadAll(r.Body)
			require.NoError(t, err)

			msg.Append(message.NewPart(b))
		}
	}))
	defer ts.Close()

	conf := client.NewConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := NewClient(conf)
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.New([][]byte{
			[]byte(testStr + "PART-A"),
			[]byte(testStr + "PART-B"),
		})

		_, err = h.Send(context.Background(), testMsg, testMsg)
		require.NoError(t, err)

		select {
		case resMsg := <-resultChan:
			assert.Equal(t, 2, resMsg.Len())
			assert.Equal(t, testStr+"PART-A", string(resMsg.Get(0).Get()))
			assert.Equal(t, testStr+"PART-B", string(resMsg.Get(1).Get()))
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}
}

func TestHTTPClientReceive(t *testing.T) {
	nTestLoops := 1000

	j := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testStr := fmt.Sprintf("test%v", j)
		j++
		w.Header().Set("foo-bar", "baz-0")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(testStr + "PART-A"))
	}))
	defer ts.Close()

	conf := client.NewConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := NewClient(conf)
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", j)
		resMsg, err := h.Send(context.Background(), nil, nil)
		require.NoError(t, err)

		assert.Equal(t, 1, resMsg.Len())
		assert.Equal(t, testStr+"PART-A", string(resMsg.Get(0).Get()))
		assert.Equal(t, "", resMsg.Get(0).Metadata().Get("foo-bar"))
		assert.Equal(t, "201", resMsg.Get(0).Metadata().Get("http_status_code"))
	}
}

func TestHTTPClientReceiveHeaders(t *testing.T) {
	nTestLoops := 1000

	j := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testStr := fmt.Sprintf("test%v", j)
		j++
		w.Header().Set("foo-bar", "baz-0")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(testStr + "PART-A"))
	}))
	defer ts.Close()

	conf := client.NewConfig()
	conf.URL = ts.URL + "/testpost"
	conf.CopyResponseHeaders = true

	h, err := NewClient(conf)
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", j)
		resMsg, err := h.Send(context.Background(), nil, nil)
		require.NoError(t, err)

		assert.Equal(t, 1, resMsg.Len())
		assert.Equal(t, testStr+"PART-A", string(resMsg.Get(0).Get()))
		assert.Equal(t, "baz-0", resMsg.Get(0).Metadata().Get("foo-bar"))
		assert.Equal(t, "201", resMsg.Get(0).Metadata().Get("http_status_code"))
	}
}

func TestHTTPClientReceiveMultipart(t *testing.T) {
	nTestLoops := 1000

	j := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testStr := fmt.Sprintf("test%v", j)
		j++
		msg := message.New([][]byte{
			[]byte(testStr + "PART-A"),
			[]byte(testStr + "PART-B"),
		})

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for i := 0; i < msg.Len(); i++ {
			part, err := writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{"application/octet-stream"},
				"foo-bar":      []string{"baz-" + strconv.Itoa(i), "ignored"},
			})
			require.NoError(t, err)

			_, err = io.Copy(part, bytes.NewReader(msg.Get(i).Get()))
			require.NoError(t, err)
		}
		writer.Close()

		w.Header().Add("Content-Type", writer.FormDataContentType())
		w.WriteHeader(http.StatusCreated)
		w.Write(body.Bytes())
	}))
	defer ts.Close()

	conf := client.NewConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := NewClient(conf)
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", j)
		resMsg, err := h.Send(context.Background(), nil, nil)
		require.NoError(t, err)

		assert.Equal(t, 2, resMsg.Len())
		assert.Equal(t, testStr+"PART-A", string(resMsg.Get(0).Get()))
		assert.Equal(t, testStr+"PART-B", string(resMsg.Get(1).Get()))
		assert.Equal(t, "", resMsg.Get(0).Metadata().Get("foo-bar"))
		assert.Equal(t, "201", resMsg.Get(0).Metadata().Get("http_status_code"))
		assert.Equal(t, "", resMsg.Get(1).Metadata().Get("foo-bar"))
		assert.Equal(t, "201", resMsg.Get(1).Metadata().Get("http_status_code"))
	}
}

func TestHTTPClientReceiveMultipartWithHeaders(t *testing.T) {
	nTestLoops := 1000

	j := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testStr := fmt.Sprintf("test%v", j)
		j++
		msg := message.New([][]byte{
			[]byte(testStr + "PART-A"),
			[]byte(testStr + "PART-B"),
		})

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for i := 0; i < msg.Len(); i++ {
			part, err := writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{"application/octet-stream"},
				"foo-bar":      []string{"baz-" + strconv.Itoa(i), "ignored"},
			})
			require.NoError(t, err)

			_, err = io.Copy(part, bytes.NewReader(msg.Get(i).Get()))
			require.NoError(t, err)
		}
		writer.Close()

		w.Header().Add("Content-Type", writer.FormDataContentType())
		w.WriteHeader(http.StatusCreated)
		w.Write(body.Bytes())
	}))
	defer ts.Close()

	conf := client.NewConfig()
	conf.URL = ts.URL + "/testpost"
	conf.CopyResponseHeaders = true

	h, err := NewClient(conf)
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", j)
		resMsg, err := h.Send(context.Background(), nil, nil)
		require.NoError(t, err)

		assert.Equal(t, 2, resMsg.Len())
		assert.Equal(t, testStr+"PART-A", string(resMsg.Get(0).Get()))
		assert.Equal(t, testStr+"PART-B", string(resMsg.Get(1).Get()))
		assert.Equal(t, "baz-0", resMsg.Get(0).Metadata().Get("foo-bar"))
		assert.Equal(t, "201", resMsg.Get(0).Metadata().Get("http_status_code"))
		assert.Equal(t, "baz-1", resMsg.Get(1).Metadata().Get("foo-bar"))
		assert.Equal(t, "201", resMsg.Get(1).Metadata().Get("http_status_code"))
	}
}
