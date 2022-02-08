package writer

import (
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/roundtrip"
	"github.com/Jeffail/benthos/v3/lib/metrics"
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

	conf := NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"
	conf.Retry = "1ms"
	conf.NumRetries = 3

	h, err := NewHTTPClient(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = h.Write(message.QuickBatch([][]byte{[]byte("test")})); err == nil {
		t.Error("Expected error from end of retries")
	}

	if exp, act := uint32(4), atomic.LoadUint32(&reqCount); exp != act {
		t.Errorf("Wrong count of HTTP attempts: %v != %v", exp, act)
	}

	h.CloseAsync()
	if err = h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientBasic(t *testing.T) {
	nTestLoops := 1000

	resultChan := make(chan *message.Batch, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := message.QuickBatch(nil)
		defer func() {
			resultChan <- msg
		}()

		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		msg.Append(message.NewPart(b))
	}))
	defer ts.Close()

	conf := NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := NewHTTPClient(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.QuickBatch([][]byte{[]byte(testStr)})

		if err = h.Write(testMsg); err != nil {
			t.Error(err)
		}

		select {
		case resMsg := <-resultChan:
			if resMsg.Len() != 1 {
				t.Errorf("Wrong # parts: %v != %v", resMsg.Len(), 1)
				return
			}
			if exp, actual := testStr, string(resMsg.Get(0).Get()); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}

	h.CloseAsync()
	if err = h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientSyncResponse(t *testing.T) {
	nTestLoops := 1000

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		w.Header().Add("fooheader", "foovalue")
		w.Write([]byte("echo: "))
		w.Write(b)
	}))
	defer ts.Close()

	conf := NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"
	conf.PropagateResponse = true

	h, err := NewHTTPClient(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)

		resultStore := roundtrip.NewResultStore()
		testMsg := message.QuickBatch([][]byte{[]byte(testStr)})
		roundtrip.AddResultStore(testMsg, resultStore)

		require.NoError(t, h.Write(testMsg))
		resMsgs := resultStore.Get()
		require.Len(t, resMsgs, 1)

		resMsg := resMsgs[0]
		require.Equal(t, 1, resMsg.Len())
		assert.Equal(t, "echo: "+testStr, string(resMsg.Get(0).Get()))
		assert.Equal(t, "", resMsg.Get(0).MetaGet("fooheader"))
	}

	h.CloseAsync()
	if err = h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientSyncResponseCopyHeaders(t *testing.T) {
	nTestLoops := 1000

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		w.Header().Add("fooheader", "foovalue")
		w.Write([]byte("echo: "))
		w.Write(b)
	}))
	defer ts.Close()

	conf := NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"
	conf.PropagateResponse = true
	conf.CopyResponseHeaders = true

	h, err := NewHTTPClient(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)

		resultStore := roundtrip.NewResultStore()
		testMsg := message.QuickBatch([][]byte{[]byte(testStr)})
		roundtrip.AddResultStore(testMsg, resultStore)

		require.NoError(t, h.Write(testMsg))
		resMsgs := resultStore.Get()
		require.Len(t, resMsgs, 1)

		resMsg := resMsgs[0]
		require.Equal(t, 1, resMsg.Len())
		assert.Equal(t, "echo: "+testStr, string(resMsg.Get(0).Get()))
		assert.Equal(t, "foovalue", resMsg.Get(0).MetaGet("fooheader"))
	}

	h.CloseAsync()
	if err = h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPClientMultipart(t *testing.T) {
	nTestLoops := 1000

	resultChan := make(chan *message.Batch, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := message.QuickBatch(nil)
		defer func() {
			resultChan <- msg
		}()

		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			t.Errorf("Bad media type: %v -> %v", r.Header.Get("Content-Type"), err)
			return
		}

		if strings.HasPrefix(mediaType, "multipart/") {
			mr := multipart.NewReader(r.Body, params["boundary"])
			for {
				p, err := mr.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Error(err)
					return
				}
				msgBytes, err := io.ReadAll(p)
				if err != nil {
					t.Error(err)
					return
				}
				msg.Append(message.NewPart(msgBytes))
			}
		} else {
			b, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			msg.Append(message.NewPart(b))
		}
	}))
	defer ts.Close()

	conf := NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := NewHTTPClient(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.QuickBatch([][]byte{
			[]byte(testStr + "PART-A"),
			[]byte(testStr + "PART-B"),
		})

		if err = h.Write(testMsg); err != nil {
			t.Error(err)
		}

		select {
		case resMsg := <-resultChan:
			if resMsg.Len() != 2 {
				t.Errorf("Wrong # parts: %v != %v", resMsg.Len(), 2)
				return
			}
			if exp, actual := testStr+"PART-A", string(resMsg.Get(0).Get()); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
			if exp, actual := testStr+"PART-B", string(resMsg.Get(1).Get()); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}

	h.CloseAsync()
	if err = h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}
func TestHTTPOutputClientMultipartBody(t *testing.T) {
	nTestLoops := 1000
	resultChan := make(chan *message.Batch, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := message.QuickBatch(nil)
		defer func() {
			resultChan <- msg
		}()

		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			t.Errorf("Bad media type: %v -> %v", r.Header.Get("Content-Type"), err)
			return
		}

		if strings.HasPrefix(mediaType, "multipart/") {
			mr := multipart.NewReader(r.Body, params["boundary"])
			for {
				p, err := mr.NextPart()

				if err == io.EOF {
					break
				}
				if err != nil {
					t.Error(err)
					return
				}
				msgBytes, err := io.ReadAll(p)
				if err != nil {
					t.Error(err)
					return
				}
				msg.Append(message.NewPart(msgBytes))
			}
		}
	}))
	defer ts.Close()

	conf := NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"
	conf.Multipart = []HTTPClientMultipartExpression{
		{
			ContentDisposition: `form-data; name="text"`,
			ContentType:        "text/plain",
			Body:               "PART-A"},
		{
			ContentDisposition: `form-data; name="file1"; filename="a.txt"`,
			ContentType:        "text/plain",
			Body:               "PART-B"},
	}
	h, err := NewHTTPClient(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < nTestLoops; i++ {
		if err = h.Write(message.QuickBatch([][]byte{[]byte("test")})); err != nil {
			t.Error(err)
		}
		select {
		case resMsg := <-resultChan:
			if resMsg.Len() != len(conf.Multipart) {
				t.Errorf("Wrong # parts: %v != %v", resMsg.Len(), 2)
				return
			}
			if exp, actual := "PART-A", string(resMsg.Get(0).Get()); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
			if exp, actual := "PART-B", string(resMsg.Get(1).Get()); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}

	h.CloseAsync()
	if err = h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestHTTPOutputClientMultipartHeaders(t *testing.T) {
	resultChan := make(chan *message.Batch, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := message.QuickBatch(nil)
		defer func() {
			resultChan <- msg
		}()

		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			t.Errorf("Bad media type: %v -> %v", r.Header.Get("Content-Type"), err)
			return
		}

		if strings.HasPrefix(mediaType, "multipart/") {
			mr := multipart.NewReader(r.Body, params["boundary"])
			for {
				p, err := mr.NextPart()

				if err == io.EOF {
					break
				}
				if err != nil {
					t.Error(err)
					return
				}
				a, err := json.Marshal(p.Header)
				if err != nil {
					t.Error(err)
					return
				}
				msg.Append(message.NewPart(a))
			}
		}
	}))
	defer ts.Close()

	conf := NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"
	conf.Multipart = []HTTPClientMultipartExpression{
		{
			ContentDisposition: `form-data; name="text"`,
			ContentType:        "text/plain",
			Body:               "PART-A"},
		{
			ContentDisposition: `form-data; name="file1"; filename="a.txt"`,
			ContentType:        "text/plain",
			Body:               "PART-B"},
	}
	h, err := NewHTTPClient(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Write(message.QuickBatch([][]byte{[]byte("test")})); err != nil {
		t.Error(err)
	}
	select {
	case resMsg := <-resultChan:
		for i := range conf.Multipart {
			if resMsg.Len() != len(conf.Multipart) {
				t.Errorf("Wrong # parts: %v != %v", resMsg.Len(), 2)
				return
			}
			mp := make(map[string][]string)
			err := json.Unmarshal(resMsg.Get(i).Get(), &mp)
			if err != nil {
				t.Error(err)
			}
			if exp, actual := conf.Multipart[i].ContentDisposition, mp["Content-Disposition"]; exp != actual[0] {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
			if exp, actual := conf.Multipart[i].ContentType, mp["Content-Type"]; exp != actual[0] {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
		}
	case <-time.After(time.Second):
		t.Errorf("Action timed out")
		return

	}
	h.CloseAsync()
	if err = h.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}
