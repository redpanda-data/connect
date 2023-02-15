package io

import (
	"context"
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/transaction"
)

func parseYAMLOutputConf(t testing.TB, formatStr string, args ...any) (conf output.Config) {
	t.Helper()
	conf = output.NewConfig()
	require.NoError(t, yaml.Unmarshal(fmt.Appendf(nil, formatStr, args...), &conf))
	return
}

func TestHTTPClientMultipartEnabled(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	resultChan := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(mediaType, "multipart/"))

		mr := multipart.NewReader(r.Body, params["boundary"])
		for {
			p, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)

			msgBytes, err := io.ReadAll(p)
			require.NoError(t, err)

			resultChan <- string(msgBytes)
		}
	}))
	defer ts.Close()

	conf := parseYAMLOutputConf(t, `
http_client:
  url: %v/testpost
  batch_as_multipart: true
`, ts.URL)

	h, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	tChan := make(chan message.Transaction)
	require.NoError(t, h.Consume(tChan))

	resChan := make(chan error)
	select {
	case tChan <- message.NewTransaction(message.QuickBatch([][]byte{
		[]byte("PART-A"),
		[]byte("PART-B"),
		[]byte("PART-C"),
	}), resChan):
	case <-time.After(time.Second):
		t.Fatal("Action timed out")
	}

	for _, exp := range []string{
		"PART-A",
		"PART-B",
		"PART-C",
	} {
		select {
		case resMsg := <-resultChan:
			assert.Equal(t, exp, resMsg)
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}

	select {
	case res := <-resChan:
		assert.NoError(t, res)
	case <-time.After(time.Second):
		t.Fatal("Action timed out")
	}

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))
}

func TestHTTPClientMultipartDisabled(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	resultChan := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resBytes, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		resultChan <- string(resBytes)
	}))
	defer ts.Close()

	conf := parseYAMLOutputConf(t, `
http_client:
  url: %v/testpost
  max_in_flight: 1
`, ts.URL)

	h, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	tChan := make(chan message.Transaction)
	require.NoError(t, h.Consume(tChan))

	resChan := make(chan error)
	select {
	case tChan <- message.NewTransaction(message.QuickBatch([][]byte{
		[]byte("PART-A"),
		[]byte("PART-B"),
		[]byte("PART-C"),
	}), resChan):
	case <-time.After(time.Second):
		t.Fatal("Action timed out")
	}

	for _, exp := range []string{
		"PART-A",
		"PART-B",
		"PART-C",
	} {
		select {
		case resMsg := <-resultChan:
			assert.Equal(t, exp, resMsg)
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}

	select {
	case res := <-resChan:
		assert.NoError(t, res)
	case <-time.After(time.Second):
		t.Fatal("Action timed out")
	}

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))
}

func writeBatchToStreamed(ctx context.Context, t *testing.T, batch message.Batch, out output.Streamed) (err error) {
	t.Helper()

	tChan := make(chan message.Transaction)
	require.NoError(t, out.Consume(tChan))

	return writeBatchToChan(ctx, t, batch, tChan)
}

func writeBatchToChan(ctx context.Context, t *testing.T, batch message.Batch, tChan chan message.Transaction) (err error) {
	t.Helper()

	resChan := make(chan error)
	select {
	case tChan <- message.NewTransaction(batch, resChan):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	select {
	case err = <-resChan:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return
}

func TestHTTPClientRetries(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	var reqCount uint32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint32(&reqCount, 1)
		http.Error(w, "test error", http.StatusForbidden)
	}))
	defer ts.Close()

	conf := parseYAMLOutputConf(t, `
http_client:
  url: %v/testpost
  retry_period: 1ms
  retries: 3
`, ts.URL)

	h, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	require.Error(t, writeBatchToStreamed(ctx, t, message.QuickBatch([][]byte{[]byte("test")}), h))
	if exp, act := uint32(4), atomic.LoadUint32(&reqCount); exp != act {
		t.Errorf("Wrong count of HTTP attempts: %v != %v", exp, act)
	}

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))
}

func TestHTTPClientBasic(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	nTestLoops := 1000

	resultChan := make(chan message.Batch, 1)
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
		msg = append(msg, message.NewPart(b))
	}))
	defer ts.Close()

	conf := parseYAMLOutputConf(t, `
http_client:
  url: %v/testpost
`, ts.URL)

	h, err := mock.NewManager().NewOutput(conf)
	if err != nil {
		t.Fatal(err)
	}

	tChan := make(chan message.Transaction)
	require.NoError(t, h.Consume(tChan))

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.QuickBatch([][]byte{[]byte(testStr)})

		if err = writeBatchToChan(ctx, t, testMsg, tChan); err != nil {
			t.Error(err)
		}

		select {
		case resMsg := <-resultChan:
			if resMsg.Len() != 1 {
				t.Errorf("Wrong # parts: %v != %v", resMsg.Len(), 1)
				return
			}
			if exp, actual := testStr, string(resMsg.Get(0).AsBytes()); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))
}

func TestHTTPClientSyncResponse(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	nTestLoops := 1000

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		w.Header().Add("fooheader", "foovalue")
		_, _ = w.Write([]byte("echo: "))
		_, _ = w.Write(b)
	}))
	defer ts.Close()

	conf := parseYAMLOutputConf(t, `
http_client:
  url: %v/testpost
  propagate_response: true
`, ts.URL)

	h, err := mock.NewManager().NewOutput(conf)
	if err != nil {
		t.Fatal(err)
	}

	tChan := make(chan message.Transaction)
	require.NoError(t, h.Consume(tChan))

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)

		resultStore := transaction.NewResultStore()
		testMsg := message.QuickBatch([][]byte{[]byte(testStr)})
		transaction.AddResultStore(testMsg, resultStore)

		require.NoError(t, writeBatchToChan(ctx, t, testMsg, tChan))
		resMsgs := resultStore.Get()
		require.Len(t, resMsgs, 1)

		resMsg := resMsgs[0]
		require.Equal(t, 1, resMsg.Len())
		assert.Equal(t, "echo: "+testStr, string(resMsg.Get(0).AsBytes()))
		assert.Equal(t, "", resMsg.Get(0).MetaGetStr("fooheader"))
	}

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))
}

func TestHTTPClientSyncResponseCopyHeaders(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	nTestLoops := 1000

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		w.Header().Add("fooheader", "foovalue")
		_, _ = w.Write([]byte("echo: "))
		_, _ = w.Write(b)
	}))
	defer ts.Close()

	conf := parseYAMLOutputConf(t, `
http_client:
  url: %v/testpost
  propagate_response: true
  extract_headers:
    include_patterns: [ ".*" ]
`, ts.URL)

	h, err := mock.NewManager().NewOutput(conf)
	if err != nil {
		t.Fatal(err)
	}

	tChan := make(chan message.Transaction)
	require.NoError(t, h.Consume(tChan))

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)

		resultStore := transaction.NewResultStore()
		testMsg := message.QuickBatch([][]byte{[]byte(testStr)})
		transaction.AddResultStore(testMsg, resultStore)

		require.NoError(t, writeBatchToChan(ctx, t, testMsg, tChan))
		resMsgs := resultStore.Get()
		require.Len(t, resMsgs, 1)

		resMsg := resMsgs[0]
		require.Equal(t, 1, resMsg.Len())
		assert.Equal(t, "echo: "+testStr, string(resMsg.Get(0).AsBytes()))
		assert.Equal(t, "foovalue", resMsg.Get(0).MetaGetStr("fooheader"))
	}

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))
}

func TestHTTPClientMultipart(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	nTestLoops := 1000

	resultChan := make(chan message.Batch, 1)
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
				msg = append(msg, message.NewPart(msgBytes))
			}
		} else {
			b, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			msg = append(msg, message.NewPart(b))
		}
	}))
	defer ts.Close()

	conf := parseYAMLOutputConf(t, `
http_client:
  url: %v/testpost
  batch_as_multipart: true
`, ts.URL)

	h, err := mock.NewManager().NewOutput(conf)
	if err != nil {
		t.Fatal(err)
	}

	tChan := make(chan message.Transaction)
	require.NoError(t, h.Consume(tChan))

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.QuickBatch([][]byte{
			[]byte(testStr + "PART-A"),
			[]byte(testStr + "PART-B"),
		})

		require.NoError(t, writeBatchToChan(ctx, t, testMsg, tChan))

		select {
		case resMsg := <-resultChan:
			if resMsg.Len() != 2 {
				t.Errorf("Wrong # parts: %v != %v", resMsg.Len(), 2)
				return
			}
			if exp, actual := testStr+"PART-A", string(resMsg.Get(0).AsBytes()); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
			if exp, actual := testStr+"PART-B", string(resMsg.Get(1).AsBytes()); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))
}

func TestHTTPOutputClientMultipartBody(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	nTestLoops := 1000
	resultChan := make(chan message.Batch, 1)
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
				msg = append(msg, message.NewPart(msgBytes))
			}
		}
	}))
	defer ts.Close()

	conf := parseYAMLOutputConf(t, `
http_client:
  url: %v/testpost
  multipart:
    - content_disposition: 'form-data; name="text"'
      content_type: 'text/plain'
      body: PART-A
    - content_disposition: 'form-data; name="file1"; filename="a.txt"'
      content_type: 'text/plain'
      body: PART-B
`, ts.URL)

	h, err := mock.NewManager().NewOutput(conf)
	if err != nil {
		t.Fatal(err)
	}

	tChan := make(chan message.Transaction)
	require.NoError(t, h.Consume(tChan))

	for i := 0; i < nTestLoops; i++ {
		require.NoError(t, writeBatchToChan(ctx, t, message.QuickBatch([][]byte{[]byte("test")}), tChan))

		select {
		case resMsg := <-resultChan:
			if resMsg.Len() != 2 {
				t.Errorf("Wrong # parts: %v != %v", resMsg.Len(), 2)
				return
			}
			if exp, actual := "PART-A", string(resMsg.Get(0).AsBytes()); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
			if exp, actual := "PART-B", string(resMsg.Get(1).AsBytes()); exp != actual {
				t.Errorf("Wrong result, %v != %v", exp, actual)
				return
			}
		case <-time.After(time.Second):
			t.Errorf("Action timed out")
			return
		}
	}

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))
}

func TestHTTPOutputClientMultipartHeaders(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	resultChan := make(chan message.Batch, 1)
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
				msg = append(msg, message.NewPart(a))
			}
		}
	}))
	defer ts.Close()

	conf := parseYAMLOutputConf(t, `
http_client:
  url: %v/testpost
  multipart:
    - content_disposition: 'form-data; name="text"'
      content_type: 'text/plain'
      body: PART-A
    - content_disposition: 'form-data; name="file1"; filename="a.txt"'
      content_type: 'text/plain'
      body: PART-B
`, ts.URL)

	h, err := mock.NewManager().NewOutput(conf)
	if err != nil {
		t.Fatal(err)
	}

	require.NoError(t, writeBatchToStreamed(ctx, t, message.QuickBatch([][]byte{[]byte("test")}), h))

	expHeaders := [][2]string{
		{`form-data; name="text"`, "text/plain"},
		{`form-data; name="file1"; filename="a.txt"`, "text/plain"},
	}

	select {
	case resMsg := <-resultChan:
		for i, exp := range expHeaders {
			if resMsg.Len() != 2 {
				t.Errorf("Wrong # parts: %v != %v", resMsg.Len(), 2)
				return
			}
			mp := make(map[string][]string)
			err := json.Unmarshal(resMsg.Get(i).AsBytes(), &mp)
			if err != nil {
				t.Error(err)
			}
			assert.Equal(t, []string{exp[0]}, mp["Content-Disposition"])
			assert.Equal(t, []string{exp[1]}, mp["Content-Type"])
		}
	case <-time.After(time.Second):
		t.Errorf("Action timed out")
		return
	}

	h.TriggerCloseNow()
	require.NoError(t, h.WaitForClose(ctx))
}
