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

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/transaction"
)

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

	conf := output.NewConfig()
	conf.Type = "http_client"
	conf.HTTPClient.BatchAsMultipart = true
	conf.HTTPClient.URL = ts.URL + "/testpost"

	h, err := newHTTPClientOutput(conf, mock.NewManager())
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

	conf := output.NewConfig()
	conf.Type = "http_client"
	conf.HTTPClient.URL = ts.URL + "/testpost"
	conf.HTTPClient.BatchAsMultipart = false
	conf.HTTPClient.MaxInFlight = 1

	h, err := newHTTPClientOutput(conf, mock.NewManager())
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

func TestHTTPClientRetries(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	var reqCount uint32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint32(&reqCount, 1)
		http.Error(w, "test error", http.StatusForbidden)
	}))
	defer ts.Close()

	conf := output.NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"
	conf.Retry = "1ms"
	conf.NumRetries = 3

	h, err := newHTTPClientWriter(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	if err = h.WriteBatch(context.Background(), message.QuickBatch([][]byte{[]byte("test")})); err == nil {
		t.Error("Expected error from end of retries")
	}

	if exp, act := uint32(4), atomic.LoadUint32(&reqCount); exp != act {
		t.Errorf("Wrong count of HTTP attempts: %v != %v", exp, act)
	}

	require.NoError(t, h.Close(ctx))
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

	conf := output.NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := newHTTPClientWriter(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.QuickBatch([][]byte{[]byte(testStr)})

		if err = h.WriteBatch(context.Background(), testMsg); err != nil {
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

	require.NoError(t, h.Close(ctx))
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

	conf := output.NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"
	conf.PropagateResponse = true

	h, err := newHTTPClientWriter(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)

		resultStore := transaction.NewResultStore()
		testMsg := message.QuickBatch([][]byte{[]byte(testStr)})
		transaction.AddResultStore(testMsg, resultStore)

		require.NoError(t, h.WriteBatch(context.Background(), testMsg))
		resMsgs := resultStore.Get()
		require.Len(t, resMsgs, 1)

		resMsg := resMsgs[0]
		require.Equal(t, 1, resMsg.Len())
		assert.Equal(t, "echo: "+testStr, string(resMsg.Get(0).AsBytes()))
		assert.Equal(t, "", resMsg.Get(0).MetaGetStr("fooheader"))
	}

	require.NoError(t, h.Close(ctx))
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

	conf := output.NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"
	conf.PropagateResponse = true
	conf.ExtractMetadata.IncludePatterns = []string{".*"}

	h, err := newHTTPClientWriter(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)

		resultStore := transaction.NewResultStore()
		testMsg := message.QuickBatch([][]byte{[]byte(testStr)})
		transaction.AddResultStore(testMsg, resultStore)

		require.NoError(t, h.WriteBatch(context.Background(), testMsg))
		resMsgs := resultStore.Get()
		require.Len(t, resMsgs, 1)

		resMsg := resMsgs[0]
		require.Equal(t, 1, resMsg.Len())
		assert.Equal(t, "echo: "+testStr, string(resMsg.Get(0).AsBytes()))
		assert.Equal(t, "foovalue", resMsg.Get(0).MetaGetStr("fooheader"))
	}

	require.NoError(t, h.Close(ctx))
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

	conf := output.NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := newHTTPClientWriter(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.QuickBatch([][]byte{
			[]byte(testStr + "PART-A"),
			[]byte(testStr + "PART-B"),
		})

		if err = h.WriteBatch(context.Background(), testMsg); err != nil {
			t.Error(err)
		}

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

	require.NoError(t, h.Close(ctx))
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

	conf := output.NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"
	conf.Multipart = []output.HTTPClientMultipartExpression{
		{
			ContentDisposition: `form-data; name="text"`,
			ContentType:        "text/plain",
			Body:               "PART-A",
		},
		{
			ContentDisposition: `form-data; name="file1"; filename="a.txt"`,
			ContentType:        "text/plain",
			Body:               "PART-B",
		},
	}
	h, err := newHTTPClientWriter(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < nTestLoops; i++ {
		if err = h.WriteBatch(context.Background(), message.QuickBatch([][]byte{[]byte("test")})); err != nil {
			t.Error(err)
		}
		select {
		case resMsg := <-resultChan:
			if resMsg.Len() != len(conf.Multipart) {
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

	require.NoError(t, h.Close(ctx))
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

	conf := output.NewHTTPClientConfig()
	conf.URL = ts.URL + "/testpost"
	conf.Multipart = []output.HTTPClientMultipartExpression{
		{
			ContentDisposition: `form-data; name="text"`,
			ContentType:        "text/plain",
			Body:               "PART-A",
		},
		{
			ContentDisposition: `form-data; name="file1"; filename="a.txt"`,
			ContentType:        "text/plain",
			Body:               "PART-B",
		},
	}
	h, err := newHTTPClientWriter(conf, mock.NewManager())
	if err != nil {
		t.Fatal(err)
	}
	if err = h.WriteBatch(context.Background(), message.QuickBatch([][]byte{[]byte("test")})); err != nil {
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
			err := json.Unmarshal(resMsg.Get(i).AsBytes(), &mp)
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

	require.NoError(t, h.Close(ctx))
}
