package httpclient

import (
	"bytes"
	"context"
	"fmt"
	"io"
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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestHTTPClientRetries(t *testing.T) {
	var reqCount uint32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint32(&reqCount, 1)
		http.Error(w, "test error", http.StatusForbidden)
	}))
	defer ts.Close()

	conf := NewOldConfig()
	conf.URL = ts.URL + "/testpost"
	conf.Retry = "1ms"
	conf.NumRetries = 3

	h, err := NewClientFromOldConfig(conf, mock.NewManager())
	require.NoError(t, err)
	defer h.Close(context.Background())

	out := message.QuickBatch([][]byte{[]byte("test")})
	_, err = h.Send(context.Background(), out)
	assert.Error(t, err)
	assert.Equal(t, uint32(4), atomic.LoadUint32(&reqCount))
}

func TestHTTPClientBadRequest(t *testing.T) {
	conf := NewOldConfig()
	conf.URL = "htp://notvalid:1111"
	conf.Verb = "notvalid\n"
	conf.NumRetries = 3

	h, err := NewClientFromOldConfig(conf, mock.NewManager())
	require.NoError(t, err)

	out := message.QuickBatch([][]byte{[]byte("test")})
	_, err = h.Send(context.Background(), out)
	assert.Error(t, err)
}

func TestHTTPClientSendBasic(t *testing.T) {
	nTestLoops := 1000

	resultChan := make(chan message.Batch, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := message.QuickBatch(nil)
		defer func() {
			resultChan <- msg
		}()

		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		msg = append(msg, message.NewPart(b))
	}))
	defer ts.Close()

	conf := NewOldConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := NewClientFromOldConfig(conf, mock.NewManager())
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.QuickBatch([][]byte{[]byte(testStr)})

		_, err = h.Send(context.Background(), testMsg)
		require.NoError(t, err)

		select {
		case resMsg := <-resultChan:
			require.Equal(t, 1, resMsg.Len())
			assert.Equal(t, testStr, string(resMsg.Get(0).AsBytes()))
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}
}

func TestHTTPClientBadContentType(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		_, err = w.Write(bytes.ToUpper(b))
		require.NoError(t, err)
	}))
	t.Cleanup(ts.Close)

	conf := NewOldConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := NewClientFromOldConfig(conf, mock.NewManager())
	require.NoError(t, err)

	testMsg := message.QuickBatch([][]byte{[]byte("hello world")})

	res, err := h.Send(context.Background(), testMsg)
	require.NoError(t, err)

	require.Equal(t, 1, res.Len())
	assert.Equal(t, "HELLO WORLD", string(res.Get(0).AsBytes()))
}

func TestHTTPClientDropOn(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"foo":"bar"}`))
	}))
	defer ts.Close()

	conf := NewOldConfig()
	conf.URL = ts.URL + "/testpost"
	conf.DropOn = []int{400}

	h, err := NewClientFromOldConfig(conf, mock.NewManager())
	require.NoError(t, err)

	testMsg := message.QuickBatch([][]byte{[]byte(`{"bar":"baz"}`)})

	_, err = h.Send(context.Background(), testMsg)
	require.Error(t, err)
}

func TestHTTPClientSuccessfulOn(t *testing.T) {
	var reqs int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"foo":"bar"}`))
		atomic.AddInt32(&reqs, 1)
	}))
	defer ts.Close()

	conf := NewOldConfig()
	conf.URL = ts.URL + "/testpost"
	conf.SuccessfulOn = []int{400}

	h, err := NewClientFromOldConfig(conf, mock.NewManager())
	require.NoError(t, err)

	testMsg := message.QuickBatch([][]byte{[]byte(`{"bar":"baz"}`)})
	resMsg, err := h.Send(context.Background(), testMsg)
	require.NoError(t, err)

	assert.Equal(t, `{"foo":"bar"}`, string(resMsg.Get(0).AsBytes()))
	assert.Equal(t, int32(1), atomic.LoadInt32(&reqs))
}

func TestHTTPClientSendInterpolate(t *testing.T) {
	nTestLoops := 1000

	resultChan := make(chan message.Batch, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/firstvar", r.URL.Path)
		assert.Equal(t, "hdr-secondvar", r.Header.Get("dynamic"))
		assert.Equal(t, "foo", r.Header.Get("static"))
		assert.Equal(t, "simpleHost.com", r.Host)

		msg := message.QuickBatch(nil)
		defer func() {
			resultChan <- msg
		}()

		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		msg = append(msg, message.NewPart(b))
	}))
	defer ts.Close()

	conf := NewOldConfig()
	conf.URL = ts.URL + `/${! json("foo.bar") }`
	conf.Headers["static"] = "foo"
	conf.Headers["dynamic"] = `hdr-${!json("foo.baz")}`
	conf.Headers["Host"] = "simpleHost.com"

	h, err := NewClientFromOldConfig(conf, mock.NewManager())
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf(`{"test":%v,"foo":{"bar":"firstvar","baz":"secondvar"}}`, i)
		testMsg := message.QuickBatch([][]byte{[]byte(testStr)})

		_, err = h.Send(context.Background(), testMsg)
		require.NoError(t, err)

		select {
		case resMsg := <-resultChan:
			require.Equal(t, 1, resMsg.Len())
			assert.Equal(t, testStr, string(resMsg.Get(0).AsBytes()))
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}
}

func TestHTTPClientSendMultipart(t *testing.T) {
	nTestLoops := 1000

	resultChan := make(chan message.Batch, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := message.QuickBatch(nil)
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

				msgBytes, err := io.ReadAll(p)
				require.NoError(t, err)

				msg = append(msg, message.NewPart(msgBytes))
			}
		} else {
			b, err := io.ReadAll(r.Body)
			require.NoError(t, err)

			msg = append(msg, message.NewPart(b))
		}
	}))
	defer ts.Close()

	conf := NewOldConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := NewClientFromOldConfig(conf, mock.NewManager())
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := message.QuickBatch([][]byte{
			[]byte(testStr + "PART-A"),
			[]byte(testStr + "PART-B"),
		})

		_, err = h.Send(context.Background(), testMsg)
		require.NoError(t, err)

		select {
		case resMsg := <-resultChan:
			assert.Equal(t, 2, resMsg.Len())
			assert.Equal(t, testStr+"PART-A", string(resMsg.Get(0).AsBytes()))
			assert.Equal(t, testStr+"PART-B", string(resMsg.Get(1).AsBytes()))
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
		_, _ = w.Write([]byte(testStr + "PART-A"))
	}))
	defer ts.Close()

	conf := NewOldConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := NewClientFromOldConfig(conf, mock.NewManager())
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", j)
		resMsg, err := h.Send(context.Background(), nil)
		require.NoError(t, err)

		assert.Equal(t, 1, resMsg.Len())
		assert.Equal(t, testStr+"PART-A", string(resMsg.Get(0).AsBytes()))
		assert.Equal(t, "", resMsg.Get(0).MetaGetStr("foo-bar"))
		assert.Equal(t, "201", resMsg.Get(0).MetaGetStr("http_status_code"))
	}
}

func TestHTTPClientSendMetaFilter(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `
foo_a: %v
bar_a: %v
foo_b: %v
bar_b: %v
`,
			r.Header.Get("foo_a"),
			r.Header.Get("bar_a"),
			r.Header.Get("foo_b"),
			r.Header.Get("bar_b"),
		)
	}))
	defer ts.Close()

	conf := NewOldConfig()
	conf.URL = ts.URL + "/testpost"
	conf.Metadata.IncludePrefixes = []string{"foo_"}

	h, err := NewClientFromOldConfig(conf, mock.NewManager())
	require.NoError(t, err)

	sendMsg := message.QuickBatch([][]byte{[]byte("hello world")})
	part := sendMsg.Get(0)
	part.MetaSetMut("foo_a", "foo a value")
	part.MetaSetMut("foo_b", "foo b value")
	part.MetaSetMut("bar_a", "bar a value")
	part.MetaSetMut("bar_b", "bar b value")

	resMsg, err := h.Send(context.Background(), sendMsg)
	require.NoError(t, err)

	assert.Equal(t, 1, resMsg.Len())
	assert.Equal(t, `
foo_a: foo a value
bar_a: 
foo_b: foo b value
bar_b: 
`, string(resMsg.Get(0).AsBytes()))
}

func TestHTTPClientReceiveHeadersWithMetadataFiltering(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("foobar", "baz")
		w.Header().Set("extra", "val")
		w.WriteHeader(http.StatusCreated)
	}))
	defer ts.Close()

	conf := NewOldConfig()
	conf.URL = ts.URL

	for _, tt := range []struct {
		name            string
		noExtraMetadata bool
		includePrefixes []string
		includePatterns []string
	}{
		{
			name:            "no extra metadata",
			noExtraMetadata: true,
		},
		{
			name:            "include_prefixes only",
			includePrefixes: []string{"foo"},
		},
		{
			name:            "include_patterns only",
			includePatterns: []string{".*bar"},
		},
	} {
		conf.ExtractMetadata.IncludePrefixes = tt.includePrefixes
		conf.ExtractMetadata.IncludePatterns = tt.includePatterns
		h, err := NewClientFromOldConfig(conf, mock.NewManager())
		if err != nil {
			t.Fatalf("%s: %s", tt.name, err)
		}

		resMsg, err := h.Send(context.Background(), nil)
		if err != nil {
			t.Fatalf("%s: %s", tt.name, err)
		}

		metadataCount := 0
		_ = resMsg.Get(0).MetaIterStr(func(_, _ string) error { metadataCount++; return nil })

		if tt.noExtraMetadata {
			if metadataCount > 1 {
				t.Errorf("%s: wrong number of metadata items: %d", tt.name, metadataCount)
			}
			if exp, act := "", resMsg.Get(0).MetaGetStr("foobar"); exp != act {
				t.Errorf("%s: wrong metadata value: %v != %v", tt.name, act, exp)
			}
		} else if exp, act := "baz", resMsg.Get(0).MetaGetStr("foobar"); exp != act {
			t.Errorf("%s: wrong metadata value: %v != %v", tt.name, act, exp)
		}
	}
}

func TestHTTPClientReceiveMultipart(t *testing.T) {
	nTestLoops := 1000

	j := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testStr := fmt.Sprintf("test%v", j)
		j++
		msg := message.QuickBatch([][]byte{
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

			_, err = io.Copy(part, bytes.NewReader(msg.Get(i).AsBytes()))
			require.NoError(t, err)
		}
		writer.Close()

		w.Header().Add("Content-Type", writer.FormDataContentType())
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(body.Bytes())
	}))
	defer ts.Close()

	conf := NewOldConfig()
	conf.URL = ts.URL + "/testpost"

	h, err := NewClientFromOldConfig(conf, mock.NewManager())
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", j)
		resMsg, err := h.Send(context.Background(), nil)
		require.NoError(t, err)

		assert.Equal(t, 2, resMsg.Len())
		assert.Equal(t, testStr+"PART-A", string(resMsg.Get(0).AsBytes()))
		assert.Equal(t, testStr+"PART-B", string(resMsg.Get(1).AsBytes()))
		assert.Equal(t, "", resMsg.Get(0).MetaGetStr("foo-bar"))
		assert.Equal(t, "201", resMsg.Get(0).MetaGetStr("http_status_code"))
		assert.Equal(t, "", resMsg.Get(1).MetaGetStr("foo-bar"))
		assert.Equal(t, "201", resMsg.Get(1).MetaGetStr("http_status_code"))
	}
}
