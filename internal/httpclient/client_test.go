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

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

func clientConfig(t testing.TB, confStr string, args ...any) OldConfig {
	t.Helper()

	spec := service.NewConfigSpec().Field(ConfigField("GET", false))
	parsed, err := spec.ParseYAML(fmt.Sprintf(confStr, args...), nil)
	require.NoError(t, err)

	conf, err := ConfigFromParsed(parsed)
	require.NoError(t, err)
	return conf
}

func TestHTTPClientRetries(t *testing.T) {
	var reqCount uint32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddUint32(&reqCount, 1)
		http.Error(w, "test error", http.StatusForbidden)
	}))
	defer ts.Close()

	conf := clientConfig(t, `
url: %v
retry_period: 1ms
retries: 3
`, ts.URL+"/testpost")

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)
	defer h.Close(context.Background())

	_, err = h.Send(context.Background(), service.MessageBatch{service.NewMessage([]byte("test"))})
	assert.Error(t, err)
	assert.Equal(t, uint32(4), atomic.LoadUint32(&reqCount))
}

func TestHTTPClientBadRequest(t *testing.T) {
	conf := clientConfig(t, `
url: htp://notvalid:1111
verb: notvalid
retry_period: 1ms
retries: 3
`)

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)

	_, err = h.Send(context.Background(), service.MessageBatch{service.NewMessage([]byte("test"))})
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

	conf := clientConfig(t, `
url: %v
`, ts.URL+"/testpost")

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := service.MessageBatch{
			service.NewMessage([]byte(testStr)),
		}

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

	conf := clientConfig(t, `
url: %v
`, ts.URL+"/testpost")

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)

	testMsg := service.MessageBatch{service.NewMessage([]byte("hello world"))}

	res, err := h.Send(context.Background(), testMsg)
	require.NoError(t, err)

	require.Len(t, res, 1)

	mBytes, err := res[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "HELLO WORLD", string(mBytes))
}

func TestHTTPClientDropOn(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"foo":"bar"}`))
	}))
	defer ts.Close()

	conf := clientConfig(t, `
url: %v
drop_on: [ 400 ]
`, ts.URL+"/testpost")

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)

	testMsg := service.MessageBatch{service.NewMessage([]byte(`{"bar":"baz"}`))}

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

	conf := clientConfig(t, `
url: %v
successful_on: [ 400 ]
`, ts.URL+"/testpost")

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)

	testMsg := service.MessageBatch{service.NewMessage([]byte(`{"bar":"baz"}`))}
	resMsg, err := h.Send(context.Background(), testMsg)
	require.NoError(t, err)

	mBytes, err := resMsg[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"foo":"bar"}`, string(mBytes))
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

	conf := clientConfig(t, `
url: %v
headers:
  "static": "foo"
  "dynamic": 'hdr-${!json("foo.baz")}'
  "Host": "simpleHost.com"
`, ts.URL+`/${! json("foo.bar") }`)

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf(`{"test":%v,"foo":{"bar":"firstvar","baz":"secondvar"}}`, i)
		testMsg := service.MessageBatch{service.NewMessage([]byte(testStr))}

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

	conf := clientConfig(t, `
url: %v
`, ts.URL+"/testpost")

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", i)
		testMsg := service.MessageBatch{
			service.NewMessage([]byte(testStr + "PART-A")),
			service.NewMessage([]byte(testStr + "PART-B")),
		}

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

	conf := clientConfig(t, `
url: %v
`, ts.URL+"/testpost")

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", j)
		resMsg, err := h.Send(context.Background(), nil)
		require.NoError(t, err)

		assert.Len(t, resMsg, 1)

		mBytes, err := resMsg[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, testStr+"PART-A", string(mBytes))

		v, _ := resMsg[0].MetaGet("foo-bar")
		assert.Equal(t, "", v)
		v, _ = resMsg[0].MetaGet("http_status_code")
		assert.Equal(t, "201", v)
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

	conf := clientConfig(t, `
url: %v
metadata:
  include_prefixes: [ "foo_" ]
`, ts.URL+"/testpost")

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)

	sendMsg := service.MessageBatch{service.NewMessage([]byte("hello world"))}
	part := sendMsg[0]
	part.MetaSetMut("foo_a", "foo a value")
	part.MetaSetMut("foo_b", "foo b value")
	part.MetaSetMut("bar_a", "bar a value")
	part.MetaSetMut("bar_b", "bar b value")

	resMsg, err := h.Send(context.Background(), sendMsg)
	require.NoError(t, err)

	assert.Len(t, resMsg, 1)

	mBytes, err := resMsg[0].AsBytes()
	require.NoError(t, err)

	assert.Equal(t, `
foo_a: foo a value
bar_a: 
foo_b: foo b value
bar_b: 
`, string(mBytes))
}

func TestHTTPClientReceiveHeadersWithMetadataFiltering(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("foobar", "baz")
		w.Header().Set("extra", "val")
		w.WriteHeader(http.StatusCreated)
	}))
	defer ts.Close()

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
		if tt.includePrefixes == nil {
			tt.includePrefixes = []string{}
		}
		if tt.includePatterns == nil {
			tt.includePatterns = []string{}
		}
		conf := clientConfig(t, `
url: %v
extract_headers:
  include_prefixes: %v
  include_patterns: %v
`, ts.URL, gabs.Wrap(tt.includePrefixes).String(), gabs.Wrap(tt.includePatterns).String())

		h, err := NewClientFromOldConfig(conf, service.MockResources())
		if err != nil {
			t.Fatalf("%s: %s", tt.name, err)
		}

		resMsg, err := h.Send(context.Background(), nil)
		if err != nil {
			t.Fatalf("%s: %s", tt.name, err)
		}

		metadataCount := 0
		_ = resMsg[0].MetaWalk(func(_, _ string) error { metadataCount++; return nil })

		if tt.noExtraMetadata {
			if metadataCount > 1 {
				t.Errorf("%s: wrong number of metadata items: %d", tt.name, metadataCount)
			}
			v, _ := resMsg[0].MetaGet("foobar")
			if exp, act := "", v; exp != act {
				t.Errorf("%s: wrong metadata value: %v != %v", tt.name, act, exp)
			}
		} else {
			v, _ := resMsg[0].MetaGet("foobar")
			if exp, act := "baz", v; exp != act {
				t.Errorf("%s: wrong metadata value: %v != %v", tt.name, act, exp)
			}
		}
	}
}

func TestHTTPClientReceiveMultipart(t *testing.T) {
	nTestLoops := 1000

	j := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testStr := fmt.Sprintf("test%v", j)
		j++
		msg := service.MessageBatch{
			service.NewMessage([]byte(testStr + "PART-A")),
			service.NewMessage([]byte(testStr + "PART-B")),
		}

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for i := 0; i < len(msg); i++ {
			part, err := writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{"application/octet-stream"},
				"foo-bar":      []string{"baz-" + strconv.Itoa(i), "ignored"},
			})
			require.NoError(t, err)

			mBytes, err := msg[i].AsBytes()
			require.NoError(t, err)

			_, err = io.Copy(part, bytes.NewReader(mBytes))
			require.NoError(t, err)
		}
		writer.Close()

		w.Header().Add("Content-Type", writer.FormDataContentType())
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(body.Bytes())
	}))
	defer ts.Close()

	conf := clientConfig(t, `
url: %v
`, ts.URL)

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)

	for i := 0; i < nTestLoops; i++ {
		testStr := fmt.Sprintf("test%v", j)
		resMsg, err := h.Send(context.Background(), nil)
		require.NoError(t, err)

		assert.Len(t, resMsg, 2)

		mBytes, err := resMsg[0].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, testStr+"PART-A", string(mBytes))

		mBytes, err = resMsg[1].AsBytes()
		require.NoError(t, err)
		assert.Equal(t, testStr+"PART-B", string(mBytes))

		v, _ := resMsg[0].MetaGet("foo-bar")
		assert.Equal(t, "", v)

		v, _ = resMsg[0].MetaGet("http_status_code")
		assert.Equal(t, "201", v)

		v, _ = resMsg[1].MetaGet("foo-bar")
		assert.Equal(t, "", v)

		v, _ = resMsg[1].MetaGet("http_status_code")
		assert.Equal(t, "201", v)
	}
}

func TestHTTPClientBadTLS(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		_, _ = w.Write(bytes.ToUpper(b))
	}))
	defer ts.Close()

	conf := clientConfig(t, `
url: %v
retries: 0
`, ts.URL)

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)

	_, err = h.Send(context.Background(), service.MessageBatch{
		service.NewMessage([]byte("hello world")),
	})
	require.Error(t, err)
}

func TestHTTPClientProxyConf(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("this shouldnt be hit directly")
	}))
	defer ts.Close()

	tsProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, r.Host, strings.TrimPrefix(ts.URL, "http://"))
		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		_, _ = w.Write(bytes.ToUpper(b))
	}))
	defer tsProxy.Close()

	conf := clientConfig(t, `
url: %v
proxy_url: %v
`, ts.URL+"/testpost", tsProxy.URL)

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)

	resBatch, err := h.Send(context.Background(), service.MessageBatch{
		service.NewMessage([]byte("hello world")),
	})
	require.NoError(t, err)
	require.Len(t, resBatch, 1)

	mBytes, err := resBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "HELLO WORLD", string(mBytes))
}

func TestHTTPClientProxyAndTLSConf(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("this shouldnt be hit directly")
	}))
	defer ts.Close()

	tsProxy := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, r.Host, strings.TrimPrefix(ts.URL, "http://"))
		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		_, _ = w.Write(bytes.ToUpper(b))
	}))
	defer tsProxy.Close()

	conf := clientConfig(t, `
url: %v
tls:
  enabled: true
  skip_cert_verify: true
proxy_url: %v
`, ts.URL+"/testpost", tsProxy.URL)

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)

	resBatch, err := h.Send(context.Background(), service.MessageBatch{
		service.NewMessage([]byte("hello world")),
	})
	require.NoError(t, err)
	require.Len(t, resBatch, 1)

	mBytes, err := resBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "HELLO WORLD", string(mBytes))
}

func TestHTTPClientOAuth2Conf(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer footoken", r.Header.Get("Authorization"))
		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		_, _ = w.Write(bytes.ToUpper(b))
	}))
	defer ts.Close()

	tsOAuth2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Basic Zm9va2V5OmZvb3NlY3JldA==", r.Header.Get("Authorization"))
		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, "grant_type=client_credentials", string(b))
		_, _ = w.Write([]byte(`access_token=footoken&token_type=Bearer`))
	}))
	defer tsOAuth2.Close()

	conf := clientConfig(t, `
url: %v
oauth2:
  enabled: true
  token_url: %v
  client_key: fookey
  client_secret: foosecret
`, ts.URL+"/testpost", tsOAuth2.URL)

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)

	resBatch, err := h.Send(context.Background(), service.MessageBatch{
		service.NewMessage([]byte("hello world")),
	})
	require.NoError(t, err)
	require.Len(t, resBatch, 1)

	mBytes, err := resBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "HELLO WORLD", string(mBytes))
}

func TestHTTPClientOAuth2AndTLSConf(t *testing.T) {
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer footoken", r.Header.Get("Authorization"))
		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		_, _ = w.Write(bytes.ToUpper(b))
	}))
	defer ts.Close()

	tsOAuth2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Basic Zm9va2V5OmZvb3NlY3JldA==", r.Header.Get("Authorization"))
		b, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, "grant_type=client_credentials", string(b))
		_, _ = w.Write([]byte(`access_token=footoken&token_type=Bearer`))
	}))
	defer tsOAuth2.Close()

	conf := clientConfig(t, `
url: %v
oauth2:
  enabled: true
  token_url: %v
  client_key: fookey
  client_secret: foosecret
tls:
  enabled: true
  skip_cert_verify: true
`, ts.URL+"/testpost", tsOAuth2.URL)

	h, err := NewClientFromOldConfig(conf, service.MockResources())
	require.NoError(t, err)

	resBatch, err := h.Send(context.Background(), service.MessageBatch{
		service.NewMessage([]byte("hello world")),
	})
	require.NoError(t, err)
	require.Len(t, resBatch, 1)

	mBytes, err := resBatch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "HELLO WORLD", string(mBytes))
}
