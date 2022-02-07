package output

import (
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPClientMultipartEnabled(t *testing.T) {
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

	conf := NewConfig()
	conf.Type = TypeHTTPClient
	conf.HTTPClient.URL = ts.URL + "/testpost"

	h, err := NewHTTPClient(conf, types.NoopMgr(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	tChan := make(chan types.Transaction)
	require.NoError(t, h.Consume(tChan))

	resChan := make(chan types.Response)
	select {
	case tChan <- types.NewTransaction(message.QuickBatch([][]byte{
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
		assert.NoError(t, res.Error())
	case <-time.After(time.Second):
		t.Fatal("Action timed out")
	}

	h.CloseAsync()
	require.NoError(t, h.WaitForClose(time.Second))
}

func TestHTTPClientMultipartDisabled(t *testing.T) {
	resultChan := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resBytes, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		resultChan <- string(resBytes)
	}))
	defer ts.Close()

	conf := NewConfig()
	conf.Type = TypeHTTPClient
	conf.HTTPClient.URL = ts.URL + "/testpost"
	conf.HTTPClient.BatchAsMultipart = false

	h, err := NewHTTPClient(conf, types.NoopMgr(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	tChan := make(chan types.Transaction)
	require.NoError(t, h.Consume(tChan))

	resChan := make(chan types.Response)
	select {
	case tChan <- types.NewTransaction(message.QuickBatch([][]byte{
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
		assert.NoError(t, res.Error())
	case <-time.After(time.Second):
		t.Fatal("Action timed out")
	}

	h.CloseAsync()
	require.NoError(t, h.WaitForClose(time.Second))
}
