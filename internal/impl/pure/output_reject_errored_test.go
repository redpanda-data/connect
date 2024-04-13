package pure_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestRejectErroredHappy(t *testing.T) {
	var resMut sync.Mutex
	results := map[string][]string{} // Maps seen paths to seen data

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		resMut.Lock()
		defer resMut.Unlock()

		results[r.URL.Path] = append(results[r.URL.Path], string(body))
	}))

	conf := parseYAMLOutputConf(t, strings.ReplaceAll(`
processors:
  - mapping: 'root = content().uppercase()'
fallback:
  - reject_errored:
      http_client:
        url: $URL/a
        retries: 1
        retry_period: "1ms"
  - http_client:
      url: $URL/dlq
      retries: 1
      retry_period: "1ms"
`, "$URL", server.URL))

	s, err := bundle.AllOutputs.Init(conf, mock.NewManager())
	require.NoError(t, err)

	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	require.NoError(t, s.Consume(sendChan))

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		s.TriggerCloseNow()
		require.NoError(t, s.WaitForClose(ctx))
		done()
	})

	for _, testBatch := range [][]string{
		{
			"test a",
		},
		{
			"test b",
			"test c",
			"test d",
			"test e",
		},
	} {
		var b message.Batch
		for _, m := range testBatch {
			b = append(b, message.NewPart([]byte(m)))
		}

		select {
		case sendChan <- message.NewTransaction(b, resChan):
		case <-time.After(time.Second * 30):
			t.Fatal("Action timed out")
		}

		select {
		case err := <-resChan:
			require.NoError(t, err)
		case <-time.After(time.Second * 2):
			t.Fatal("Action timed out")
		}
	}

	resMut.Lock()
	assert.Equal(t, map[string][]string{
		"/a": {
			"TEST A",
			"TEST B",
			"TEST C",
			"TEST D",
			"TEST E",
		},
	}, results)
	resMut.Unlock()
}

func TestRejectErroredSad(t *testing.T) {
	var resMut sync.Mutex
	results := map[string][]string{} // Maps seen paths to seen data

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		resMut.Lock()
		defer resMut.Unlock()

		results[r.URL.Path] = append(results[r.URL.Path], string(body))
	}))

	conf := parseYAMLOutputConf(t, strings.ReplaceAll(`
processors:
  - mapping: 'root = if content().contains("nope") { throw("no way") }'
fallback:
  - reject_errored:
      http_client:
        url: $URL/a
        retries: 1
        retry_period: "1ms"
  - http_client:
      url: $URL/dlq
      retries: 1
      retry_period: "1ms"
`, "$URL", server.URL))

	s, err := bundle.AllOutputs.Init(conf, mock.NewManager())
	require.NoError(t, err)

	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	require.NoError(t, s.Consume(sendChan))

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		s.TriggerCloseNow()
		require.NoError(t, s.WaitForClose(ctx))
		done()
	})

	for _, testBatch := range [][]string{
		{
			"test nope a",
		},
		{
			"test b",
			"test nope c",
			"test d",
			"test nope e",
		},
	} {
		var b message.Batch
		for _, m := range testBatch {
			b = append(b, message.NewPart([]byte(m)))
		}

		select {
		case sendChan <- message.NewTransaction(b, resChan):
		case <-time.After(time.Second * 30):
			t.Fatal("Action timed out")
		}

		select {
		case err := <-resChan:
			require.NoError(t, err)
		case <-time.After(time.Second * 2):
			t.Fatal("Action timed out")
		}
	}

	resMut.Lock()
	assert.Equal(t, map[string][]string{
		"/a": {
			"test b",
			"test d",
		},
		"/dlq": {
			"test nope a",
			"test nope c",
			"test nope e",
		},
	}, results)
	resMut.Unlock()
}

func TestRejectErroredSadWholeBatch(t *testing.T) {
	var resMut sync.Mutex
	results := map[string][]string{} // Maps seen paths to seen data

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		resMut.Lock()
		defer resMut.Unlock()

		results[r.URL.Path] = append(results[r.URL.Path], string(body))
	}))

	conf := parseYAMLOutputConf(t, strings.ReplaceAll(`
processors:
  - mapping: 'root = if content().contains("nope") { throw("no way") }'
fallback:
  - reject_errored:
      http_client:
        url: $URL/a
        retries: 1
        retry_period: "1ms"
  - http_client:
      url: $URL/dlq
      retries: 1
      retry_period: "1ms"
`, "$URL", server.URL))

	s, err := bundle.AllOutputs.Init(conf, mock.NewManager())
	require.NoError(t, err)

	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	require.NoError(t, s.Consume(sendChan))

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		s.TriggerCloseNow()
		require.NoError(t, s.WaitForClose(ctx))
		done()
	})

	for _, testBatch := range [][]string{
		{
			"test nope a",
			"test nope b",
			"test nope c",
			"test nope d",
		},
	} {
		var b message.Batch
		for _, m := range testBatch {
			b = append(b, message.NewPart([]byte(m)))
		}

		select {
		case sendChan <- message.NewTransaction(b, resChan):
		case <-time.After(time.Second * 30):
			t.Fatal("Action timed out")
		}

		select {
		case err := <-resChan:
			require.NoError(t, err)
		case <-time.After(time.Second * 2):
			t.Fatal("Action timed out")
		}
	}

	resMut.Lock()
	assert.Equal(t, map[string][]string{
		"/dlq": {
			"test nope a",
			"test nope b",
			"test nope c",
			"test nope d",
		},
	}, results)
	resMut.Unlock()
}

func TestRejectErroredNestedBatchErrors(t *testing.T) {
	var resMut sync.Mutex
	results := map[string][]string{} // Maps seen paths to seen data

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		resMut.Lock()
		defer resMut.Unlock()

		results[r.URL.Path] = append(results[r.URL.Path], string(body))
	}))

	conf := parseYAMLOutputConf(t, strings.ReplaceAll(`
processors:
  - mapping: 'root = if content().contains("nope") { throw("no way") }'
fallback:
  - reject_errored:
      reject_errored:
        http_client:
          url: $URL/a
          retries: 1
          retry_period: "1ms"
      processors:
        - mapping: 'root = if content().contains("nah") { throw("nuh uh") }'
  - http_client:
      url: $URL/dlq
      retries: 1
      retry_period: "1ms"
`, "$URL", server.URL))

	s, err := bundle.AllOutputs.Init(conf, mock.NewManager())
	require.NoError(t, err)

	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	require.NoError(t, s.Consume(sendChan))

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		s.TriggerCloseNow()
		require.NoError(t, s.WaitForClose(ctx))
		done()
	})

	for _, testBatch := range [][]string{
		{
			"test nope a",
			"test b",
			"test nah c",
			"test nope d",
		},
		{
			"test nah e",
		},
		{
			"test nah f",
			"test nah g",
		},
	} {
		var b message.Batch
		for _, m := range testBatch {
			b = append(b, message.NewPart([]byte(m)))
		}

		select {
		case sendChan <- message.NewTransaction(b, resChan):
		case <-time.After(time.Second * 30):
			t.Fatal("Action timed out")
		}

		select {
		case err := <-resChan:
			require.NoError(t, err)
		case <-time.After(time.Second * 2):
			t.Fatal("Action timed out")
		}
	}

	resMut.Lock()
	assert.Equal(t, map[string][]string{
		"/a": {
			"test b",
		},
		"/dlq": {
			"test nope a",
			"test nah c",
			"test nope d",
			"test nah e",
			"test nah f",
			"test nah g",
		},
	}, results)
	resMut.Unlock()
}

func TestRejectErroredNestedTotalErrors(t *testing.T) {
	var resMut sync.Mutex
	results := map[string][]string{} // Maps seen paths to seen data

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		resMut.Lock()
		defer resMut.Unlock()

		results[r.URL.Path] = append(results[r.URL.Path], string(body))
	}))

	conf := parseYAMLOutputConf(t, strings.ReplaceAll(`
processors:
  - mapping: 'root = if content().contains("nope") { throw("no way") }'
fallback:
  - reject_errored:
      reject: "everything"
  - http_client:
      url: $URL/dlq
      retries: 1
      retry_period: "1ms"
`, "$URL", server.URL))

	s, err := bundle.AllOutputs.Init(conf, mock.NewManager())
	require.NoError(t, err)

	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	require.NoError(t, s.Consume(sendChan))

	t.Cleanup(func() {
		ctx, done := context.WithTimeout(context.Background(), time.Second*30)
		s.TriggerCloseNow()
		require.NoError(t, s.WaitForClose(ctx))
		done()
	})

	for _, testBatch := range [][]string{
		{
			"test nope a",
			"test b",
			"test c",
			"test nope d",
		},
		{
			"test nope e",
		},
		{
			"test f",
		},
		{
			"test g",
			"test h",
		},
	} {
		var b message.Batch
		for _, m := range testBatch {
			b = append(b, message.NewPart([]byte(m)))
		}

		select {
		case sendChan <- message.NewTransaction(b, resChan):
		case <-time.After(time.Second * 30):
			t.Fatal("Action timed out")
		}

		select {
		case err := <-resChan:
			require.NoError(t, err)
		case <-time.After(time.Second * 2):
			t.Fatal("Action timed out")
		}
	}

	resMut.Lock()
	assert.Equal(t, map[string][]string{
		"/dlq": {
			"test nope a",
			"test b",
			"test c",
			"test nope d",
			"test nope e",
			"test f",
			"test g",
			"test h",
		},
	}, results)
	resMut.Unlock()
}
