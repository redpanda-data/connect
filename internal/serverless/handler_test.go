package serverless

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/config"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestHandlerAsync(t *testing.T) {
	var results [][]byte
	var resMut sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resMut.Lock()
		defer resMut.Unlock()

		resBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, resBytes)

		_, _ = w.Write([]byte("success"))
	}))
	defer ts.Close()

	var err error
	conf := config.New()
	conf.Output, err = output.FromYAML(fmt.Sprintf(`
http_client:
  url: %v
`, ts.URL))
	require.NoError(t, err)

	h, err := NewHandler(conf)
	if err != nil {
		t.Fatal(err)
	}

	var res any
	if res, err = h.Handle(context.Background(), map[string]any{"foo": "bar"}); err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]any{"message": "request successful"}, res; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong sync response: %v != %v", exp, act)
	}
	if exp, act := [][]byte{[]byte(`{"foo":"bar"}`)}, results; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong sync response: %s != %s", exp, act)
	}

	if err = h.Close(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestHandlerSync(t *testing.T) {
	conf := config.New()
	conf.Output.Type = ServerlessResponseType

	h, err := NewHandler(conf)
	if err != nil {
		t.Fatal(err)
	}

	var res any
	if res, err = h.Handle(context.Background(), map[string]any{"foo": "bar"}); err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]any{"foo": "bar"}, res; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong sync response: %v != %v", exp, act)
	}

	if err = h.Close(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestHandlerSyncBatch(t *testing.T) {
	conf := config.New()
	conf.Output.Type = ServerlessResponseType

	pConf, err := processor.FromYAML(`
select_parts:
  parts: [ 0, 0, 0 ]
`)
	require.NoError(t, err)

	conf.Pipeline.Processors = append(conf.Pipeline.Processors, pConf)

	h, err := NewHandler(conf)
	if err != nil {
		t.Fatal(err)
	}

	var res any
	if res, err = h.Handle(context.Background(), map[string]any{"foo": "bar"}); err != nil {
		t.Fatal(err)
	}
	if exp, act := []any{
		map[string]any{"foo": "bar"},
		map[string]any{"foo": "bar"},
		map[string]any{"foo": "bar"},
	}, res; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong sync response: %v != %v", exp, act)
	}

	if err = h.Close(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestHandlerSyncBatches(t *testing.T) {
	conf := config.New()
	conf.Output.Type = ServerlessResponseType

	pConf, err := processor.FromYAML(`
select_parts:
  parts: [ 0, 0, 0 ]
`)
	require.NoError(t, err)

	conf.Pipeline.Processors = append(conf.Pipeline.Processors, pConf)

	pConf = processor.NewConfig()
	pConf.Type = "split"

	conf.Pipeline.Processors = append(conf.Pipeline.Processors, pConf)

	h, err := NewHandler(conf)
	if err != nil {
		t.Fatal(err)
	}

	var res any
	if res, err = h.Handle(context.Background(), map[string]any{"foo": "bar"}); err != nil {
		t.Fatal(err)
	}
	if exp, act := []any{
		[]any{map[string]any{"foo": "bar"}},
		[]any{map[string]any{"foo": "bar"}},
		[]any{map[string]any{"foo": "bar"}},
	}, res; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong sync response: %v != %v", exp, act)
	}

	if err = h.Close(time.Second * 10); err != nil {
		t.Error(err)
	}
}

func TestHandlerCombined(t *testing.T) {
	var results [][]byte
	var resMut sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resMut.Lock()
		defer resMut.Unlock()

		resBytes, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		results = append(results, resBytes)

		_, _ = w.Write([]byte("success"))
	}))
	defer ts.Close()

	conf := config.New()
	var err error
	conf.Output, err = output.FromYAML(fmt.Sprintf(`
broker:
  outputs:
    - sync_response: {}
    - http_client:
        url: %v
`, ts.URL))
	require.NoError(t, err)

	h, err := NewHandler(conf)
	if err != nil {
		t.Fatal(err)
	}

	var res any
	if res, err = h.Handle(context.Background(), map[string]any{"foo": "bar"}); err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]any{"foo": "bar"}, res; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong sync response: %v != %v", exp, act)
	}
	if exp, act := [][]byte{[]byte(`{"foo":"bar"}`)}, results; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong sync response: %s != %s", exp, act)
	}

	if err = h.Close(time.Second * 10); err != nil {
		t.Error(err)
	}
}
