package javascript

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessorBasic(t *testing.T) {
	conf, err := javascriptProcessorConfig().ParseYAML(`
code: |
  (() => {
    let foo = "hello world"
    benthos.v0_msg_set_string(benthos.v0_msg_as_string() + foo);
  })();
`, nil)
	require.NoError(t, err)

	proc, err := newJavascriptProcessorFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	bCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	resBatches, err := proc.ProcessBatch(bCtx, service.MessageBatch{
		service.NewMessage([]byte("first ")),
		service.NewMessage([]byte("second ")),
	})
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 2)

	resBytes, err := resBatches[0][0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "first hello world", string(resBytes))

	resBytes, err = resBatches[0][1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "second hello world", string(resBytes))

	require.NoError(t, proc.Close(bCtx))
}

func TestProcessorNoEncapsulation(t *testing.T) {
	conf, err := javascriptProcessorConfig().ParseYAML(`
code: 'benthos.v0_msg_set_string(benthos.v0_msg_as_string() + "hello world");'
`, nil)
	require.NoError(t, err)

	proc, err := newJavascriptProcessorFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	bCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	resBatches, err := proc.ProcessBatch(bCtx, service.MessageBatch{
		service.NewMessage([]byte("first ")),
		service.NewMessage([]byte("second ")),
	})
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 2)

	resBytes, err := resBatches[0][0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "first hello world", string(resBytes))

	resBytes, err = resBatches[0][1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "second hello world", string(resBytes))

	require.NoError(t, proc.Close(bCtx))
}

func TestProcessorMetadata(t *testing.T) {
	conf, err := javascriptProcessorConfig().ParseYAML(`
code: |
  (() => {
    benthos.v0_msg_set_meta("testa", "hello world");
    benthos.v0_msg_set_meta("testb", benthos.v0_msg_get_meta("testa") + " two");
    benthos.v0_msg_set_meta("testc", ["first","second"]);
    benthos.v0_msg_set_meta("testd", 123.4);
  })();
`, nil)
	require.NoError(t, err)

	proc, err := newJavascriptProcessorFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	bCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	resBatches, err := proc.ProcessBatch(bCtx, service.MessageBatch{
		service.NewMessage([]byte("first")),
	})
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 1)

	outMsg := resBatches[0][0]

	resBytes, err := outMsg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "first", string(resBytes))

	metV, exists := outMsg.MetaGetMut("testa")
	require.True(t, exists)
	assert.Equal(t, "hello world", metV)

	metV, exists = outMsg.MetaGetMut("testb")
	require.True(t, exists)
	assert.Equal(t, "hello world two", metV)

	metV, exists = outMsg.MetaGetMut("testc")
	require.True(t, exists)
	assert.Equal(t, []any{"first", "second"}, metV)

	metV, exists = outMsg.MetaGetMut("testd")
	require.True(t, exists)
	assert.Equal(t, 123.4, metV)

	require.NoError(t, proc.Close(bCtx))
}

func TestProcessorStructured(t *testing.T) {
	conf, err := javascriptProcessorConfig().ParseYAML(`
code: |
  (() => {
    let thing = benthos.v0_msg_as_structured();
    thing.num_keys = Object.keys(thing).length;
    delete thing["b"];
    benthos.v0_msg_set_structured(thing);
  })();
`, nil)
	require.NoError(t, err)

	proc, err := newJavascriptProcessorFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	bCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	resBatches, err := proc.ProcessBatch(bCtx, service.MessageBatch{
		service.NewMessage([]byte(`{"a":"a value","b":"b value"}`)),
	})
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 1)

	outMsg := resBatches[0][0]

	resBytes, err := outMsg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"a":"a value","num_keys":2}`, string(resBytes))

	require.NoError(t, proc.Close(bCtx))
}

func TestProcessorStructuredImut(t *testing.T) {
	conf, err := javascriptProcessorConfig().ParseYAML(`
code: |
  (() => {
    let thing = benthos.v0_msg_as_structured();
    thing.num_keys = Object.keys(thing).length;
    delete thing["b"];
    benthos.v0_msg_set_meta("result", thing);
  })();
`, nil)
	require.NoError(t, err)

	proc, err := newJavascriptProcessorFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	bCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	resBatches, err := proc.ProcessBatch(bCtx, service.MessageBatch{
		service.NewMessage([]byte(`{"a":"a value","b":"b value"}`)),
	})
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 1)

	outMsg := resBatches[0][0]

	resBytes, err := outMsg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"a":"a value","b":"b value"}`, string(resBytes))

	metV, exists := outMsg.MetaGetMut("result")
	require.True(t, exists)
	assert.Equal(t, map[string]any{
		"a":        "a value",
		"num_keys": int64(2),
	}, metV)

	require.NoError(t, proc.Close(bCtx))
}

func TestProcessorErrorHandling(t *testing.T) {
	conf, err := javascriptProcessorConfig().ParseYAML(`
code: |
  (() => {
    try {
      let thing = benthos.v0_msg_as_structured();
      benthos.v0_msg_set_meta("no_err", thing);
    } catch (e) {
      benthos.v0_msg_set_meta("err", e);
    }
  })();
`, nil)
	require.NoError(t, err)

	proc, err := newJavascriptProcessorFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	bCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	resBatches, err := proc.ProcessBatch(bCtx, service.MessageBatch{
		service.NewMessage([]byte(`not a structured message`)),
	})
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 1)

	outMsg := resBatches[0][0]

	resBytes, err := outMsg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `not a structured message`, string(resBytes))

	allMeta := map[string]any{}
	_ = outMsg.MetaWalkMut(func(key string, value any) error {
		allMeta[key] = value
		return nil
	})
	assert.Equal(t, map[string]any{
		"err": "invalid character 'o' in literal null (expecting 'u')",
	}, allMeta)

	require.NoError(t, proc.Close(bCtx))
}

func TestProcessorBasicFromFile(t *testing.T) {
	tmpDir := t.TempDir()
	require.NoError(t, os.WriteFile(path.Join(tmpDir, "foo.js"), []byte(`
(() => {
  let foo = "hello world"
  benthos.v0_msg_set_string(benthos.v0_msg_as_string() + foo);
})();
`), 0o644))

	conf, err := javascriptProcessorConfig().ParseYAML(fmt.Sprintf(`
file: %v
`, path.Join(tmpDir, "foo.js")), nil)
	require.NoError(t, err)

	proc, err := newJavascriptProcessorFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	bCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	resBatches, err := proc.ProcessBatch(bCtx, service.MessageBatch{
		service.NewMessage([]byte("first ")),
		service.NewMessage([]byte("second ")),
	})
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 2)

	resBytes, err := resBatches[0][0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "first hello world", string(resBytes))

	resBytes, err = resBatches[0][1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "second hello world", string(resBytes))

	require.NoError(t, proc.Close(bCtx))
}

func TestProcessorBasicFromModule(t *testing.T) {
	tmpDir := t.TempDir()
	// The file must have the .js extension and be imported without it using `require('blobber')`
	require.NoError(t, os.WriteFile(path.Join(tmpDir, "blobber.js"), []byte(`
function blobber() {
	return 'blobber module';
}

module.exports = blobber;
`), 0o644))

	conf, err := javascriptProcessorConfig().ParseYAML(fmt.Sprintf(`
code: |
  (() => {
    const blobber = require('blobber');

    benthos.v0_msg_set_string(benthos.v0_msg_as_string() + blobber());
  })();
global_folders: [ "%s" ]
`, tmpDir), nil)
	require.NoError(t, err)

	proc, err := newJavascriptProcessorFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	bCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	resBatches, err := proc.ProcessBatch(bCtx, service.MessageBatch{
		service.NewMessage([]byte("hello ")),
	})
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 1)

	resBytes, err := resBatches[0][0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "hello blobber module", string(resBytes))

	require.NoError(t, proc.Close(bCtx))
}

func TestProcessorHTTPFetch(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bodyBytes, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "nah", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte("echo: "))
		_, _ = w.Write(bytes.ToUpper(bodyBytes))
	}))

	conf, err := javascriptProcessorConfig().ParseYAML(fmt.Sprintf(`
code: |
  (() => {
    let foo = benthos.v0_fetch("%v", {}, "GET", benthos.v0_msg_as_string());
    benthos.v0_msg_set_string(foo.status.toString() + ": " + foo.body);
  })();
`, testServer.URL), nil)
	require.NoError(t, err)

	proc, err := newJavascriptProcessorFromConfig(conf, service.MockResources())
	require.NoError(t, err)

	bCtx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	resBatches, err := proc.ProcessBatch(bCtx, service.MessageBatch{
		service.NewMessage([]byte("first")),
		service.NewMessage([]byte("second")),
	})
	require.NoError(t, err)
	require.Len(t, resBatches, 1)
	require.Len(t, resBatches[0], 2)

	resBytes, err := resBatches[0][0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "200: echo: FIRST", string(resBytes))

	resBytes, err = resBatches[0][1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "200: echo: SECOND", string(resBytes))

	require.NoError(t, proc.Close(bCtx))
}
