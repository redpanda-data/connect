package sr

import (
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	franz_sr "github.com/twmb/franz-go/pkg/sr"
	//"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type Schema struct {
	Name string `json:"name"`
}

var noopReqSign = func(fs.FS, *http.Request) error { return nil }

func mustJBytes(t testing.TB, obj any) []byte {
	t.Helper()
	b, err := json.Marshal(obj)
	require.NoError(t, err)
	return b
}

func TestWalkReferences(t *testing.T) {
	tCtx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	rootSchema := `[
  "benthos.namespace.com.foo",
  "benthos.namespace.com.bar",
  "benthos.namespace.com.baz"
]`

	fooSchema := `{
	"namespace": "benthos.namespace.com",
	"type": "record",
	"name": "foo",
	"fields": [
		{ "name": "Woof", "type": "string"}
	]
}`

	barSchema := `{
	"namespace": "benthos.namespace.com",
	"type": "record",
	"name": "bar",
	"fields": [
		{ "name": "Moo", "type": "string"}
	]
}`

	bazSchema := `{
	"namespace": "benthos.namespace.com",
	"type": "record",
	"name": "baz",
	"fields": [
		{ "name": "Miao", "type": "benthos.namespace.com.foo" }
	]
}`

	urlStr := runSchemaRegistryServer(t, func(path string) ([]byte, error) {
		switch path {
		case "/subjects/root/versions/1", "/schemas/ids/1":
			return mustJBytes(t, map[string]any{
				"id":         1,
				"version":    1,
				"schema":     rootSchema,
				"schemaType": "AVRO",
				"references": []any{
					map[string]any{"name": "benthos.namespace.com.foo", "subject": "foo", "version": 1},
					map[string]any{"name": "benthos.namespace.com.bar", "subject": "bar", "version": 1},
					map[string]any{"name": "benthos.namespace.com.baz", "subject": "baz", "version": 1},
				},
			}), nil
		case "/subjects/root2/versions/1", "/schemas/ids/5":
			return mustJBytes(t, map[string]any{
				"id":         5,
				"version":    1,
				"schema":     rootSchema,
				"schemaType": "AVRO",
				"references": []any{
					map[string]any{"name": "benthos.namespace.com.baz", "subject": "baz", "version": 1},
					map[string]any{"name": "benthos.namespace.com.bar", "subject": "bar", "version": 1},
					map[string]any{"name": "benthos.namespace.com.foo", "subject": "foo", "version": 1},
				},
			}), nil
		case "/subjects/root3/versions/1", "/schemas/ids/6":
			return mustJBytes(t, map[string]any{
				"id":         6,
				"version":    1,
				"schema":     rootSchema,
				"schemaType": "AVRO",
				"references": []any{
					map[string]any{"name": "benthos.namespace.com.bar", "subject": "bar", "version": 1},
					map[string]any{"name": "benthos.namespace.com.baz", "subject": "baz", "version": 1},
					map[string]any{"name": "benthos.namespace.com.foo", "subject": "foo", "version": 1},
				},
			}), nil

		case "/subjects/foo/versions/1", "/schemas/ids/2":
			return mustJBytes(t, map[string]any{
				"id": 2, "version": 1, "schemaType": "AVRO",
				"schema": fooSchema,
			}), nil
		case "/subjects/bar/versions/1", "/schemas/ids/3":
			return mustJBytes(t, map[string]any{
				"id": 3, "version": 1, "schemaType": "AVRO",
				"schema": barSchema,
			}), nil
		case "/subjects/baz/versions/1", "/schemas/ids/4":
			return mustJBytes(t, map[string]any {
				"id": 4, 
				"version": 1,
				"schema": bazSchema,
				"schemaType": "AVRO",
				"references": []any{
					map[string]any{"name": "benthos.namespace.com.foo", "subject": "foo", "version": 1},
				},
			}), nil
		}		
		return nil, nil
	})

	tests := []struct {
		name				string 
		schemaId    int
		output     	[]string
	}{
		{
			name: "root",
			schemaId: 1,
			output: []string{
				"benthos.namespace.com.foo",
				"benthos.namespace.com.bar",
  			"benthos.namespace.com.baz",
			},
		},
		{
			name: "foo",
			schemaId: 2,
			output: []string{},
		},
		{
			name: "baz",
			schemaId: 4,
			output: []string{
				"benthos.namespace.com.foo",
			},
		},
		{
			name: "root2",
			schemaId: 5,
			output: []string{
				"benthos.namespace.com.foo",
				"benthos.namespace.com.baz",
  			"benthos.namespace.com.bar",
			},
		},
		{
			name: "root3",
			schemaId: 6,
			output: []string{
				"benthos.namespace.com.bar",
				"benthos.namespace.com.foo",
  			"benthos.namespace.com.baz",
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			client, err := NewClient(urlStr, noopReqSign, nil, service.MockResources())
			require.NoError(t, err)
			schema, err := client.GetSchemaByID(tCtx, test.schemaId, false)
			require.NoError(t, err)

			schemas := []string{}
			walkErr := client.WalkReferences(tCtx, schema.References, func(ctx context.Context, name string, schema franz_sr.Schema) error {
				schemas = append(schemas, name)
				return nil
			});
			require.NoError(t, walkErr)
			require.Len(t, schemas, len(test.output))
			for i, name := range schemas {
				require.Equal(t, test.output[i], name)
			}
		})
	}
}


func runSchemaRegistryServer(t testing.TB, fn func(path string) ([]byte, error)) string {
	t.Helper()

	var reqMut sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqMut.Lock()
		defer reqMut.Unlock()

		b, err := fn(r.URL.EscapedPath())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(b) == 0 {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_, _ = w.Write(b)
	}))
	t.Cleanup(ts.Close)

	return ts.URL
}

