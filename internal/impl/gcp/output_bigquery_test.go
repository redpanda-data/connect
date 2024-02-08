package gcp

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/benthosdev/benthos/v4/public/service"
)

func gcpBigQueryConfFromYAML(t *testing.T, yamlStr string) gcpBigQueryOutputConfig {
	t.Helper()
	spec := gcpBigQueryConfig()
	parsedConf, err := spec.ParseYAML(yamlStr, nil)
	require.NoError(t, err)

	conf, err := gcpBigQueryOutputConfigFromParsed(parsedConf)
	require.NoError(t, err)

	return conf
}

func TestNewGCPBigQueryOutputJsonNewLineOk(t *testing.T) {
	output, err := newGCPBigQueryOutput(gcpBigQueryOutputConfig{}, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
}

func TestNewGCPBigQueryOutputCsvDefaultConfigIsoOk(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)

	output, err := newGCPBigQueryOutput(config, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, ",", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvDefaultConfigUtfOk(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)

	output, err := newGCPBigQueryOutput(config, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, ",", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvCustomConfigIsoOk(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.FieldDelimiter = "¨"

	output, err := newGCPBigQueryOutput(config, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, "\xa8", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvCustomConfigUtfOk(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)
	config.CSVOptions.FieldDelimiter = "¨"

	output, err := newGCPBigQueryOutput(config, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, "¨", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvHeaderIsoOk(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.Header = []string{"a", "â", "ã", "ä"}

	output, err := newGCPBigQueryOutput(config, nil)

	require.NoError(t, err)
	require.Equal(t, "\"a\",\"\xe2\",\"\xe3\",\"\xe4\"", string(output.csvHeaderBytes))
}

func TestNewGCPBigQueryOutputCsvHeaderUtfOk(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Header = []string{"a", "â", "ã", "ä"}

	output, err := newGCPBigQueryOutput(config, nil)

	require.NoError(t, err)
	require.Equal(t, "\"a\",\"â\",\"ã\",\"ä\"", string(output.csvHeaderBytes))
}

func TestNewGCPBigQueryOutputCsvFieldDelimiterIsoError(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.FieldDelimiter = "\xa8"

	_, err := newGCPBigQueryOutput(config, nil)

	require.Error(t, err)
}

func TestNewGCPBigQueryOutputCsvHeaderIsoError(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: foo
dataset: bar
table: baz
`)
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.Header = []string{"\xa8"}

	_, err := newGCPBigQueryOutput(config, nil)

	require.Error(t, err)
}

func TestGCPBigQueryOutputConvertToIsoOk(t *testing.T) {
	value := "\"a\"¨\"â\"¨\"ã\"¨\"ä\""

	result, err := convertToIso([]byte(value))

	require.NoError(t, err)
	require.Equal(t, "\"a\"\xa8\"\xe2\"\xa8\"\xe3\"\xa8\"\xe4\"", string(result))
}

func TestGCPBigQueryOutputConvertToIsoError(t *testing.T) {
	value := "\xa8"

	_, err := convertToIso([]byte(value))
	require.Error(t, err)
}

func TestGCPBigQueryOutputDatasetDoNotExists(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	_, fakeStorageAddr := makeFakeStorageServer(t)

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)
	output.mwClientURL = gcpMWClientURL(fakeStorageAddr)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())

	require.EqualError(t, err, "dataset does not exist: dataset_meow")
}

func TestGCPBigQueryOutputDatasetDoNotExistsUnknownError(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	_, fakeStorageAddr := makeFakeStorageServer(t)

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)
	output.mwClientURL = gcpMWClientURL(fakeStorageAddr)

	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer done()

	err = output.Connect(ctx)
	defer output.Close(context.Background())

	require.Error(t, err)
	require.Contains(t, err.Error(), "googleapi: got HTTP response code 500 with body: {}")
}

func TestGCPBigQueryOutputTableDoNotExists(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/projects/project_meow/datasets/dataset_meow" {
				_, _ = w.Write([]byte(`{"id" : "dataset_meow"}`))

				return
			}

			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	_, fakeStorageAddr := makeFakeStorageServer(t)

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)
	output.mwClientURL = gcpMWClientURL(fakeStorageAddr)

	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer done()

	err = output.Connect(ctx)
	defer output.Close(context.Background())

	require.Error(t, err)
	require.Contains(t, err.Error(), "table does not exist: table_meow")
}

func TestGCPBigQueryOutputTableDoNotExistsUnknownError(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/projects/project_meow/datasets/dataset_meow" {
				_, _ = w.Write([]byte(`{"id" : "dataset_meow"}`))

				return
			}

			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	_, fakeStorageAddr := makeFakeStorageServer(t)

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)
	output.mwClientURL = gcpMWClientURL(fakeStorageAddr)

	ctx, done := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer done()

	err = output.Connect(ctx)
	defer output.Close(context.Background())

	require.Error(t, err)
	require.Contains(t, err.Error(), "googleapi: got HTTP response code 500 with body: {}")
}

func TestGCPBigQueryOutputConnectOk(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"id" : "dataset_meow"}`))
		}),
	)
	defer server.Close()

	_, fakeStorageAddr := makeFakeStorageServer(t)

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)
	output.mwClientURL = gcpMWClientURL(fakeStorageAddr)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())

	require.NoError(t, err)
}

func TestGCPBigQueryOutputWriteOk(t *testing.T) {
	serverCalledCount := 0
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			serverCalledCount++

			// checking dataset existence
			if r.URL.Path == "/projects/project_meow/datasets/dataset_meow" {
				_, _ = w.Write([]byte(`{"id" : "dataset_meow"}`))
				return
			}

			// checking table existence
			if r.URL.Path == "/projects/project_meow/datasets/dataset_meow/tables/table_meow" {
				_, _ = w.Write([]byte(`{
  "id": "table_meow",
  "schema": {
    "fields": [
      { "name": "what1", "type": "STRING" },
      { "name": "what2", "type": "INTEGER" },
      { "name": "what3", "type": "BOOLEAN" }
    ]
  }
}`))
				return
			}

			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	fakeStorageServer, fakeStorageAddr := makeFakeStorageServer(t)

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)
	output.mwClientURL = gcpMWClientURL(fakeStorageAddr)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())

	require.NoError(t, err)

	err = output.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"what1":"meow1","what2":1,"what3":true}`)),
		service.NewMessage([]byte(`{"what1":"meow2","what2":2,"what3":false}`)),
	})
	require.NoError(t, err)

	require.NotNil(t, fakeStorageServer.Data)

	require.Equal(t, 2, serverCalledCount)
	require.True(t, strings.Contains(string(fakeStorageServer.Data), `{"what1":"meow1","what2":1,"what3":true}`+"\n"+`{"what1":"meow2","what2":2,"what3":false}`))
}

func TestGCPBigQueryOutputWriteError(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// checking dataset existence
			if r.URL.Path == "/projects/project_meow/datasets/dataset_meow" {
				_, _ = w.Write([]byte(`{"id" : "dataset_meow"}`))
				return
			}

			// checking table existence
			if r.URL.Path == "/projects/project_meow/datasets/dataset_meow/tables/table_meow" {
				_, _ = w.Write([]byte(`{"id" : "table_meow"}`))
				return
			}

			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	_, fakeStorageAddr := makeFakeStorageServer(t)

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)
	output.mwClientURL = gcpMWClientURL(fakeStorageAddr)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())

	require.NoError(t, err)

	err = output.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"what1":"meow1","what2":1,"what3":true}`)),
		service.NewMessage([]byte(`{"what1":"meow2","what2":2,"what3":false}`)),
	})
	require.Error(t, err)
}

func makeFakeStorageServer(t *testing.T) (*fakeBigQueryWriteServer, string) {
	fakeServer := &fakeBigQueryWriteServer{}
	grpcServer := grpc.NewServer()
	storagepb.RegisterBigQueryWriteServer(grpcServer, fakeServer)

	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			log.Fatal(err)
		}
	}()
	return fakeServer, listener.Addr().String()
}
