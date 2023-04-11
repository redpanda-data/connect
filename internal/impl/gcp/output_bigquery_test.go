package gcp

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

func TestGCPBigQueryOutputCreateTableLoaderOk(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"id" : "dataset_meow"}`))
		}),
	)
	defer server.Close()

	// Setting non-default values
	outputConfig := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
write_disposition: WRITE_TRUNCATE
create_disposition: CREATE_NEVER
format: CSV
auto_detect: true
ignore_unknown_values: true
max_bad_records: 123
csv:
  field_delimiter: ';'
  allow_jagged_rows: true
  allow_quoted_newlines: true
  encoding: ISO-8859-1
  skip_leading_rows: 10
`)

	output, err := newGCPBigQueryOutput(outputConfig, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)
	err = output.Connect(context.Background())
	defer output.Close(context.Background())
	require.NoError(t, err)

	data := []byte("1,2,3")
	loader := output.createTableLoader(&data)

	assert.Equal(t, "table_meow", loader.Dst.TableID)
	assert.Equal(t, "dataset_meow", loader.Dst.DatasetID)
	assert.Equal(t, "project_meow", loader.Dst.ProjectID)
	assert.Equal(t, bigquery.TableWriteDisposition(outputConfig.WriteDisposition), loader.WriteDisposition)
	assert.Equal(t, bigquery.TableCreateDisposition(outputConfig.CreateDisposition), loader.CreateDisposition)

	readerSource, ok := loader.Src.(*bigquery.ReaderSource)
	require.True(t, ok)

	assert.Equal(t, bigquery.DataFormat(outputConfig.Format), readerSource.SourceFormat)
	assert.Equal(t, outputConfig.AutoDetect, readerSource.AutoDetect)
	assert.Equal(t, outputConfig.IgnoreUnknownValues, readerSource.IgnoreUnknownValues)
	assert.Equal(t, int64(outputConfig.MaxBadRecords), readerSource.MaxBadRecords)

	expectedCsvOptions := outputConfig.CSVOptions

	assert.Equal(t, expectedCsvOptions.FieldDelimiter, readerSource.FieldDelimiter)
	assert.Equal(t, expectedCsvOptions.AllowJaggedRows, readerSource.AllowJaggedRows)
	assert.Equal(t, expectedCsvOptions.AllowQuotedNewlines, readerSource.AllowQuotedNewlines)
	assert.Equal(t, bigquery.Encoding(expectedCsvOptions.Encoding), readerSource.Encoding)
	assert.Equal(t, int64(expectedCsvOptions.SkipLeadingRows), readerSource.SkipLeadingRows)
}

func TestGCPBigQueryOutputDatasetDoNotExists(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)

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

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)

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

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
create_disposition: CREATE_NEVER
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)

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

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
create_disposition: CREATE_NEVER
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)

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

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())

	require.NoError(t, err)
}

func TestGCPBigQueryOutputConnectWithoutTableOk(t *testing.T) {
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

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())

	require.NoError(t, err)
}

func TestGCPBigQueryOutputWriteOk(t *testing.T) {
	serverCalledCount := 0
	var body []byte
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			serverCalledCount++

			// checking dataset existence
			if r.URL.Path == "/projects/project_meow/datasets/dataset_meow" {
				_, _ = w.Write([]byte(`{"id" : "dataset_meow"}`))
				return
			}

			// job execution called with job.Run()
			if r.URL.Path == "/upload/bigquery/v2/projects/project_meow/jobs" {
				var err error
				body, err = io.ReadAll(r.Body)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				_, _ = w.Write([]byte(`{"jobReference" : {"jobId" : "1"}}`))
				return
			}

			// job status called with job.Wait()
			if r.URL.Path == "/projects/project_meow/jobs/1" {
				_, _ = w.Write([]byte(`{"status":{"state":"DONE"}}`))
				return
			}

			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())

	require.NoError(t, err)

	err = output.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"what1":"meow1","what2":1,"what3":true}`)),
		service.NewMessage([]byte(`{"what1":"meow2","what2":2,"what3":false}`)),
	})
	require.NoError(t, err)

	require.NotNil(t, body)

	require.Equal(t, 3, serverCalledCount)

	require.True(t, strings.Contains(string(body), `{"what1":"meow1","what2":1,"what3":true}`+"\n"+`{"what1":"meow2","what2":2,"what3":false}`))
}

func TestGCPBigQueryOutputWriteError(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// checking dataset existence
			if r.URL.Path == "/projects/project_meow/datasets/dataset_meow" {
				_, _ = w.Write([]byte(`{"id" : "dataset_meow"}`))
				return
			}

			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)

	err = output.Connect(context.Background())
	defer output.Close(context.Background())

	require.NoError(t, err)

	err = output.WriteBatch(context.Background(), service.MessageBatch{
		service.NewMessage([]byte(`{"what1":"meow1","what2":1,"what3":true}`)),
		service.NewMessage([]byte(`{"what1":"meow2","what2":2,"what3":false}`)),
	})
	require.Error(t, err)
}
