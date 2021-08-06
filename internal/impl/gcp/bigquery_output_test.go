package gcp

import (
	"context"
	"flag"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/stretchr/testify/require"
)

func TestNewGCPBigQueryOutputJsonNewLineOk(t *testing.T) {
	output, err := newGCPBigQueryOutput(output.GCPBigQueryConfig{}, nil, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
}

func TestNewGCPBigQueryOutputCsvDefaultConfigIsoOk(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)

	output, err := newGCPBigQueryOutput(config, nil, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, ",", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvDefaultConfigUtfOk(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)

	output, err := newGCPBigQueryOutput(config, nil, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, ",", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvCustomConfigIsoOk(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.FieldDelimiter = "¨"

	output, err := newGCPBigQueryOutput(config, nil, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, "\xa8", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvCustomConfigUtfOk(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)
	config.CSVOptions.FieldDelimiter = "¨"

	output, err := newGCPBigQueryOutput(config, nil, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, "¨", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvHeaderIsoOk(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.Header = []string{"a", "â", "ã", "ä"}

	output, err := newGCPBigQueryOutput(config, nil, nil)

	require.NoError(t, err)
	require.Equal(t, "\"a\",\"\xe2\",\"\xe3\",\"\xe4\"", string(output.csvHeaderBytes))
}

func TestNewGCPBigQueryOutputCsvHeaderUtfOk(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Header = []string{"a", "â", "ã", "ä"}

	output, err := newGCPBigQueryOutput(config, nil, nil)

	require.NoError(t, err)
	require.Equal(t, "\"a\",\"â\",\"ã\",\"ä\"", string(output.csvHeaderBytes))
}

func TestNewGCPBigQueryOutputCsvFieldDelimiterIsoError(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.FieldDelimiter = "\xa8"

	_, err := newGCPBigQueryOutput(config, nil, nil)

	require.Error(t, err)
}

func TestNewGCPBigQueryOutputCsvHeaderIsoError(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.Header = []string{"\xa8"}

	_, err := newGCPBigQueryOutput(config, nil, nil)

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
	// Setting non-default values
	outputConfig := output.NewGCPBigQueryConfig()
	outputConfig.ProjectID = "project_meow"
	outputConfig.DatasetID = "dataset_meow"
	outputConfig.TableID = "table_meow"
	outputConfig.WriteDisposition = string(bigquery.WriteTruncate)
	outputConfig.CreateDisposition = string(bigquery.CreateNever)
	outputConfig.Format = string(bigquery.CSV)
	outputConfig.AutoDetect = true
	outputConfig.IgnoreUnknownValues = true
	outputConfig.MaxBadRecords = 123
	outputConfig.CSVOptions.FieldDelimiter = ";"
	outputConfig.CSVOptions.AllowJaggedRows = true
	outputConfig.CSVOptions.AllowQuotedNewlines = true
	outputConfig.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	outputConfig.CSVOptions.SkipLeadingRows = 10

	output, err := newGCPBigQueryOutput(outputConfig, nil, nil)

	require.NoError(t, err)

	var data = []byte("1,2,3")
	loader := output.createTableLoader(&data)

	require.Equal(t, "table_meow", loader.Dst.TableID)
	require.Equal(t, "dataset_meow", loader.Dst.DatasetID)
	require.Equal(t, "project_meow", loader.Dst.ProjectID)
	require.Equal(t, bigquery.TableWriteDisposition(outputConfig.WriteDisposition), loader.WriteDisposition)
	require.Equal(t, bigquery.TableCreateDisposition(outputConfig.CreateDisposition), loader.CreateDisposition)

	readerSource, ok := loader.Src.(*bigquery.ReaderSource)
	require.True(t, ok)

	require.Equal(t, bigquery.DataFormat(outputConfig.Format), readerSource.SourceFormat)
	require.Equal(t, outputConfig.AutoDetect, readerSource.AutoDetect)
	require.Equal(t, outputConfig.IgnoreUnknownValues, readerSource.IgnoreUnknownValues)
	require.Equal(t, outputConfig.MaxBadRecords, readerSource.MaxBadRecords)

	expectedCsvOptions := outputConfig.CSVOptions

	require.Equal(t, expectedCsvOptions.FieldDelimiter, readerSource.FieldDelimiter)
	require.Equal(t, expectedCsvOptions.AllowJaggedRows, readerSource.AllowJaggedRows)
	require.Equal(t, expectedCsvOptions.AllowQuotedNewlines, readerSource.AllowQuotedNewlines)
	require.Equal(t, bigquery.Encoding(expectedCsvOptions.Encoding), readerSource.Encoding)
	require.Equal(t, expectedCsvOptions.SkipLeadingRows, readerSource.SkipLeadingRows)
}

func TestGCPBigQueryOutputIntegration(t *testing.T) {
	// This is an integration test because httptest may take a while to execute

	if m := flag.Lookup("test.run").Value.String(); m == "" || regexp.MustCompile(strings.Split(m, "/")[0]).FindString(t.Name()) == "" {
		t.Skip("Skipping as execution was not requested explicitly using go test -run ^TestIntegration$")
	}

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	t.Run("testDatasetDoNotExists", func(t *testing.T) {
		testDatasetDoNotExists(t)
	})

	t.Run("testDatasetDoNotExistsUnknownError", func(t *testing.T) {
		testDatasetDoNotExistsUnknownError(t)
	})

	t.Run("testTableDoNotExists", func(t *testing.T) {
		testTableDoNotExists(t)
	})

	t.Run("testTableDoNotExistsUnknownError", func(t *testing.T) {
		testTableDoNotExistsUnknownError(t)
	})

	t.Run("testConnectOk", func(t *testing.T) {
		testConnectOk(t)
	})

	t.Run("testConnectWithoutTableOk", func(t *testing.T) {
		testConnectWithoutTableOk(t)
	})

	t.Run("testWriteOk", func(t *testing.T) {
		testWriteOk(t)
	})

	t.Run("testWriteError", func(t *testing.T) {
		testWriteError(t)
	})
}

func testDatasetDoNotExists(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	config := output.NewGCPBigQueryConfig()
	config.ProjectID = "project_meow"
	config.DatasetID = "dataset_meow"

	output, err := newGCPBigQueryOutput(config, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	output.clientConf.url = server.URL

	err = output.ConnectWithContext(context.Background())
	defer output.CloseAsync()

	require.EqualError(t, err, "dataset does not exists: dataset_meow")
}

func testDatasetDoNotExistsUnknownError(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	config := output.NewGCPBigQueryConfig()
	config.ProjectID = "project_meow"
	config.DatasetID = "dataset_meow"

	output, err := newGCPBigQueryOutput(config, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	output.clientConf.url = server.URL

	err = output.ConnectWithContext(context.Background())
	defer output.CloseAsync()

	require.EqualError(t, err, "error checking dataset existence: googleapi: got HTTP response code 500 with body: {}")
}

func testTableDoNotExists(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/projects/project_meow/datasets/dataset_meow" {
				w.Write([]byte(`{"id" : "dataset_meow"}`))

				return
			}

			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	config := output.NewGCPBigQueryConfig()
	config.ProjectID = "project_meow"
	config.DatasetID = "dataset_meow"
	config.TableID = "table_meow"
	config.CreateDisposition = string(bigquery.CreateNever)

	output, err := newGCPBigQueryOutput(config, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	output.clientConf.url = server.URL

	err = output.ConnectWithContext(context.Background())
	defer output.CloseAsync()

	require.EqualError(t, err, "table does not exists: table_meow")
}

func testTableDoNotExistsUnknownError(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/projects/project_meow/datasets/dataset_meow" {
				w.Write([]byte(`{"id" : "dataset_meow"}`))

				return
			}

			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	config := output.NewGCPBigQueryConfig()
	config.ProjectID = "project_meow"
	config.DatasetID = "dataset_meow"
	config.TableID = "table_meow"
	config.CreateDisposition = string(bigquery.CreateNever)

	output, err := newGCPBigQueryOutput(config, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	output.clientConf.url = server.URL

	err = output.ConnectWithContext(context.Background())
	defer output.CloseAsync()

	require.EqualError(t, err, "error checking table existence: googleapi: got HTTP response code 500 with body: {}")
}

func testConnectOk(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte(`{"id" : "dataset_meow"}`))
		}),
	)
	defer server.Close()

	config := output.NewGCPBigQueryConfig()
	config.ProjectID = "project_meow"
	config.DatasetID = "dataset_meow"

	output, err := newGCPBigQueryOutput(config, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	output.clientConf.url = server.URL

	err = output.ConnectWithContext(context.Background())
	defer output.CloseAsync()
	require.NoError(t, err)
}

func testConnectWithoutTableOk(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/projects/project_meow/datasets/dataset_meow" {
				w.Write([]byte(`{"id" : "dataset_meow"}`))

				return
			}

			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	config := output.NewGCPBigQueryConfig()
	config.ProjectID = "project_meow"
	config.DatasetID = "dataset_meow"

	output, err := newGCPBigQueryOutput(config, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	output.clientConf.url = server.URL

	err = output.ConnectWithContext(context.Background())
	defer output.CloseAsync()
	require.NoError(t, err)
}

func testWriteOk(t *testing.T) {
	serverCalledCount := 0
	var body []byte
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			serverCalledCount++

			// checking dataset existence
			if r.URL.Path == "/projects/project_meow/datasets/dataset_meow" {
				w.Write([]byte(`{"id" : "dataset_meow"}`))
				return
			}

			// job execution called with job.Run()
			if r.URL.Path == "/upload/bigquery/v2/projects/project_meow/jobs" {
				var err error
				body, err = ioutil.ReadAll(r.Body)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					return
				}

				w.Write([]byte(`{"jobReference" : {"jobId" : "1"}}`))
				return
			}

			// job status called with job.Wait()
			if r.URL.Path == "/projects/project_meow/jobs/1" {
				w.Write([]byte(`{"status":{"state":"DONE"}}`))
				return
			}

			w.WriteHeader(http.StatusNotFound)
			w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	config := output.NewGCPBigQueryConfig()
	config.ProjectID = "project_meow"
	config.DatasetID = "dataset_meow"
	config.TableID = "table_meow"

	output, err := newGCPBigQueryOutput(config, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	output.clientConf.url = server.URL

	err = output.ConnectWithContext(context.Background())
	defer output.CloseAsync()
	require.NoError(t, err)

	err = output.WriteWithContext(context.Background(), message.New([][]byte{
		[]byte(`{"what1":"meow1","what2":1,"what3":true}`),
		[]byte(`{"what1":"meow2","what2":2,"what3":false}`),
	}))
	require.NoError(t, err)

	require.NotNil(t, body)

	require.Equal(t, 3, serverCalledCount)

	require.True(t, strings.Contains(string(body), `{"what1":"meow1","what2":1,"what3":true}`+"\n"+`{"what1":"meow2","what2":2,"what3":false}`))
}

func testWriteError(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// checking dataset existence
			if r.URL.Path == "/projects/project_meow/datasets/dataset_meow" {
				w.Write([]byte(`{"id" : "dataset_meow"}`))
				return
			}

			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("{}"))
		}),
	)
	defer server.Close()

	config := output.NewGCPBigQueryConfig()
	config.ProjectID = "project_meow"
	config.DatasetID = "dataset_meow"
	config.TableID = "table_meow"

	output, err := newGCPBigQueryOutput(config, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	output.clientConf.url = server.URL

	err = output.ConnectWithContext(context.Background())
	defer output.CloseAsync()
	require.NoError(t, err)

	err = output.WriteWithContext(context.Background(), message.New([][]byte{
		[]byte(`{"what1":"meow1","what2":1,"what3":true}`),
		[]byte(`{"what1":"meow2","what2":2,"what3":false}`),
	}))
	require.Error(t, err)
}
