package gcp

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"

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

// credentials_json_encoded value is base64 encoded valid json with dummy data
func TestGCPBigQueryConnectWithCredsJSON(t *testing.T) {
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
credentials_json_encoded: ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiaWRvbm90ZXhpc3Rwcm9qZWN0IiwKICAicHJpdmF0ZV9rZXlfaWQiOiAibng4MDNpbzJ1dDVneGFoM3B4NnRmeTkwd3ltOWZsYzIybzR0aG50eSIsCiAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVwhIW5vdGFyZWFscHJpdmF0ZWtleSEhIW56c3Bzd2FzdjZidGE3YW5uZDJhN2loaG00bGthYjViZHVqNWlmYmdmdTh4cnp3YTZtZHA4MHE0MmdhMGZyNWltYzloMHExMm1majA2a2J1MDRjMndpNmd5cXc2bGVnNWE0cmZuaHlpcXZjZzM2aGx1MHpxeHNxZWZpYTZxOXNxMDIyOGRtZzZtdnBsbnpzcDltMnBqdW95N250cXRkcnhoc215d3I4ZXhzN3hydGp1MWV5YTNya2V1dzRva2Q0YjI0aW9pdXZwN3ByaTg4aDJveWhzMzBuaDF6bDBxZ2x5bGEzN2xsYzJ5emx2ODg1MmRlMnV3eWM5M20wcWlpN3Vod2dxdXJ6NHl3djVnenhxaDh6aTV6Z2pwOW52cXU3eDUxcHZjc3lxc3BhNWtkeTY0Z3hndmwwYTN4bDVjZ3IyNDJ2a3VzbXduZHo4b3Rua2poZjI3aTlrdGFiMG5rdnp0eTBwYXNyNmo3Y2FlMWY0bWdicmwwNXJ4a2FjbTYwMHN4eWgzOTl2enBkMTc1cWdzdjBkMXM3cHJ0djc2OHRoa2V1Y3hxdnJvcGViZjYzMGdjZzg2OGFsMTJmazZseHdrOHB0cndkbm95aHJnMXM5ZDlyazRrZW9iY3A4a3pwMzUyZXc2eTF4Z2ttZmliemxlZm0wMWlydG8ydTB1M2xkY2sxd3FveTB1emtxdzA4MGZuZmVqMmUzNzg2d3BjYmVsYTNvZjZlaHp4Y2g4MGl3aGVwNDJjejhpamZzeDR6ZHBwa2p6NHhwN3dmenU0cjNkNWlucjN0MW9xOWJjNnYxdjBqMmhueHFiMzAwOXQ1MHowbGtrdjA5Y3duMzFvdHg0NWMxcG90OTYwdWRkZTQ1M2M2cTA5YWkwcGtzbHVnb2N3OXR4MXR6Z3VoMnZhZjM5cmRtZGo4bndoYjJkMnM1anlwdjc0eWZrdDJoNTU0NnRkYnBydzN5MnF4Mmd1enBzd3IxeWw1ZHRpaHo1amlsdzBvaTd0NDc2cWhranB3dTR1ZnR5NnYyc29mYmZ2M3d4ZmpnODZjaXZjNmdxNGUwYWFvc3BvcXAyd2g4cGRoaXFmZGR0bWZnMmd0Z3RyNDhicGdwbjF0ZzFzeDlmYmFuM3VrZW1nczJjY2wwcnNqOTFqdDkyY2s5MGdxMm1sbnV2c3JyOXhiZHlieXM4azcyZGdranF4d3B3czJtazZ6OTJxMXNjY3N2d2NmbHFxanU0MTJndGg0OWZidjd0b21obTg2ZzR0YWJkdGpiOWYwYWV2dGgwenRkY3ByNWZlbjd1ODhydzYycmRsc25mNTY5Nm0yYzdsdjR2XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLAogICJjbGllbnRfZW1haWwiOiAidGVzdG1lQGlkb25vdGV4aXN0cHJvamVjdC5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgImNsaWVudF9pZCI6ICI5NzM1NzE1MzIyNDUwNjY5MzM3OCIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvdGVzdG1lJTQwaWRvbm90ZXhpc3Rwcm9qZWN0LmlhbS5nc2VydmljZWFjY291bnQuY29tIgp9
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)

	opt, err := output.clientURL.buildClientOptions(config.CredentialsJSON)
	defer output.Close(context.Background())

	require.NoError(t, err)
	require.Lenf(t, opt, 2, "Unexpected number of Client Options")

	actualCredsJSON := opt[0]
	decodedCred, _ := base64.StdEncoding.DecodeString(config.CredentialsJSON)
	expectedValue := option.WithCredentialsJSON(decodedCred)
	require.EqualValues(t, expectedValue, actualCredsJSON, "GCP Credentials JSON not set as expected.")
}

// credentials_json_encoded value is base64 encoded valid json with dummy data
func TestGCPBigQueryConnectNoServerUrlAndWithEncCredsJSON(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
credentials_json_encoded: ewogICJ0eXBlIjogInNlcnZpY2VfYWNjb3VudCIsCiAgInByb2plY3RfaWQiOiAiaWRvbm90ZXhpc3Rwcm9qZWN0IiwKICAicHJpdmF0ZV9rZXlfaWQiOiAibng4MDNpbzJ1dDVneGFoM3B4NnRmeTkwd3ltOWZsYzIybzR0aG50eSIsCiAgInByaXZhdGVfa2V5IjogIi0tLS0tQkVHSU4gUFJJVkFURSBLRVktLS0tLVwhIW5vdGFyZWFscHJpdmF0ZWtleSEhIW56c3Bzd2FzdjZidGE3YW5uZDJhN2loaG00bGthYjViZHVqNWlmYmdmdTh4cnp3YTZtZHA4MHE0MmdhMGZyNWltYzloMHExMm1majA2a2J1MDRjMndpNmd5cXc2bGVnNWE0cmZuaHlpcXZjZzM2aGx1MHpxeHNxZWZpYTZxOXNxMDIyOGRtZzZtdnBsbnpzcDltMnBqdW95N250cXRkcnhoc215d3I4ZXhzN3hydGp1MWV5YTNya2V1dzRva2Q0YjI0aW9pdXZwN3ByaTg4aDJveWhzMzBuaDF6bDBxZ2x5bGEzN2xsYzJ5emx2ODg1MmRlMnV3eWM5M20wcWlpN3Vod2dxdXJ6NHl3djVnenhxaDh6aTV6Z2pwOW52cXU3eDUxcHZjc3lxc3BhNWtkeTY0Z3hndmwwYTN4bDVjZ3IyNDJ2a3VzbXduZHo4b3Rua2poZjI3aTlrdGFiMG5rdnp0eTBwYXNyNmo3Y2FlMWY0bWdicmwwNXJ4a2FjbTYwMHN4eWgzOTl2enBkMTc1cWdzdjBkMXM3cHJ0djc2OHRoa2V1Y3hxdnJvcGViZjYzMGdjZzg2OGFsMTJmazZseHdrOHB0cndkbm95aHJnMXM5ZDlyazRrZW9iY3A4a3pwMzUyZXc2eTF4Z2ttZmliemxlZm0wMWlydG8ydTB1M2xkY2sxd3FveTB1emtxdzA4MGZuZmVqMmUzNzg2d3BjYmVsYTNvZjZlaHp4Y2g4MGl3aGVwNDJjejhpamZzeDR6ZHBwa2p6NHhwN3dmenU0cjNkNWlucjN0MW9xOWJjNnYxdjBqMmhueHFiMzAwOXQ1MHowbGtrdjA5Y3duMzFvdHg0NWMxcG90OTYwdWRkZTQ1M2M2cTA5YWkwcGtzbHVnb2N3OXR4MXR6Z3VoMnZhZjM5cmRtZGo4bndoYjJkMnM1anlwdjc0eWZrdDJoNTU0NnRkYnBydzN5MnF4Mmd1enBzd3IxeWw1ZHRpaHo1amlsdzBvaTd0NDc2cWhranB3dTR1ZnR5NnYyc29mYmZ2M3d4ZmpnODZjaXZjNmdxNGUwYWFvc3BvcXAyd2g4cGRoaXFmZGR0bWZnMmd0Z3RyNDhicGdwbjF0ZzFzeDlmYmFuM3VrZW1nczJjY2wwcnNqOTFqdDkyY2s5MGdxMm1sbnV2c3JyOXhiZHlieXM4azcyZGdranF4d3B3czJtazZ6OTJxMXNjY3N2d2NmbHFxanU0MTJndGg0OWZidjd0b21obTg2ZzR0YWJkdGpiOWYwYWV2dGgwenRkY3ByNWZlbjd1ODhydzYycmRsc25mNTY5Nm0yYzdsdjR2XG4tLS0tLUVORCBQUklWQVRFIEtFWS0tLS0tXG4iLAogICJjbGllbnRfZW1haWwiOiAidGVzdG1lQGlkb25vdGV4aXN0cHJvamVjdC5pYW0uZ3NlcnZpY2VhY2NvdW50LmNvbSIsCiAgImNsaWVudF9pZCI6ICI5NzM1NzE1MzIyNDUwNjY5MzM3OCIsCiAgImF1dGhfdXJpIjogImh0dHBzOi8vYWNjb3VudHMuZ29vZ2xlLmNvbS9vL29hdXRoMi9hdXRoIiwKICAidG9rZW5fdXJpIjogImh0dHBzOi8vb2F1dGgyLmdvb2dsZWFwaXMuY29tL3Rva2VuIiwKICAiYXV0aF9wcm92aWRlcl94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL29hdXRoMi92MS9jZXJ0cyIsCiAgImNsaWVudF94NTA5X2NlcnRfdXJsIjogImh0dHBzOi8vd3d3Lmdvb2dsZWFwaXMuY29tL3JvYm90L3YxL21ldGFkYXRhL3g1MDkvdGVzdG1lJTQwaWRvbm90ZXhpc3Rwcm9qZWN0LmlhbS5nc2VydmljZWFjY291bnQuY29tIgp9
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	var opt []option.ClientOption
	opt, err = output.clientURL.buildClientOptions(config.CredentialsJSON)
	defer output.Close(context.Background())

	require.NoError(t, err)
	require.Lenf(t, opt, 2, "Unexpected number of Client Options")

	actualCredsJSON := opt[0]
	decodedCred, _ := base64.StdEncoding.DecodeString(config.CredentialsJSON)
	expectedValue := option.WithCredentialsJSON(decodedCred)
	require.EqualValues(t, expectedValue, actualCredsJSON, "GCP Credentials JSON not set as expected.")
}

func TestGCPBigQueryConnectNoCredsJSON(t *testing.T) {
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

	opt, err := output.clientURL.buildClientOptions(config.CredentialsJSON)
	defer output.Close(context.Background())

	require.NoError(t, err)
	require.Lenf(t, opt, 2, "Unexpected number of Client Options")

	actualCredsJSON := opt[0]
	expectedValue := option.WithoutAuthentication()
	require.EqualValues(t, expectedValue, actualCredsJSON, "Expected withoutAuthentication() as option when Creds Json is not mentioned.")
}
