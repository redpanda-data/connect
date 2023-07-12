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

// credentials_json value is base64 encoded valid json with dummy data
func TestGCPBigQueryConnectWithCredsJson(t *testing.T) {
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
credentials_json: |
  {
  "type": "service_account",
  "project_id": "idonotexistproject",
  "private_key_id": "nx803io2ut5gxah3px6tfy90wym9flc22o4thnty",
  "private_key": "-----BEGIN PRIVATE KEY-----\!!notarealprivatekey!!!nzspswasv6bta7annd2a7ihhm4lkab5bduj5ifbgfu8xrzwa6mdp80q42ga0fr5imc9h0q12mfj06kbu04c2wi6gyqw6leg5a4rfnhyiqvcg36hlu0zqxsqefia6q9sq0228dmg6mvplnzsp9m2pjuoy7ntqtdrxhsmywr8exs7xrtju1eya3rkeuw4okd4b24ioiuvp7pri88h2oyhs30nh1zl0qglyla37llc2yzlv8852de2uwyc93m0qii7uhwgqurz4ywv5gzxqh8zi5zgjp9nvqu7x51pvcsyqspa5kdy64gxgvl0a3xl5cgr242vkusmwndz8otnkjhf27i9ktab0nkvzty0pasr6j7cae1f4mgbrl05rxkacm600sxyh399vzpd175qgsv0d1s7prtv768thkeucxqvropebf630gcg868al12fk6lxwk8ptrwdnoyhrg1s9d9rk4keobcp8kzp352ew6y1xgkmfibzlefm01irto2u0u3ldck1wqoy0uzkqw080fnfej2e3786wpcbela3of6ehzxch80iwhep42cz8ijfsx4zdppkjz4xp7wfzu4r3d5inr3t1oq9bc6v1v0j2hnxqb3009t50z0lkkv09cwn31otx45c1pot960udde453c6q09ai0pkslugocw9tx1tzguh2vaf39rdmdj8nwhb2d2s5jypv74yfkt2h5546tdbprw3y2qx2guzpswr1yl5dtihz5jilw0oi7t476qhkjpwu4ufty6v2sofbfv3wxfjg86civc6gq4e0aaospoqp2wh8pdhiqfddtmfg2gtgtr48bpgpn1tg1sx9fban3ukemgs2ccl0rsj91jt92ck90gq2mlnuvsrr9xbdybys8k72dgkjqxwpws2mk6z92q1sccsvwcflqqju412gth49fbv7tomhm86g4tabdtjb9f0aevth0ztdcpr5fen7u88rw62rdlsnf5696m2c7lv4v\n-----END PRIVATE KEY-----\n",
  "client_email": "testme@idonotexistproject.iam.gserviceaccount.com",
  "client_id": "97357153224506693378",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/testme%40idonotexistproject.iam.gserviceaccount.com"
  }
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	output.clientURL = gcpBQClientURL(server.URL)

	opt, err := output.clientURL.getClientOptionsForOutputBQ(config.CredentialsJSON)
	defer output.Close(context.Background())

	require.NoError(t, err)
	require.Lenf(t, opt, 3, "Unexpected number of Client Options")

	actualCredsJSON := opt[2]
	expectedValue := option.WithCredentialsJSON([]byte(cleanCredsJSON(config.CredentialsJSON)))
	require.EqualValues(t, expectedValue, actualCredsJSON, "GCP Credentials Json not set as expected.")
}

// credentials_json value is base64 encoded valid json with dummy data
func TestGCPBigQueryConnectNoServerUrlAndWithEncCredsJSON(t *testing.T) {
	config := gcpBigQueryConfFromYAML(t, `
project: project_meow
dataset: dataset_meow
table: table_meow
credentials_json: |
  {
  "type": "service_account",
  "project_id": "idonotexistproject",
  "private_key_id": "nx803io2ut5gxah3px6tfy90wym9flc22o4thnty",
  "private_key": "-----BEGIN PRIVATE KEY-----\!!notarealprivatekey!!!nzspswasv6bta7annd2a7ihhm4lkab5bduj5ifbgfu8xrzwa6mdp80q42ga0fr5imc9h0q12mfj06kbu04c2wi6gyqw6leg5a4rfnhyiqvcg36hlu0zqxsqefia6q9sq0228dmg6mvplnzsp9m2pjuoy7ntqtdrxhsmywr8exs7xrtju1eya3rkeuw4okd4b24ioiuvp7pri88h2oyhs30nh1zl0qglyla37llc2yzlv8852de2uwyc93m0qii7uhwgqurz4ywv5gzxqh8zi5zgjp9nvqu7x51pvcsyqspa5kdy64gxgvl0a3xl5cgr242vkusmwndz8otnkjhf27i9ktab0nkvzty0pasr6j7cae1f4mgbrl05rxkacm600sxyh399vzpd175qgsv0d1s7prtv768thkeucxqvropebf630gcg868al12fk6lxwk8ptrwdnoyhrg1s9d9rk4keobcp8kzp352ew6y1xgkmfibzlefm01irto2u0u3ldck1wqoy0uzkqw080fnfej2e3786wpcbela3of6ehzxch80iwhep42cz8ijfsx4zdppkjz4xp7wfzu4r3d5inr3t1oq9bc6v1v0j2hnxqb3009t50z0lkkv09cwn31otx45c1pot960udde453c6q09ai0pkslugocw9tx1tzguh2vaf39rdmdj8nwhb2d2s5jypv74yfkt2h5546tdbprw3y2qx2guzpswr1yl5dtihz5jilw0oi7t476qhkjpwu4ufty6v2sofbfv3wxfjg86civc6gq4e0aaospoqp2wh8pdhiqfddtmfg2gtgtr48bpgpn1tg1sx9fban3ukemgs2ccl0rsj91jt92ck90gq2mlnuvsrr9xbdybys8k72dgkjqxwpws2mk6z92q1sccsvwcflqqju412gth49fbv7tomhm86g4tabdtjb9f0aevth0ztdcpr5fen7u88rw62rdlsnf5696m2c7lv4v\n-----END PRIVATE KEY-----\n",
  "client_email": "testme@idonotexistproject.iam.gserviceaccount.com",
  "client_id": "97357153224506693378",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/testme%40idonotexistproject.iam.gserviceaccount.com"
  }
`)

	output, err := newGCPBigQueryOutput(config, nil)
	require.NoError(t, err)

	opt, err := output.clientURL.getClientOptionsForOutputBQ(config.CredentialsJSON)
	defer output.Close(context.Background())

	require.NoError(t, err)
	require.Lenf(t, opt, 1, "Unexpected number of Client Options")

	actualCredsJSON := opt[0]
	expectedValue := option.WithCredentialsJSON([]byte(cleanCredsJSON(config.CredentialsJSON)))
	require.EqualValues(t, expectedValue, actualCredsJSON, "GCP Credentials Json not set as expected.")
}
