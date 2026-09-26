// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcp

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"golang.org/x/text/encoding/charmap"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type gcpBigQueryCSVConfig struct {
	Header              []string
	FieldDelimiter      string
	AllowJaggedRows     bool
	AllowQuotedNewlines bool
	Encoding            string
	SkipLeadingRows     int
}

func gcpBigQueryCSVConfigFromParsed(conf *service.ParsedConfig) (csvconf gcpBigQueryCSVConfig, err error) {
	if csvconf.Header, err = conf.FieldStringList("header"); err != nil {
		return
	}
	if csvconf.FieldDelimiter, err = conf.FieldString("field_delimiter"); err != nil {
		return
	}
	if csvconf.AllowJaggedRows, err = conf.FieldBool("allow_jagged_rows"); err != nil {
		return
	}
	if csvconf.AllowQuotedNewlines, err = conf.FieldBool("allow_quoted_newlines"); err != nil {
		return
	}
	if csvconf.Encoding, err = conf.FieldString("encoding"); err != nil {
		return
	}
	if csvconf.SkipLeadingRows, err = conf.FieldInt("skip_leading_rows"); err != nil {
		return
	}
	return
}

type gcpBigQueryOutputConfig struct {
	JobProjectID        string
	ProjectID           string
	DatasetID           string
	TableID             string
	Format              string
	WriteDisposition    string
	CreateDisposition   string
	AutoDetect          bool
	IgnoreUnknownValues bool
	MaxBadRecords       int
	JobLabels           map[string]string
	CredentialsJSON     string

	// CSV options
	CSVOptions gcpBigQueryCSVConfig
}

func gcpBigQueryOutputConfigFromParsed(conf *service.ParsedConfig) (gconf gcpBigQueryOutputConfig, err error) {
	if gconf.ProjectID, err = conf.FieldString("project"); err != nil {
		return
	}
	if gconf.ProjectID == "" {
		gconf.ProjectID = bigquery.DetectProjectID
	}
	if gconf.JobProjectID, err = conf.FieldString("job_project"); err != nil {
		return
	}
	if gconf.JobProjectID == "" {
		gconf.JobProjectID = gconf.ProjectID
	}
	if gconf.DatasetID, err = conf.FieldString("dataset"); err != nil {
		return
	}
	if gconf.TableID, err = conf.FieldString("table"); err != nil {
		return
	}
	if gconf.Format, err = conf.FieldString("format"); err != nil {
		return
	}
	if gconf.WriteDisposition, err = conf.FieldString("write_disposition"); err != nil {
		return
	}
	if gconf.CreateDisposition, err = conf.FieldString("create_disposition"); err != nil {
		return
	}
	if gconf.IgnoreUnknownValues, err = conf.FieldBool("ignore_unknown_values"); err != nil {
		return
	}
	if gconf.MaxBadRecords, err = conf.FieldInt("max_bad_records"); err != nil {
		return
	}
	if gconf.AutoDetect, err = conf.FieldBool("auto_detect"); err != nil {
		return
	}
	if gconf.JobLabels, err = conf.FieldStringMap("job_labels"); err != nil {
		return
	}
	if gconf.CredentialsJSON, err = conf.FieldString("credentials_json"); err != nil {
		return
	}
	if gconf.CSVOptions, err = gcpBigQueryCSVConfigFromParsed(conf.Namespace("csv")); err != nil {
		return
	}
	return
}

type gcpBQClientURL string

func (g gcpBQClientURL) NewClient(ctx context.Context, conf gcpBigQueryOutputConfig) (*bigquery.Client, error) {
	if g == "" {
		var err error
		var opt []option.ClientOption
		opt, err = getClientOptionWithCredential(conf.CredentialsJSON, opt)
		if err != nil {
			return nil, err
		}
		return bigquery.NewClient(ctx, conf.JobProjectID, opt...)
	}
	return bigquery.NewClient(ctx, conf.JobProjectID, option.WithoutAuthentication(), option.WithEndpoint(string(g)))
}

func gcpBigQueryConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("GCP", "Services").
		Version("3.55.0").
		Summary(`Sends messages as new rows to a Google Cloud BigQuery table.`).
		Description(`
== Credentials

By default Redpanda Connect will use a shared credentials file when connecting to GCP services. You can find out more in xref:guides:cloud/gcp.adoc[].

== Format

This output currently supports only CSV, NEWLINE_DELIMITED_JSON and PARQUET, formats. Learn more about how to use GCP BigQuery with them here:

- ` + "https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json[`NEWLINE_DELIMITED_JSON`^]" + `
- ` + "https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv[`CSV`^]" + `
- ` + "https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-parquet[`PARQUET`^]" + `

Each message may contain multiple elements separated by newlines. For example a single message containing:

` + "```json" + `
{"key": "1"}
{"key": "2"}
` + "```" + `

Is equivalent to two separate messages:

` + "```json" + `
{"key": "1"}
` + "```" + `

And:

` + "```json" + `
{"key": "2"}
` + "```" + `

The same is true for the CSV format.

=== CSV

For the CSV format when the field ` + "`csv.header`" + ` is specified a header row will be inserted as the first line of each message batch. If this field is not provided then the first message of each message batch must include a header line.

=== Parquet

For parquet, the data can be encoded using the ` + "`parquet_encode`" + ` processor and each message that is sent to the output must be a full parquet message.

` + service.OutputPerformanceDocs(true, true)).
		Field(service.NewStringField("project").Description("Specify the project ID of the dataset to insert data into. If not set, the project ID is inferred from the project linked to the service account or read from the `GOOGLE_CLOUD_PROJECT` environment variable.").
			ShortDescription("The project ID of the dataset to insert into. Inferred from credentials or GOOGLE_CLOUD_PROJECT if unset.").Default("")).
		Field(service.NewStringField("job_project").Description("Specify the project ID in which jobs are executed. If not set, the `project` value is used.").Default("")).
		Field(service.NewStringField("dataset").Description("The BigQuery Dataset ID.")).
		Field(service.NewStringField("table").Description("The table to insert messages into.")).
		Field(service.NewStringEnumField("format", string(bigquery.JSON), string(bigquery.CSV), string(bigquery.Parquet)).
			Description("The format of each incoming message.").
			Default(string(bigquery.JSON))).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of message batches to have in flight at a given time. Increase this value to improve throughput.").
			Default(64)). // TODO: Tune this default
		Field(service.NewStringEnumField("write_disposition",
			string(bigquery.WriteAppend), string(bigquery.WriteEmpty), string(bigquery.WriteTruncate)).
			Description("Specifies how existing data in a destination table is treated.").
			Advanced().
			Default(string(bigquery.WriteAppend))).
		Field(service.NewStringEnumField("create_disposition", string(bigquery.CreateIfNeeded), string(bigquery.CreateNever)).
			Description(`Specifies the circumstances under which a destination table is created.

* Use ` + "`" + `CREATE_IF_NEEDED` + "`" + ` to create the destination table if it does not already exist. Tables are created atomically on successful completion of a job.
* Use ` + "`" + `CREATE_NEVER` + "`" + ` if the destination table must already exist. Tables are not created automatically.`).
			ShortDescription("When the destination table should be created, such as CREATE_IF_NEEDED.").
			Advanced().
			Default(string(bigquery.CreateIfNeeded))).
		Field(service.NewBoolField("ignore_unknown_values").
			Description(`Set this value to ` + "`" + `true` + "`" + ` to tolerate values that do not match the schema. Unknown values are ignored:

* For the ` + "`" + `CSV` + "`" + ` format, extra values at the end of a line are ignored.
* For the ` + "`" + `NEWLINE_DELIMITED_JSON` + "`" + ` format, values that do not match any column name are ignored.

By default, this value is set to ` + "`" + `false` + "`" + `, and records containing unknown values are treated as bad records. Use the ` + "`" + `max_bad_records` + "`" + ` field to customize how bad records are handled.`).
			ShortDescription("Tolerate values that do not match the schema, ignoring them rather than failing the write.").
			Advanced().
			Default(false)).
		Field(service.NewIntField("max_bad_records").
			Description("The maximum number of bad records that BigQuery ignores when reading data. This includes records with unknown values when `ignore_unknown_values` is `false`. If the number of bad records exceeds this value, the load job fails.").
			Advanced().
			Default(0)).
		Field(service.NewBoolField("auto_detect").
			Description(`Whether this component automatically infers the options and schema for ` + "`" + `CSV` + "`" + ` and ` + "`" + `NEWLINE_DELIMITED_JSON` + "`" + ` sources.

If this value is set to ` + "`" + `false` + "`" + ` and the destination table doesn't exist, the output throws an insertion error as it is unable to insert data.

CAUTION: This field delegates schema detection to the GCP BigQuery service. For the ` + "`" + `CSV` + "`" + ` format, values like ` + "`" + `no` + "`" + ` may be treated as booleans.`).
			ShortDescription("Automatically infer options and schema for CSV and JSON sources.").
			Advanced().
			Default(false)).
		Field(bqJobLabelsField("load")).
		Field(service.NewStringField("credentials_json").Description(`Sets the https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account[Google Service Account Credentials JSON^] (optional).

WARNING: When using xref:configuration:interpolation.adoc#bloblang-queries[interpolation functions] to populate this field, wrap the function in single quotes, not double quotes. For example, use ` + "`" + `'${secrets.GCP_CREDENTIALS_JSON}'` + "`" + ` instead of ` + "`" + `"${secrets.GCP_CREDENTIALS_JSON}"` + "`" + `. Double quotes cause JSON parsing errors because the credentials already contain JSON content.`).Secret().Default("")).
		Field(service.NewObjectField("csv",
			service.NewStringListField("header").
				Description("A list of values to use as the header for each batch of messages. If not specified, the first line of each message is used as the header.").
				ShortDescription("Values to use as the header for each batch. The first line of each message is used if unset.").
				Default([]any{}),
			service.NewStringField("field_delimiter").
				Description("The separator for fields in a CSV file. The output uses this value when reading or exporting data.").
				Default(","),
			service.NewBoolField("allow_jagged_rows").
				Description("Set to `true` to tolerate missing trailing optional columns in CSV data. Missing values are treated as nulls.").
				Advanced().
				Default(false),
			service.NewBoolField("allow_quoted_newlines").
				Description("Whether quoted data sections containing new lines are allowed when reading CSV data.").
				Advanced().
				Default(false),
			service.NewStringEnumField("encoding", string(bigquery.UTF_8), string(bigquery.ISO_8859_1)).
				Description("The character encoding of CSV data.").
				Advanced().
				Default(string(bigquery.UTF_8)),
			service.NewIntField("skip_leading_rows").
				Description("The number of rows at the top of a CSV file that BigQuery will skip when reading data. The default value is `1`, which allows Redpanda Connect to add the specified header in the first line of each batch sent to BigQuery.").
				ShortDescription("Number of rows at the top of a CSV file that BigQuery skips when reading.").
				Advanced().
				Default(1),
		).Description("Specify how CSV data is interpreted.")).
		Field(service.NewBatchPolicyField("batching"))
}

func init() {
	service.MustRegisterBatchOutput(
		"gcp_bigquery", gcpBigQueryConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (output service.BatchOutput, batchPol service.BatchPolicy, maxInFlight int, err error) {
			if batchPol, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			var gconf gcpBigQueryOutputConfig
			if gconf, err = gcpBigQueryOutputConfigFromParsed(conf); err != nil {
				return
			}
			output, err = newGCPBigQueryOutput(gconf, mgr.Logger())
			return
		})
}

type gcpBigQueryOutput struct {
	conf      gcpBigQueryOutputConfig
	clientURL gcpBQClientURL

	client  *bigquery.Client
	connMut sync.RWMutex

	fieldDelimiterBytes []byte
	csvHeaderBytes      []byte
	// if nil, then this is a format that we expect to be created upstream in a processor and each
	// message is a file that needs to be loaded.
	newLineBytes []byte

	log *service.Logger
}

func newGCPBigQueryOutput(
	conf gcpBigQueryOutputConfig,
	log *service.Logger,
) (*gcpBigQueryOutput, error) {
	g := &gcpBigQueryOutput{
		conf: conf,
		log:  log,
	}
	if conf.Format == string(bigquery.Parquet) {
		return g, nil
	}
	g.newLineBytes = []byte("\n")
	if conf.Format != string(bigquery.CSV) {
		return g, nil
	}

	g.fieldDelimiterBytes = []byte(conf.CSVOptions.FieldDelimiter)

	if len(conf.CSVOptions.Header) > 0 {
		header := fmt.Sprint("\"", strings.Join(conf.CSVOptions.Header, fmt.Sprint("\"", conf.CSVOptions.FieldDelimiter, "\"")), "\"")
		g.csvHeaderBytes = []byte(header)
	}

	if conf.CSVOptions.Encoding == string(bigquery.UTF_8) {
		return g, nil
	}

	var err error
	if g.fieldDelimiterBytes, err = convertToIso(g.fieldDelimiterBytes); err != nil {
		return nil, fmt.Errorf("error parsing csv.field_delimiter field: %w", err)
	}

	if g.newLineBytes, err = convertToIso([]byte("\n")); err != nil {
		return nil, fmt.Errorf("error creating newline bytes: %w", err)
	}

	if len(g.csvHeaderBytes) == 0 {
		return g, nil
	}

	if g.csvHeaderBytes, err = convertToIso(g.csvHeaderBytes); err != nil {
		return nil, fmt.Errorf("error parsing csv.header field: %w", err)
	}
	return g, nil
}

// convertToIso converts a utf-8 byte encoding to iso-8859-1 byte encoding.
func convertToIso(value []byte) (result []byte, err error) {
	return charmap.ISO8859_1.NewEncoder().Bytes(value)
}

func (g *gcpBigQueryOutput) Connect(ctx context.Context) (err error) {
	g.connMut.Lock()
	defer g.connMut.Unlock()

	var client *bigquery.Client
	if client, err = g.clientURL.NewClient(context.Background(), g.conf); err != nil {
		err = fmt.Errorf("error creating big query client: %w", err)
		return
	}
	defer func() {
		if err != nil {
			client.Close()
		}
	}()

	dataset := client.DatasetInProject(g.conf.ProjectID, g.conf.DatasetID)
	if _, err = dataset.Metadata(ctx); err != nil {
		if hasStatusCode(err, http.StatusNotFound) {
			err = fmt.Errorf("dataset does not exist: %v", g.conf.DatasetID)
		} else {
			err = fmt.Errorf("error checking dataset existence: %w", err)
		}
		return
	}

	if g.conf.CreateDisposition == string(bigquery.CreateNever) {
		table := dataset.Table(g.conf.TableID)
		if _, err = table.Metadata(ctx); err != nil {
			if hasStatusCode(err, http.StatusNotFound) {
				err = fmt.Errorf("table does not exist: %v", g.conf.TableID)
			} else {
				err = fmt.Errorf("error checking table existence: %w", err)
			}
			return
		}
	}

	g.client = client
	return nil
}

func hasStatusCode(err error, code int) bool {
	if e, ok := err.(*googleapi.Error); ok && e.Code == code {
		return true
	}
	return false
}

func (g *gcpBigQueryOutput) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	g.connMut.RLock()
	client := g.client
	g.connMut.RUnlock()
	if client == nil {
		return service.ErrNotConnected
	}

	if g.newLineBytes == nil {
		var batchErr *service.BatchError
		setErr := func(idx int, err error) {
			if batchErr == nil {
				batchErr = service.NewBatchError(batch, err)
			}
			batchErr = batchErr.Failed(idx, err)
		}
		jobs := map[int]*bigquery.Job{}
		for idx, msg := range batch {
			msgBytes, err := msg.AsBytes()
			if err != nil {
				setErr(idx, err)
				continue
			}
			job, err := g.createTableLoader(&msgBytes).Run(ctx)
			if err != nil {
				setErr(idx, err)
				continue
			}
			jobs[idx] = job
		}
		for idx, job := range jobs {
			status, err := job.Wait(ctx)
			if err != nil {
				setErr(idx, fmt.Errorf("error while waiting on bigquery job: %w", err))
				continue
			}
			if err = errorFromStatus(status); err != nil {
				setErr(idx, err)
			}
		}
		if batchErr != nil {
			return batchErr
		}
		return nil
	}

	var data bytes.Buffer

	if g.csvHeaderBytes != nil {
		_, _ = data.Write(g.csvHeaderBytes)
	}

	for _, msg := range batch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return err
		}
		if data.Len() > 0 {
			_, _ = data.Write(g.newLineBytes)
		}
		_, _ = data.Write(msgBytes)
	}

	dataBytes := data.Bytes()
	job, err := g.createTableLoader(&dataBytes).Run(ctx)
	if err != nil {
		return err
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("error while waiting on bigquery job: %w", err)
	}

	return errorFromStatus(status)
}

func (g *gcpBigQueryOutput) createTableLoader(data *[]byte) *bigquery.Loader {
	table := g.client.DatasetInProject(g.conf.ProjectID, g.conf.DatasetID).Table(g.conf.TableID)

	source := bigquery.NewReaderSource(bytes.NewReader(*data))
	source.SourceFormat = bigquery.DataFormat(g.conf.Format)
	source.AutoDetect = g.conf.AutoDetect
	source.IgnoreUnknownValues = g.conf.IgnoreUnknownValues
	source.MaxBadRecords = int64(g.conf.MaxBadRecords)

	if g.conf.Format == string(bigquery.CSV) {
		source.FieldDelimiter = g.conf.CSVOptions.FieldDelimiter
		source.AllowJaggedRows = g.conf.CSVOptions.AllowJaggedRows
		source.AllowQuotedNewlines = g.conf.CSVOptions.AllowQuotedNewlines
		source.Encoding = bigquery.Encoding(g.conf.CSVOptions.Encoding)
		source.SkipLeadingRows = int64(g.conf.CSVOptions.SkipLeadingRows)
	}

	loader := table.LoaderFrom(source)

	loader.CreateDisposition = bigquery.TableCreateDisposition(g.conf.CreateDisposition)
	loader.WriteDisposition = bigquery.TableWriteDisposition(g.conf.WriteDisposition)
	loader.Labels = g.conf.JobLabels

	return loader
}

func (g *gcpBigQueryOutput) Close(context.Context) error {
	g.connMut.Lock()
	if g.client != nil {
		g.client.Close()
		g.client = nil
	}
	g.connMut.Unlock()
	return nil
}
