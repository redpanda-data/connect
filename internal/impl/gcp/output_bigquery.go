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

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/public/service"
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
	if gconf.CSVOptions, err = gcpBigQueryCSVConfigFromParsed(conf.Namespace("csv")); err != nil {
		return
	}
	return
}

type gcpBQClientURL string

func (g gcpBQClientURL) NewClient(ctx context.Context, projectID string) (*bigquery.Client, error) {
	if g == "" {
		return bigquery.NewClient(ctx, projectID)
	}
	return bigquery.NewClient(ctx, projectID, option.WithoutAuthentication(), option.WithEndpoint(string(g)))
}

func gcpBigQueryConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("GCP", "Services").
		Version("3.55.0").
		Summary(`Sends messages as new rows to a Google Cloud BigQuery table.`).
		Description(output.Description(true, true, `
## Credentials

By default Benthos will use a shared credentials file when connecting to GCP services. You can find out more [in this document](/docs/guides/cloud/gcp).

## Format

This output currently supports only CSV and NEWLINE_DELIMITED_JSON formats. Learn more about how to use GCP BigQuery with them here:
- `+"[`NEWLINE_DELIMITED_JSON`](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json)"+`
- `+"[`CSV`](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv)"+`

Each message may contain multiple elements separated by newlines. For example a single message containing:

`+"```json"+`
{"key": "1"}
{"key": "2"}
`+"```"+`

Is equivalent to two separate messages:

`+"```json"+`
{"key": "1"}
`+"```"+`

And:

`+"```json"+`
{"key": "2"}
`+"```"+`

The same is true for the CSV format.

### CSV

For the CSV format when the field `+"`csv.header`"+` is specified a header row will be inserted as the first line of each message batch. If this field is not provided then the first message of each message batch must include a header line.`)).
		Field(service.NewStringField("project").Description("The project ID of the dataset to insert data to. If not set, it will be inferred from the credentials or read from the GOOGLE_CLOUD_PROJECT environment variable.").Default("")).
		Field(service.NewStringField("dataset").Description("The BigQuery Dataset ID.")).
		Field(service.NewStringField("table").Description("The table to insert messages to.")).
		Field(service.NewStringEnumField("format", string(bigquery.JSON), string(bigquery.CSV)).
			Description("The format of each incoming message.").
			Default(string(bigquery.JSON))).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of message batches to have in flight at a given time. Increase this to improve throughput.").
			Default(64)). // TODO: Tune this default
		Field(service.NewStringEnumField("write_disposition",
			string(bigquery.WriteAppend), string(bigquery.WriteEmpty), string(bigquery.WriteTruncate)).
			Description("Specifies how existing data in a destination table is treated.").
			Advanced().
			Default(string(bigquery.WriteAppend))).
		Field(service.NewStringEnumField("create_disposition", string(bigquery.CreateIfNeeded), string(bigquery.CreateNever)).
			Description("Specifies the circumstances under which destination table will be created. If CREATE_IF_NEEDED is used the GCP BigQuery will create the table if it does not already exist and tables are created atomically on successful completion of a job. The CREATE_NEVER option ensures the table must already exist and will not be automatically created.").
			Advanced().
			Default(string(bigquery.CreateIfNeeded))).
		Field(service.NewBoolField("ignore_unknown_values").
			Description("Causes values not matching the schema to be tolerated. Unknown values are ignored. For CSV this ignores extra values at the end of a line. For JSON this ignores named values that do not match any column name. If this field is set to false (the default value), records containing unknown values are treated as bad records. The max_bad_records field can be used to customize how bad records are handled.").
			Advanced().
			Default(false)).
		Field(service.NewIntField("max_bad_records").
			Description("The maximum number of bad records that will be ignored when reading data.").
			Advanced().
			Default(0)).
		Field(service.NewBoolField("auto_detect").
			Description("Indicates if we should automatically infer the options and schema for CSV and JSON sources. If the table doesn't exist and this field is set to `false` the output may not be able to insert data and will throw insertion error. Be careful using this field since it delegates to the GCP BigQuery service the schema detection and values like `\"no\"` may be treated as booleans for the CSV format.").
			Advanced().
			Default(false)).
		Field(service.NewStringMapField("job_labels").Description("A list of labels to add to the load job.").Default(map[string]any{})).
		Field(service.NewObjectField("csv",
			service.NewStringListField("header").
				Description("A list of values to use as header for each batch of messages. If not specified the first line of each message will be used as header.").
				Default([]any{}),
			service.NewStringField("field_delimiter").
				Description("The separator for fields in a CSV file, used when reading or exporting data.").
				Default(","),
			service.NewBoolField("allow_jagged_rows").
				Description("Causes missing trailing optional columns to be tolerated when reading CSV data. Missing values are treated as nulls.").
				Advanced().
				Default(false),
			service.NewBoolField("allow_quoted_newlines").
				Description("Sets whether quoted data sections containing newlines are allowed when reading CSV data.").
				Advanced().
				Default(false),
			service.NewStringEnumField("encoding", string(bigquery.UTF_8), string(bigquery.ISO_8859_1)).
				Description("Encoding is the character encoding of data to be read.").
				Advanced().
				Default(string(bigquery.UTF_8)),
			service.NewIntField("skip_leading_rows").
				Description("The number of rows at the top of a CSV file that BigQuery will skip when reading data. The default value is 1 since Benthos will add the specified header in the first line of each batch sent to BigQuery.").
				Advanced().
				Default(1),
		).Description("Specify how CSV data should be interpretted.")).
		Field(service.NewBatchPolicyField("batching"))
}

func init() {
	err := service.RegisterBatchOutput(
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
	if err != nil {
		panic(err)
	}
}

type gcpBigQueryOutput struct {
	conf      gcpBigQueryOutputConfig
	clientURL gcpBQClientURL

	client  *bigquery.Client
	connMut sync.RWMutex

	fieldDelimiterBytes []byte
	csvHeaderBytes      []byte
	newLineBytes        []byte

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
	if client, err = g.clientURL.NewClient(context.Background(), g.conf.ProjectID); err != nil {
		err = fmt.Errorf("error creating big query client: %w", err)
		return
	}
	defer func() {
		if err != nil {
			client.Close()
		}
	}()

	dataset := client.DatasetInProject(client.Project(), g.conf.DatasetID)
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
	table := g.client.DatasetInProject(g.client.Project(), g.conf.DatasetID).Table(g.conf.TableID)

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

func (g *gcpBigQueryOutput) Close(ctx context.Context) error {
	g.connMut.Lock()
	if g.client != nil {
		g.client.Close()
		g.client = nil
	}
	g.connMut.Unlock()
	return nil
}
