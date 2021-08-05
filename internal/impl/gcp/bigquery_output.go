package gcp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	ibatch "github.com/Jeffail/benthos/v3/internal/batch"
	"github.com/Jeffail/benthos/v3/internal/bundle"
	ioutput "github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
	"golang.org/x/text/encoding/charmap"
	"google.golang.org/api/googleapi"
)

func init() {
	bundle.AllOutputs.Add(bundle.OutputConstructorFromSimple(func(c output.Config, nm bundle.NewManagement) (output.Type, error) {
		g, err := newGCPBigQueryOutput(c.GCPBigQuery, nm.Logger(), nm.Metrics())
		if err != nil {
			return nil, err
		}
		w, err := output.NewAsyncWriter(output.TypeGCPCloudStorage, c.GCPBigQuery.MaxInFlight, g, nm.Logger(), nm.Metrics())
		if err != nil {
			return nil, err
		}
		return output.NewBatcherFromConfig(c.GCPBigQuery.Batching, w, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:    output.TypeGCPBigQuery,
		Type:    docs.TypeOutput,
		Status:  docs.StatusExperimental,
		Version: "3.52.0",
		Categories: []string{
			string(input.CategoryServices),
			string(input.CategoryGCP),
		},
		Summary: `
Sends message parts as new rows to a Google Cloud BigQuery table. Currently json is the only supported format.
Each object is stored in the dataset and table specified with the ` + "`dataset`" + ` and ` + "`table`" + ` fields.`,
		Description: ioutput.Description(true, true, `
### Credentials

By default Benthos will use a shared credentials file when connecting to GCP
services. You can find out more [in this document](/docs/guides/gcp).

### Dataset and Table

Currently this plugin cannot create a new Dataset nor a new Table, where both need
to exist for this output to be used.

### Format

Currently this plugins supports only CSV and JSON formats.

Each message may contains multiple single-line elements separated by a \n.

For example a single message containing
`+"```json"+`
{"key" : "1"}
{"key" : "2"}
`+"```"+`

and multiples messages containing
`+"```json"+`
{"key" : "1"}
`+"```"+`

`+"```json"+`
{"key" : "2"}
`+"```"+`

Will both result in two rows inside the BigQuery. The same is valid for the CSV format.

Example of invalid message:
`+"```json"+`
{
	"key" : "2"
}
`+"```"+`

#### CSV

For the CSV format the field `+"`csv.header`"+` makes benthos add the header as the first line in each batch using the `+"`csv.field_delimiter`"+` as delimiter to send data to Google Cloud BigQuery.
If this field is not provided, the first message in the output batch will be used as header.
`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldCommon("project", "The project ID of the dataset to insert data to."),
			docs.FieldCommon("dataset", "BigQuery Dataset Id. Do not include project id in this field."),
			docs.FieldCommon("table", "The table to insert messages to."),
			docs.FieldCommon("format", "The format of each incoming message.").
				HasDefault(string(bigquery.JSON)).
				HasOptions(string(bigquery.JSON), string(bigquery.CSV)),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldAdvanced("write_disposition", "Specifies how existing data in a destination table is treated.").
				HasDefault(string(bigquery.WriteAppend)).
				HasOptions(string(bigquery.WriteAppend), string(bigquery.WriteEmpty), string(bigquery.WriteTruncate)),
			docs.FieldAdvanced("create_disposition", "Specifies the circumstances under which destination table will be created. If CREATE_IF_NEEDED is used the GCP BigQuery will create the table if it does not already exist and tables are created atomically on successful completion of a job. The CREATE_NEVER option ensures the table must already exist and will not be automatically created.").
				HasDefault(string(bigquery.CreateIfNeeded)).
				HasOptions(string(bigquery.CreateIfNeeded), string(bigquery.CreateNever)),
			docs.FieldAdvanced("ignore_unknown_values", "Causes values not matching the schema to be tolerated. Unknown values are ignored. For CSV this ignores extra values at the end of a line. For JSON this ignores named values that do not match any column name. If this field is set to false (the default value), records containing unknown values are treated as bad records. The max_bad_records field can be used to customize how bad records are handled.").
				HasDefault(false).
				HasOptions("true", "false"),
			docs.FieldAdvanced("max_bad_records", "The maximum number of bad records that will be ignored when reading data.").
				HasDefault(0),
			docs.FieldAdvanced("auto_detect", "Indicates if we should automatically infer the options and schema for CSV and JSON sources. If the table doesn't exists and this field is set to false the output may not be able to insert data and will throw insertion error. Be careful using this field since it delegates to the GCP BigQuery service the schema detection and values like \"no\" may be treated as booleans for the CSV format. You should probably create the table manually and leave this unset.").
				HasDefault(false).
				HasOptions("true", "false"),
			docs.FieldCommon("csv", "Configurations used in the CSV format.").Optional().WithChildren(
				docs.FieldCommon("header", "A list of values to use as header for each batch of messages. If not specified the first line of each message will be used as header. You should not enable batching if this field is not specified.").
					Array().HasType(docs.FieldTypeString).Optional(),
				docs.FieldCommon("field_delimiter", "The separator for fields in a CSV file, used when reading or exporting data.").
					HasDefault(","),
				docs.FieldAdvanced("allow_jagged_rows", "Causes missing trailing optional columns to be tolerated when reading CSV data. Missing values are treated as nulls.").
					HasDefault(false).
					HasOptions("true", "false"),
				docs.FieldAdvanced("allow_quoted_newlines", "Sets whether quoted data sections containing newlines are allowed when reading CSV data.").
					HasDefault(false).
					HasOptions("true", "false"),
				docs.FieldAdvanced("encoding", "Encoding is the character encoding of data to be read.").
					HasDefault(string(bigquery.UTF_8)).
					HasOptions(string(bigquery.UTF_8), string(bigquery.ISO_8859_1)),
				docs.FieldAdvanced("skip_leading_rows", "The number of rows at the top of a CSV file that BigQuery will skip when reading data. The default value is 1 since Benthos will add the specified header in the first line of each batch sent to BigQuery.").
					HasDefault(1),
			),
			batch.FieldSpec(),
		).ChildDefaultAndTypesFromStruct(output.NewGCPBigQueryConfig()),
	})
}

// gcpBigQueryOutput is a benthos writer.Type implementation that writes
// messages to a GCP BigQuery dataset.
type gcpBigQueryOutput struct {
	conf output.GCPBigQueryConfig

	client  *bigquery.Client
	table   *bigquery.Table
	connMut sync.RWMutex

	fieldDelimiterBytes []byte
	csvHeaderBytes      []byte
	newLineBytes        []byte

	log   log.Modular
	stats metrics.Type
}

// newGCPBigQueryOutput creates a new GCP BigQuery dataset writer.Type.
func newGCPBigQueryOutput(
	conf output.GCPBigQueryConfig,
	log log.Modular,
	stats metrics.Type,
) (*gcpBigQueryOutput, error) {
	g := &gcpBigQueryOutput{
		conf:  conf,
		log:   log,
		stats: stats,
	}

	g.newLineBytes = []byte("\n")

	if conf.Format != string(bigquery.CSV) {
		return g, nil
	}

	g.fieldDelimiterBytes = []byte(conf.CSVOptions.FieldDelimiter)

	if conf.CSVOptions.Header != nil && len(conf.CSVOptions.Header) != 0 {
		header := fmt.Sprint("\"", strings.Join(conf.CSVOptions.Header, fmt.Sprint("\"", conf.CSVOptions.FieldDelimiter, "\"")), "\"")

		g.csvHeaderBytes = []byte(header)
	}

	if conf.CSVOptions.Encoding == string(bigquery.UTF_8) {
		return g, nil
	}

	var err error

	g.fieldDelimiterBytes, err = charmap.ISO8859_1.NewEncoder().Bytes(g.fieldDelimiterBytes)

	if err != nil {
		return nil, fmt.Errorf("error parsing csv.field_delimiter field: %w", err)
	}

	g.newLineBytes, err = charmap.ISO8859_1.NewEncoder().Bytes([]byte("\n"))

	if err != nil {
		return nil, fmt.Errorf("error creating newline bytes: %w", err)
	}

	if g.csvHeaderBytes == nil {
		return g, nil
	}

	g.csvHeaderBytes, err = charmap.ISO8859_1.NewEncoder().Bytes(g.fieldDelimiterBytes)

	if err != nil {
		return nil, fmt.Errorf("error parsing csv.header field: %w", err)
	}

	return g, nil
}

// ConnectWithContext attempts to establish a connection to the target Google
// Cloud Storage bucket.
func (g *gcpBigQueryOutput) ConnectWithContext(ctx context.Context) error {
	g.connMut.Lock()
	defer g.connMut.Unlock()

	var err error
	g.client, err = NewBigQueryClient(ctx, g.conf.ProjectID)
	if err != nil {
		return fmt.Errorf("error creating big query client: %w", err)
	}

	dataset := g.client.DatasetInProject(g.conf.ProjectID, g.conf.DatasetID)

	_, err = dataset.Metadata(ctx)
	if err != nil {
		if hasStatusCode(err, http.StatusNotFound) {
			return fmt.Errorf("dataset %v does not exists", g.conf.DatasetID)
		}

		return fmt.Errorf("error checking dataset existance: %w", err)
	}

	if g.conf.CreateDisposition == string(bigquery.CreateNever) {
		g.table = dataset.Table(g.conf.TableID)

		_, err = g.table.Metadata(ctx)
		if err != nil {
			if hasStatusCode(err, http.StatusNotFound) {
				return fmt.Errorf("table %v does not exists", g.conf.TableID)
			}

			return fmt.Errorf("error checking table existance: %w", err)
		}
	}

	g.log.Infof("Inserting message parts as objects to GCP BigQuery: %v:%v:%v\n", g.conf.ProjectID, g.conf.DatasetID, g.conf.TableID)

	return nil
}

func hasStatusCode(err error, code int) bool {
	if e, ok := err.(*googleapi.Error); ok && e.Code == code {
		return true
	}
	return false
}

// WriteWithContext attempts to write message contents to a target GCP BigQuery as files.
func (g *gcpBigQueryOutput) WriteWithContext(ctx context.Context, msg types.Message) error {
	g.connMut.RLock()
	client := g.client
	g.connMut.RUnlock()

	if client == nil {
		return types.ErrNotConnected
	}

	var data []byte

	if g.csvHeaderBytes != nil {
		data = g.csvHeaderBytes
	}

	err := writer.IterateBatchedSend(msg, func(i int, p types.Part) error {
		if data == nil {
			data = p.Get()

			return nil
		}

		data = bytes.Join([][]byte{data, p.Get()}, g.newLineBytes)

		return nil
	})

	var batchErr *ibatch.Error
	if err != nil {
		if !errors.As(err, &batchErr) {
			return err
		}
	}

	job, err := g.createTableLoader(&data).Run(ctx)

	if err != nil {
		return err
	}

	status, err := job.Wait(ctx)
	if err == nil {
		err = status.Err()
	}

	if err != nil {
		return fmt.Errorf("error inserting data in bigquery: %w", err)
	}

	return nil
}

func (g *gcpBigQueryOutput) createTableLoader(data *[]byte) *bigquery.Loader {
	table := g.client.DatasetInProject(g.conf.ProjectID, g.conf.DatasetID).Table(g.conf.TableID)

	source := bigquery.NewReaderSource(bytes.NewReader(*data))
	source.SourceFormat = bigquery.DataFormat(g.conf.Format)
	source.AutoDetect = g.conf.AutoDetect
	source.IgnoreUnknownValues = g.conf.IgnoreUnknownValues
	source.MaxBadRecords = g.conf.MaxBadRecords

	source.FieldDelimiter = g.conf.CSVOptions.FieldDelimiter
	source.AllowJaggedRows = g.conf.CSVOptions.AllowJaggedRows
	source.AllowQuotedNewlines = g.conf.CSVOptions.AllowQuotedNewlines
	source.Encoding = bigquery.Encoding(g.conf.CSVOptions.Encoding)
	source.SkipLeadingRows = g.conf.CSVOptions.SkipLeadingRows

	loader := table.LoaderFrom(source)

	loader.CreateDisposition = bigquery.TableCreateDisposition(g.conf.CreateDisposition)
	loader.WriteDisposition = bigquery.TableWriteDisposition(g.conf.WriteDisposition)

	return loader
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (g *gcpBigQueryOutput) CloseAsync() {
	go func() {
		g.connMut.Lock()
		if g.client != nil {
			g.client.Close()
			g.client = nil
		}
		g.connMut.Unlock()
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (g *gcpBigQueryOutput) WaitForClose(time.Duration) error {
	return nil
}
