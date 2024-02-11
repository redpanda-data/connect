package gcp

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"golang.org/x/text/encoding/charmap"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

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
	ProjectID string
	DatasetID string
	TableID   string
	Format    string

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

type gcpMWClientURL string

func (g gcpMWClientURL) NewClient(ctx context.Context, projectID string) (*managedwriter.Client, error) {
	if g == "" {
		return managedwriter.NewClient(ctx, projectID)
	}
	return managedwriter.NewClient(ctx,
		projectID,
		option.WithoutAuthentication(),
		option.WithEndpoint(string(g)),
		option.WithGRPCDialOption(grpc.WithInsecure()),
	)
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
	conf        gcpBigQueryOutputConfig
	clientURL   gcpBQClientURL
	mwClientURL gcpMWClientURL

	client   *bigquery.Client
	mwClient *managedwriter.Client
	connMut  sync.RWMutex

	managedStream     *managedwriter.ManagedStream
	messageDescriptor *protoreflect.MessageDescriptor

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
	if client, err = g.clientURL.NewClient(ctx, g.conf.ProjectID); err != nil {
		err = fmt.Errorf("error creating big query client: %w", err)
		return
	}
	defer func() {
		if err != nil {
			client.Close()
		}
	}()

	var mwClient *managedwriter.Client
	if mwClient, err = g.mwClientURL.NewClient(ctx, g.conf.ProjectID); err != nil {
		err = fmt.Errorf("error creating BigQuery managed writer client: %w", err)
		return
	}
	defer func() {
		if err != nil {
			mwClient.Close()
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

	table := dataset.Table(g.conf.TableID)
	metadata, err := table.Metadata(ctx)
	if err != nil {
		if hasStatusCode(err, http.StatusNotFound) {
			err = fmt.Errorf("table does not exist: %v", g.conf.TableID)
		} else {
			err = fmt.Errorf("error checking table existence: %w", err)
		}
		return
	}

	dp := getDescriptor(metadata.Schema)
	md, err := getMessageDescriptor(dp)
	if err != nil {
		return err
	}
	ms, err := mwClient.NewManagedStream(ctx,
		managedwriter.WithDestinationTable(managedwriter.TableParentFromParts(g.conf.ProjectID, g.conf.DatasetID, g.conf.TableID)),
		managedwriter.WithType(managedwriter.DefaultStream),
		managedwriter.WithSchemaDescriptor(dp),
	)
	if err != nil {
		err = fmt.Errorf("error creating BigQuery managed stream: %w", err)
		return
	}
	defer func() {
		if err != nil {
			ms.Close()
		}
	}()

	g.client = client
	g.mwClient = mwClient
	g.managedStream = ms
	g.messageDescriptor = md
	g.log.Infof("Inserting messages as objects to GCP BigQuery: %v:%v:%v\n", client.Project(), g.conf.DatasetID, g.conf.TableID)
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

	var rows [][]byte
	for _, msg := range batch {
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return err
		}
		message := dynamicpb.NewMessage(*g.messageDescriptor)
		if err := protojson.Unmarshal(msgBytes, message); err != nil {
			return err
		}
		b, err := proto.Marshal(message)
		if err != nil {
			return err
		}
		rows = append(rows, b)
	}

	result, err := g.managedStream.AppendRows(ctx, rows)
	if err != nil {
		return err
	}

	o, err := result.GetResult(ctx)
	if err != nil {
		return err
	}
	if o != managedwriter.NoStreamOffset {
		return fmt.Errorf("offset mismatch, got %d want %d", o, managedwriter.NoStreamOffset)
	}

	return nil
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

func getDescriptor(schema bigquery.Schema) *descriptorpb.DescriptorProto {
	dp := &descriptorpb.DescriptorProto{
		Name:  proto.String("Row"),
		Field: []*descriptorpb.FieldDescriptorProto{},
	}
	for i, f := range schema {
		dp.Field = append(dp.Field, &descriptorpb.FieldDescriptorProto{
			Name:   proto.String(f.Name),
			Number: proto.Int32(int32(i + 1)),
			Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			Type:   protoFieldType(f.Type).Enum(),
		})
	}
	return dp
}

func protoFieldType(ft bigquery.FieldType) descriptorpb.FieldDescriptorProto_Type {
	switch ft {
	case bigquery.IntegerFieldType:
		return descriptorpb.FieldDescriptorProto_TYPE_INT64
	case bigquery.NumericFieldType:
		return descriptorpb.FieldDescriptorProto_TYPE_INT64
	case bigquery.FloatFieldType:
		return descriptorpb.FieldDescriptorProto_TYPE_DOUBLE
	case bigquery.BooleanFieldType:
		return descriptorpb.FieldDescriptorProto_TYPE_BOOL
	case bigquery.BytesFieldType:
		return descriptorpb.FieldDescriptorProto_TYPE_BYTES
	case bigquery.RecordFieldType:
		return descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	case bigquery.JSONFieldType:
		return descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	default:
		return descriptorpb.FieldDescriptorProto_TYPE_STRING
	}
}

func getMessageDescriptor(dp *descriptorpb.DescriptorProto) (*protoreflect.MessageDescriptor, error) {
	fdp := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("dynamic.proto"),
		Syntax:      proto.String("proto3"),
		Package:     proto.String("dynamic"),
		MessageType: []*descriptorpb.DescriptorProto{dp},
	}

	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to create file descriptor: %v", err)
	}

	md := fd.Messages().ByName("Row")
	if md == nil {
		return nil, fmt.Errorf("MessageDescriptor not found")
	}
	return &md, nil
}
