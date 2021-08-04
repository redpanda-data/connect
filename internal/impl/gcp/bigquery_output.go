package gcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
	"github.com/google/uuid"
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
		Version: "3.43.0",
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

Currently JSON is the only supported format by this output.`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldCommon("project", "The project ID of the dataset to insert data to."),
			docs.FieldCommon("dataset", "BigQuery Dataset Id. Do not include project id in this field."),
			docs.FieldCommon("table", "The table to insert messages to."),
			docs.FieldCommon("format", "The format of each incoming message.").HasOptions("json").HasDefault("json"),
			docs.FieldCommon("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
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

	log   log.Modular
	stats metrics.Type
}

// newGCPBigQueryOutput creates a new GCP BigQuery dataset writer.Type.
func newGCPBigQueryOutput(
	conf output.GCPBigQueryConfig,
	log log.Modular,
	stats metrics.Type,
) (*gcpBigQueryOutput, error) {
	return &gcpBigQueryOutput{
		conf:  conf,
		log:   log,
		stats: stats,
	}, nil
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

	g.table = dataset.Table(g.conf.TableID)

	_, err = g.table.Metadata(ctx)
	if err != nil {
		if hasStatusCode(err, http.StatusNotFound) {
			return fmt.Errorf("table %v does not exists", g.conf.TableID)
		}

		return fmt.Errorf("error checking table existance: %w", err)
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

// genericRecord is the type used to store data without a well defined struct.
// It extends the bigquery.ValueSaver interface.
type genericRecord map[string]bigquery.Value

// Save needs to be implemented to be able to store data into BigQuery without providing a schema.
func (rec genericRecord) Save() (values map[string]bigquery.Value, insertID string, err error) {
	insertID = uuid.New().String()
	return rec, insertID, nil
}

var _ bigquery.ValueSaver = &genericRecord{}

// WriteWithContext attempts to write message contents to a target GCP BigQuery as files.
func (g *gcpBigQueryOutput) WriteWithContext(ctx context.Context, msg types.Message) error {
	g.connMut.RLock()
	client := g.client
	g.connMut.RUnlock()

	if client == nil {
		return types.ErrNotConnected
	}

	var data []*genericRecord
	err := writer.IterateBatchedSend(msg, func(i int, p types.Part) error {
		var messageData genericRecord

		err := json.Unmarshal(p.Get(), &messageData)

		if err != nil {
			return err
		}

		data = append(data, &messageData)

		return nil
	})

	var batchErr *ibatch.Error
	if err != nil {
		if !errors.As(err, &batchErr) {
			return err
		}
	}

	inserter := client.DatasetInProject(g.conf.ProjectID, g.conf.DatasetID).Table(g.conf.TableID).Inserter()

	return inserter.Put(ctx, data)
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
