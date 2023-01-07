package azure

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/azure/shared"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		r, err := newAzureTableStorage(conf.AzureTableStorage, nm.Logger(), nm.Metrics())
		if err != nil {
			return nil, err
		}
		return input.NewAsyncReader("azure_table_storage", input.NewAsyncPreserver(r), nm)
	}), docs.ComponentSpec{
		Name:    "azure_table_storage",
		Status:  docs.StatusBeta,
		Version: "4.10.0",
		Summary: `
Queries an Azure Storage Account Table, optionally with multiple filters.`,
		Description: `
Queries an Azure Storage Account Table, optionally with multiple filters.
## Metadata
This input adds the following metadata fields to each message:
` + "```" + `
- table_storage_name
- row_num
` + "```" + `
You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString(
				"storage_account",
				"The storage account to upload messages to. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldString(
				"storage_access_key",
				"The storage account access key. This field is ignored if `storage_connection_string` is set.",
			),
			docs.FieldString(
				"storage_connection_string",
				"A storage account connection string. This field is required if `storage_account` and `storage_access_key` are not set.",
			),
			docs.FieldString("table_name", "The table to read messages from.",
				`Foo`,
			),
			docs.FieldString("filter", "OData filter expression. Is not set all rows are returned. Valid operators are `eq, ne, gt, lt, ge and le`",
				`PartitionKey eq 'foo' and RowKey gt '1000'`,
			).Advanced(),
			docs.FieldString("select", "Select expression using OData notation. Limits the columns on each record to just those requested.",
				`PartitionKey,RowKey,Foo,Bar,Timestamp`,
			).Advanced(),
			docs.FieldInt("page_size", "Maximum number of records to return on each page.",
				`1000`,
			).Advanced(),
		).ChildDefaultAndTypesFromStruct(input.NewAzureTableStorageConfig()),
		Categories: []string{
			"Services",
			"Azure",
		},
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// AzureTableStorage is a benthos reader.Type implementation that reads rows
// from an Azure Storage Table.
type azureTableStorage struct {
	conf   input.AzureTableStorageConfig
	client *aztables.Client
	pager  *runtime.Pager[aztables.ListEntitiesResponse]
	row    int
	log    log.Modular
	stats  metrics.Type
}

// newAzureTableStorage creates a new Azure Table Storage input type.
func newAzureTableStorage(conf input.AzureTableStorageConfig, log log.Modular, stats metrics.Type) (*azureTableStorage, error) {
	client, err := shared.GetServiceClient(conf.StorageAccount, conf.StorageAccessKey, conf.StorageConnectionString)
	if err != nil {
		return nil, fmt.Errorf("invalid azure storage account credentials: %v", err)
	}
	a := &azureTableStorage{
		conf:   conf,
		log:    log,
		stats:  stats,
		client: client.NewClient(conf.TableName),
	}
	return a, nil
}

// Connect attempts to establish a connection to the target Azure Storage Table.
func (a *azureTableStorage) Connect(ctx context.Context) error {
	options := &aztables.ListEntitiesOptions{
		Filter: stringOrNil(a.conf.Filter),
		Select: stringOrNil(a.conf.Select),
		Top:    int32OrNil(a.conf.PageSize),
	}
	a.pager = a.client.NewListEntitiesPager(options)
	return nil
}

func stringOrNil(val string) *string {
	if len(val) > 0 {
		return &val
	}
	return nil
}

func int32OrNil(val int32) *int32 {
	if val > 0 {
		return &val
	}
	return nil
}

// ReadBatch attempts to read a new page from the target Azure Storage Table.
func (a *azureTableStorage) ReadBatch(ctx context.Context) (msg message.Batch, ackFn input.AsyncAckFn, err error) {
	if a.pager.More() {
		resp, nerr := a.pager.NextPage(ctx)
		if nerr != nil {
			a.log.Warnf("error fetching next page", nerr)
			return nil, nil, component.ErrTypeClosed
		}
		for _, entity := range resp.Entities {
			p := message.NewPart(entity)
			msg = append(msg, p)
		}
		_ = msg.Iter(func(_ int, part *message.Part) error {
			a.row++
			part.MetaSetMut("table_storage_name", a.conf.TableName)
			part.MetaSetMut("row_num", a.row)
			return nil
		})
		return msg, func(rctx context.Context, res error) error {
			return nil
		}, err
	}
	return nil, nil, component.ErrTypeClosed
}

// Close is called when the pipeline ends
func (a *azureTableStorage) Close(ctx context.Context) (err error) {
	return
}
