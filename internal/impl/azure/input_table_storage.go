package azure

import (
	"context"
	"sync/atomic"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	// Table Storage Input Fields
	tsiFieldTableName = "table_name"
	tsiFieldFilter    = "filter"
	tsiFieldSelect    = "select"
	tsiFieldPageSize  = "page_size"
)

type tsiConfig struct {
	client    *aztables.Client
	TableName string
	Filter    string
	Select    string
	PageSize  int32
}

func tsiConfigFromParsed(pConf *service.ParsedConfig) (conf tsiConfig, err error) {
	var svcClient *aztables.ServiceClient
	if svcClient, err = tablesServiceClientFromParsed(pConf); err != nil {
		return
	}
	if conf.TableName, err = pConf.FieldString(tsiFieldTableName); err != nil {
		return
	}
	if conf.Filter, err = pConf.FieldString(tsiFieldFilter); err != nil {
		return
	}
	if conf.Select, err = pConf.FieldString(tsiFieldSelect); err != nil {
		return
	}
	var pageSize int
	if pageSize, err = pConf.FieldInt(tsiFieldPageSize); err != nil {
		return
	}
	conf.PageSize = int32(pageSize)
	conf.client = svcClient.NewClient(conf.TableName)
	return
}

func tsiSpec() *service.ConfigSpec {
	return azureComponentSpec(false).
		Beta().
		Version("4.10.0").
		Summary(`Queries an Azure Storage Account Table, optionally with multiple filters.`).
		Description(`
Queries an Azure Storage Account Table, optionally with multiple filters.
## Metadata
This input adds the following metadata fields to each message:
`+"```"+`
- table_storage_name
- row_num
`+"```"+`
You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).`).
		Fields(
			service.NewStringField(tsiFieldTableName).
				Description("The table to read messages from.").
				Example(`Foo`),
			service.NewStringField(tsiFieldFilter).
				Description("OData filter expression. Is not set all rows are returned. Valid operators are `eq, ne, gt, lt, ge and le`").Example(`PartitionKey eq 'foo' and RowKey gt '1000'`).
				Advanced().
				Default(""),
			service.NewStringField(tsiFieldSelect).
				Description("Select expression using OData notation. Limits the columns on each record to just those requested.").
				Example(`PartitionKey,RowKey,Foo,Bar,Timestamp`).
				Advanced().
				Default(""),
			service.NewIntField(tsiFieldPageSize).
				Description("Maximum number of records to return on each page.").
				Advanced().
				Default(1000),
		)
}

func init() {
	err := service.RegisterBatchInput("azure_table_storage", tsiSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			pConf, err := tsiConfigFromParsed(conf)
			if err != nil {
				return nil, err
			}
			return newAzureTableStorage(pConf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

// AzureTableStorage is a benthos reader.Type implementation that reads rows
// from an Azure Storage Table.
type azureTableStorage struct {
	conf  tsiConfig
	pager *runtime.Pager[aztables.ListEntitiesResponse]
	row   int64
	log   *service.Logger
}

// newAzureTableStorage creates a new Azure Table Storage input type.
func newAzureTableStorage(conf tsiConfig, mgr *service.Resources) (*azureTableStorage, error) {
	a := &azureTableStorage{
		conf: conf,
		log:  mgr.Logger(),
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
	a.pager = a.conf.client.NewListEntitiesPager(options)
	return nil
}

func stringOrNil(val string) *string {
	if val != "" {
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
func (a *azureTableStorage) ReadBatch(ctx context.Context) (batch service.MessageBatch, ackFn service.AckFunc, err error) {
	for a.pager.More() {
		resp, err := a.pager.NextPage(ctx)
		if err != nil {
			if ctx.Err() == nil {
				a.log.Warnf("error fetching next page: %v", err)
			}
			return nil, nil, component.ErrTypeClosed
		}
		if len(resp.Entities) == 0 {
			continue
		}

		batch = make(service.MessageBatch, 0, len(resp.Entities))
		for _, entity := range resp.Entities {
			m := service.NewMessage(entity)
			m.MetaSetMut("table_storage_name", a.conf.TableName)
			m.MetaSetMut("row_num", atomic.AddInt64(&a.row, 1))
			batch = append(batch, m)
		}
		return batch, func(_ context.Context, res error) error {
			return nil
		}, err
	}
	return nil, nil, component.ErrTypeClosed
}

// Close is called when the pipeline ends
func (a *azureTableStorage) Close(ctx context.Context) (err error) {
	return
}
