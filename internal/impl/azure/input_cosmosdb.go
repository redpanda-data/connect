package azure

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/benthosdev/benthos/v4/internal/impl/azure/cosmosdb"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/mitchellh/mapstructure"
)

func cosmosDBInputConfig() *service.ConfigSpec {
	return cosmosdb.NewConfigSpec(`
## Metadata

This input adds the following metadata fields to each message:

` + "```" + `
- activity_id
- request_charge
` + "```" + `
`).
		// Stable(). TODO
		Version("4.10.0").
		Summary("Executes an SQL query against Azure CosmosDB and creates a batch of messages from each page of items.").
		Field(service.NewStringField("query").Description("The query to execute").Example(`SELECT c.foo FROM TestContainer AS c WHERE c.bar = "baz" AND c.timestamp < @timestamp`)).
		Field(service.NewBloblangField("args_mapping").
			Description("A [Bloblang mapping](/docs/guides/bloblang/about) that, for each message, creates a list of arguments to use with the query.").Optional().Example(`
 args_mapping: |
  root = [
    { "Name": "@timestamp", "Value": timestamp_unix() - 3600 },
]
`)).
		Field(service.NewIntField("batch_count").
			Description(`The maximum number of messages that should be accumulated into each batch. '-1' represents dynamic page size.`).
			Default(-1).
			Advanced().LintRule(`root = if this < -1 || this == 0 || this > ` + fmt.Sprint(math.MaxInt32) + ` { [ "batch_count must be must be > 0 and smaller than ` + fmt.Sprint(math.MaxInt32) + ` or -1." ] }`))
}

func init() {
	err := service.RegisterBatchInput("azure_cosmosdb", cosmosDBInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			service.BatchInput,
			error,
		) {
			i, err := newCosmosDBReaderFromConfig(conf, mgr.Logger())
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksBatched(i), nil
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type cosmosDBReader struct {
	logger *service.Logger

	endpoint         string
	keyCredential    azcosmos.KeyCredential
	databaseID       string
	containerID      string
	partitionKey     string
	partitionKeyType cosmosdb.PartitionKeyType
	query            string
	args             []azcosmos.QueryParameter
	batchCount       int

	pager   *runtime.Pager[azcosmos.QueryItemsResponse]
	connMut sync.Mutex
}

func newCosmosDBReaderFromConfig(conf *service.ParsedConfig, logger *service.Logger) (service.BatchInput, error) {
	c := cosmosDBReader{
		logger: logger,
	}

	var err error
	if c.endpoint, err = conf.FieldString("endpoint"); err != nil {
		return nil, fmt.Errorf("failed to parse endpoint: %s", err)
	}

	var key string
	if key, err = conf.FieldString("account_key"); err != nil {
		return nil, fmt.Errorf("failed to parse account_key: %s", err)
	}

	if c.keyCredential, err = azcosmos.NewKeyCredential(key); err != nil {
		return nil, fmt.Errorf("failed to deserialise account_key: %s", err)
	}

	if c.databaseID, err = conf.FieldString("database_id"); err != nil {
		return nil, fmt.Errorf("failed to parse database_id: %s", err)
	}

	if c.containerID, err = conf.FieldString("container_id"); err != nil {
		return nil, fmt.Errorf("failed to parse container_id: %s", err)
	}

	partitionKey, err := conf.FieldInterpolatedString("partition_key")
	if err != nil {
		return nil, fmt.Errorf("failed to parse partition_key: %s", err)
	}
	c.partitionKey = partitionKey.String(&service.Message{})

	partitionKeyType, err := conf.FieldString("partition_key_type")
	if err != nil {
		return nil, fmt.Errorf("failed to parse partition_key_type: %s", err)
	}
	switch cosmosdb.PartitionKeyType(partitionKeyType) {
	case cosmosdb.PartitionKeyString, cosmosdb.PartitionKeyBool, cosmosdb.PartitionKeyNumber:
		c.partitionKeyType = cosmosdb.PartitionKeyType(partitionKeyType)
	default:
		return nil, fmt.Errorf("unrecognised partition_key_type: %s", partitionKeyType)
	}

	if c.query, err = conf.FieldString("query"); err != nil {
		return nil, fmt.Errorf("failed to parse query: %s", err)
	}

	if conf.Contains("args_mapping") {
		argsMapping, err := conf.FieldBloblang("args_mapping")
		if err != nil {
			return nil, fmt.Errorf("failed to parse args_mapping: %s", err)
		}

		args, err := argsMapping.Query(nil)
		if err != nil {
			return nil, fmt.Errorf("error evaluating args_mapping: %s", err)
		}

		if err := mapstructure.Decode(args, &c.args); err != nil {
			return nil, fmt.Errorf("error converting args_mapping to CosmosDB parameters: %s", err)
		}
	}

	if c.batchCount, err = conf.FieldInt("batch_count"); err != nil {
		return nil, err
	}
	if c.batchCount < -1 || c.batchCount == 0 || c.batchCount > math.MaxInt32 {
		return nil, fmt.Errorf("batch_count must be > 0 and smaller than %d or -1, got %d", math.MaxInt32, c.batchCount)
	}

	return &c, nil
}

//------------------------------------------------------------------------------

func (c *cosmosDBReader) Connect(ctx context.Context) error {
	if c.pager != nil {
		return nil
	}

	client, err := azcosmos.NewClientWithKey(c.endpoint, c.keyCredential, nil)
	if err != nil {
		return fmt.Errorf("failed to create client: %s", err)
	}

	var container *azcosmos.ContainerClient
	if container, err = client.NewContainer(c.databaseID, c.containerID); err != nil {
		return fmt.Errorf("failed to create container client: %s", err)
	}

	var partitionKey azcosmos.PartitionKey
	switch c.partitionKeyType {
	case cosmosdb.PartitionKeyString:
		partitionKey = azcosmos.NewPartitionKeyString(c.partitionKey)
	case cosmosdb.PartitionKeyBool:
		pk, err := strconv.ParseBool(c.partitionKey)
		if err != nil {
			return fmt.Errorf("failed to parse boolean partition key: %s", err)
		}
		partitionKey = azcosmos.NewPartitionKeyBool(pk)
	case cosmosdb.PartitionKeyNumber:
		pk, err := strconv.ParseFloat(c.partitionKey, 64)
		if err != nil {
			return fmt.Errorf("failed to parse numerical partition key: %s", err)
		}
		partitionKey = azcosmos.NewPartitionKeyNumber(pk)
	}

	c.pager = container.NewQueryItemsPager(c.query, partitionKey, &azcosmos.QueryOptions{
		PageSizeHint:    int32(c.batchCount),
		QueryParameters: c.args,
	})

	return nil
}

func (c *cosmosDBReader) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	c.connMut.Lock()
	defer c.connMut.Unlock()
	if c.pager == nil {
		return nil, nil, service.ErrNotConnected
	}

	if !c.pager.More() {
		return nil, nil, service.ErrEndOfInput
	}

	queryResponse, err := c.pager.NextPage(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get next page of query response: %s", err)
	}

	resBatch := make(service.MessageBatch, 0, len(queryResponse.Items))
	for _, item := range queryResponse.Items {
		m := service.NewMessage(item)
		m.MetaSetMut("activity_id", queryResponse.ActivityID)
		m.MetaSetMut("request_charge", queryResponse.RequestCharge)

		resBatch = append(resBatch, m)
	}

	return resBatch, func(ctx context.Context, err error) error { return nil }, nil
}

func (c *cosmosDBReader) Close(ctx context.Context) error {
	c.connMut.Lock()
	defer c.connMut.Unlock()

	return nil
}
