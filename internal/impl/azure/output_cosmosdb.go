package azure

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"

	"github.com/benthosdev/benthos/v4/internal/impl/azure/cosmosdb"
	"github.com/benthosdev/benthos/v4/public/service"
)

func cosmosDBOutputConfig() *service.ConfigSpec {
	return cosmosdb.NewConfigSpec("").
		// Stable(). TODO
		Version("4.10.0").
		Summary("Publishes messages as JSON documents into Azure CosmosDB.").
		Description("When creating, replacing or upserting documents, each must have the `id` property (case-sensitive) set. It is the unique name that identifies the document, that is, no two documents share the same `id` within a logical partition. Partition and `id` uniquely identifies an item in the database. The `id` field must not exceed 255 characters. More details can be found [here](https://learn.microsoft.com/en-us/rest/api/cosmos-db/documents).").
		Field(service.NewStringAnnotatedEnumField("operation", map[string]string{
			string(cosmosdb.OperationCreate):  "Create operation.",
			string(cosmosdb.OperationDelete):  "Delete operation.",
			string(cosmosdb.OperationReplace): "Replace operation.",
			string(cosmosdb.OperationUpsert):  "Upsert operation.",
		}).Description("Operation.").Default(string(cosmosdb.OperationCreate))).
		Field(service.NewInterpolatedStringField("item_id").Description("ID of item to replace or delete. Only used by the Replace and Delete operations").Example("115a33c7-e7b3-44af-aea5-30a3fe1fb69b").Example(`${! json("foobar") }`).Optional()).
		Field(service.NewBatchPolicyField("batching")).
		Field(service.NewIntField("max_in_flight").Description("The maximum number of parallel message batches to have in flight at any given time.").Default(1)).
		LintRule(`root = if ((this.operation == "Replace" || this.operation == "Delete") && !this.exists("item_id")) || (this.exists("item_id") && (this.operation != "Replace" && this.operation != "Delete")) { [ "item_id must only be set for Replace and Delete operations." ] }`)
}

func init() {
	err := service.RegisterBatchOutput("azure_cosmosdb", cosmosDBOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			output, err = newCosmosDBWriterFromConfig(conf, mgr.Logger())
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type cosmosDBWriter struct {
	logger *service.Logger

	endpoint         string
	keyCredential    azcosmos.KeyCredential
	databaseID       string
	containerID      string
	partitionKey     *service.InterpolatedString
	partitionKeyType cosmosdb.PartitionKeyType
	itemID           *service.InterpolatedString
	Operation        cosmosdb.OperationType

	container *azcosmos.ContainerClient
	connMut   sync.Mutex
}

func newCosmosDBWriterFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*cosmosDBWriter, error) {
	c := cosmosDBWriter{
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

	if c.partitionKey, err = conf.FieldInterpolatedString("partition_key"); err != nil {
		return nil, fmt.Errorf("failed to parse partition_key: %s", err)
	}

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

	operation, err := conf.FieldString("operation")
	if err != nil {
		return nil, fmt.Errorf("failed to parse operation: %s", err)
	}
	switch cosmosdb.OperationType(operation) {
	case cosmosdb.OperationCreate, cosmosdb.OperationDelete, cosmosdb.OperationReplace, cosmosdb.OperationUpsert:
		c.Operation = cosmosdb.OperationType(operation)
	default:
		return nil, fmt.Errorf("unrecognised operation: %s", partitionKeyType)
	}

	if conf.Contains("item_id") {
		if c.itemID, err = conf.FieldInterpolatedString("item_id"); err != nil {
			return nil, fmt.Errorf("failed to parse item_id: %s", err)
		}
	}

	return &c, nil
}

//------------------------------------------------------------------------------

func (c *cosmosDBWriter) Connect(ctx context.Context) error {
	if c.container != nil {
		return nil
	}

	client, err := azcosmos.NewClientWithKey(c.endpoint, c.keyCredential, nil)
	if err != nil {
		return fmt.Errorf("failed to create client: %s", err)
	}

	if c.container, err = client.NewContainer(c.databaseID, c.containerID); err != nil {
		return fmt.Errorf("failed to create container client: %s", err)
	}

	return nil
}

func (c *cosmosDBWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	c.connMut.Lock()
	defer c.connMut.Unlock()
	if c.container == nil {
		return service.ErrNotConnected
	}

	partitionKeyStr := c.partitionKey.String(batch[0])

	var partitionKey azcosmos.PartitionKey
	switch c.partitionKeyType {
	case cosmosdb.PartitionKeyString:
		partitionKey = azcosmos.NewPartitionKeyString(partitionKeyStr)
	case cosmosdb.PartitionKeyBool:
		pk, err := strconv.ParseBool(partitionKeyStr)
		if err != nil {
			return fmt.Errorf("failed to parse boolean partition key: %s", err)
		}
		partitionKey = azcosmos.NewPartitionKeyBool(pk)
	case cosmosdb.PartitionKeyNumber:
		pk, err := strconv.ParseFloat(partitionKeyStr, 64)
		if err != nil {
			return fmt.Errorf("failed to parse numerical partition key: %s", err)
		}
		partitionKey = azcosmos.NewPartitionKeyNumber(pk)
	}

	var itemID string
	if c.itemID != nil {
		itemID = c.itemID.String(batch[0])
	}

	// Gracefully handle batches larger than 100 items by splitting them into smaller batches
	// Details here: https://learn.microsoft.com/en-us/azure/cosmos-db/concepts-limits#per-request-limits
	// and here: https://github.com/Azure/azure-cosmos-dotnet-v3/issues/1057
	const maxTransactionalBatchSize = 100
	if len(batch) > maxTransactionalBatchSize {
		c.logger.Warnf("Splitting current batch of %d items into %d item chunks due to CosmosDB transactional batch limits", len(batch), maxTransactionalBatchSize)
	}
	for lowerBound := 0; lowerBound < len(batch); lowerBound += maxTransactionalBatchSize {
		tb := c.container.NewTransactionalBatch(partitionKey)

		upperBound := lowerBound + maxTransactionalBatchSize
		if upperBound > len(batch) {
			upperBound = len(batch)
		}
		for _, msg := range batch[lowerBound:upperBound] {
			b, err := msg.AsBytes()
			if err != nil {
				return fmt.Errorf("failed to get message bytes: %s", err)
			}

			switch c.Operation {
			case cosmosdb.OperationCreate:
				tb.CreateItem(b, nil)
			case cosmosdb.OperationDelete:
				tb.DeleteItem(itemID, nil)
			case cosmosdb.OperationReplace:
				tb.ReplaceItem(itemID, b, nil)
			case cosmosdb.OperationUpsert:
				tb.UpsertItem(b, nil)
			}
		}

		resp, err := c.container.ExecuteTransactionalBatch(ctx, tb, nil)
		if err != nil {
			return fmt.Errorf("failed to execute transactional batch: %s", err)
		}
		if resp.Success {
			c.logger.Debugf("Transactional batch executed successfully. ActivityId %s consumed %f RU", resp.ActivityID, resp.RequestCharge)
		} else {
			for idx, opRes := range resp.OperationResults {
				c.logger.Errorf("Rejected batch element %d with status: %d", idx, opRes.StatusCode)
			}

			return errors.New("failed to write message batch")
		}
	}

	return nil
}

func (c *cosmosDBWriter) Close(ctx context.Context) error {
	c.connMut.Lock()
	defer c.connMut.Unlock()

	return nil
}
