package cosmosdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/gofrs/uuid"

	"github.com/benthosdev/benthos/v4/public/service"
)

// Maximum number of messages which can be pushed to Azure in a TransactionalBatch
// Details here: https://learn.microsoft.com/en-us/azure/cosmos-db/concepts-limits#per-request-limits
// and here: https://github.com/Azure/azure-cosmos-dotnet-v3/issues/1057
const maxTransactionalBatchSize = 100

// ExecMessageBatch creates a CosmosDB TransactionalBatch from the provided message batch and executes it
func ExecMessageBatch(ctx context.Context, batch service.MessageBatch, client *azcosmos.ContainerClient,
	config CRUDConfig, enableContentResponseOnWrite bool,
) (azcosmos.TransactionalBatchResponse, error) {
	if len(batch) > maxTransactionalBatchSize {
		return azcosmos.TransactionalBatchResponse{},
			fmt.Errorf("current batch has %d messages, but the CosmosDB transactional batch limit is %d", len(batch), maxTransactionalBatchSize)
	}

	pkQueryResult, err := batch.BloblangQueryValue(0, config.PartitionKeys)
	if err != nil {
		return azcosmos.TransactionalBatchResponse{}, fmt.Errorf("failed to evaluate partition key values: %s", err)
	}

	// TODO: Enable support for hierarchical / empty Partition Keys this when the following issues are addressed:
	// - https://github.com/Azure/azure-sdk-for-go/issues/18578
	// - https://github.com/Azure/azure-sdk-for-go/issues/21063
	if pkValuesList, ok := pkQueryResult.([]any); ok {
		if len(pkValuesList) != 1 {
			return azcosmos.TransactionalBatchResponse{}, errors.New("only one partition key is supported")
		}
		pkQueryResult = pkValuesList[0]
	}

	pkValue, err := GetTypedPartitionKeyValue(pkQueryResult)
	if err != nil {
		return azcosmos.TransactionalBatchResponse{}, err
	}

	tb := client.NewTransactionalBatch(pkValue)
	for idx, msg := range batch {
		var b []byte
		var err error
		if config.Operation == OperationCreate && config.AutoID {
			structuredMsg, err := msg.AsStructured()
			if err != nil {
				return azcosmos.TransactionalBatchResponse{}, fmt.Errorf("failed to get message bytes: %s", err)
			}

			if obj, ok := structuredMsg.(map[string]any); ok {
				if _, ok := obj["id"]; !ok {
					u4, err := uuid.NewV4()
					if err != nil {
						return azcosmos.TransactionalBatchResponse{}, fmt.Errorf("failed to generate uuid: %s", err)
					}
					obj["id"] = u4.String()
				}
			} else {
				return azcosmos.TransactionalBatchResponse{}, fmt.Errorf("message must contain an object, got %T instead", structuredMsg)
			}

			if b, err = json.Marshal(structuredMsg); err != nil {
				return azcosmos.TransactionalBatchResponse{}, fmt.Errorf("failed to marshal message to json: %s", err)
			}
		} else {
			b, err = msg.AsBytes()
			if err != nil {
				return azcosmos.TransactionalBatchResponse{}, fmt.Errorf("failed to get message bytes: %s", err)
			}
		}

		var id string
		if config.ItemID != nil {
			id = config.ItemID.String(msg)
		}

		switch config.Operation {
		case OperationCreate:
			tb.CreateItem(b, nil)
		case OperationDelete:
			tb.DeleteItem(id, nil)
		case OperationReplace:
			tb.ReplaceItem(id, b, nil)
		case OperationUpsert:
			tb.UpsertItem(b, nil)
		case OperationRead:
			tb.ReadItem(id, nil)
		case OperationPatch:
			patch := azcosmos.PatchOperations{}
			if config.PatchCondition != nil {
				condition, err := config.PatchCondition.TryString(msg)
				if err != nil {
					return azcosmos.TransactionalBatchResponse{}, fmt.Errorf("failed to get patch condition: %s", err)
				}
				if condition != "" {
					patch.SetCondition(condition)
				}
			}

			for _, po := range config.PatchOperations {
				path, err := po.Path.TryString(msg)
				if err != nil {
					return azcosmos.TransactionalBatchResponse{}, fmt.Errorf("failed to get patch path: %s", err)
				}

				var value any
				if po.Value != nil {
					if value, err = batch.BloblangQueryValue(idx, po.Value); err != nil {
						return azcosmos.TransactionalBatchResponse{}, fmt.Errorf("failed to evaluate patch value: %s", err)
					}
				}

				switch po.Operation {
				case patchOperationAdd:
					patch.AppendAdd(path, value)
				case patchOperationIncrement:
					if v, ok := value.(int64); ok {
						patch.AppendIncrement(path, v)
					} else {
						return azcosmos.TransactionalBatchResponse{}, fmt.Errorf("expected patch value to be int64, got %T", value)
					}
				case patchOperationRemove:
					patch.AppendRemove(path)
				case patchOperationReplace:
					patch.AppendReplace(path, value)
				case patchOperationSet:
					patch.AppendSet(path, value)
				}
			}
			tb.PatchItem(id, patch, nil)
		}
	}

	return client.ExecuteTransactionalBatch(ctx, tb, &azcosmos.TransactionalBatchOptions{
		EnableContentResponseOnWrite: enableContentResponseOnWrite,
	})
}
