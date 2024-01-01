package cosmosdb

import (
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

// GetTypedPartitionKeyValue returns a typed partition key value
func GetTypedPartitionKeyValue(pkValue any) (azcosmos.PartitionKey, error) {
	switch val := pkValue.(type) {
	case string:
		return azcosmos.NewPartitionKeyString(val), nil
	case bool:
		return azcosmos.NewPartitionKeyBool(val), nil
	case int64:
		return azcosmos.NewPartitionKeyNumber(float64(val)), nil
	case float64:
		return azcosmos.NewPartitionKeyNumber(val), nil
	case nil:
		return azcosmos.NullPartitionKey, nil
	default:
		return azcosmos.PartitionKey{}, fmt.Errorf("unsupported partition key type: %T", pkValue)
	}
}
