// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
