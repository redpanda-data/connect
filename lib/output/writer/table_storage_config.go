package writer

import (
	"github.com/Jeffail/benthos/v3/lib/message/batch"
)

//------------------------------------------------------------------------------

// AzureTableStorageConfig contains configuration fields for the AzureTableStorage output type.
type AzureTableStorageConfig struct {
	StorageAccount          string             `json:"storage_account" yaml:"storage_account"`
	StorageAccessKey        string             `json:"storage_access_key" yaml:"storage_access_key"`
	StorageConnectionString string             `json:"storage_connection_string" yaml:"storage_connection_string"`
	TableName               string             `json:"table_name" yaml:"table_name"`
	PartitionKey            string             `json:"partition_key" yaml:"partition_key"`
	RowKey                  string             `json:"row_key" yaml:"row_key"`
	Properties              map[string]string  `json:"properties" yaml:"properties"`
	InsertType              string             `json:"insert_type" yaml:"insert_type"`
	Timeout                 string             `json:"timeout" yaml:"timeout"`
	MaxInFlight             int                `json:"max_in_flight" yaml:"max_in_flight"`
	Batching                batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewAzureTableStorageConfig creates a new Config with default values.
func NewAzureTableStorageConfig() AzureTableStorageConfig {
	return AzureTableStorageConfig{
		StorageAccount:          "",
		StorageAccessKey:        "",
		StorageConnectionString: "",
		TableName:               "",
		PartitionKey:            "",
		RowKey:                  "",
		Properties:              map[string]string{},
		InsertType:              "INSERT",
		Timeout:                 "5s",
		MaxInFlight:             1,
		Batching:                batch.NewPolicyConfig(),
	}
}

//------------------------------------------------------------------------------
