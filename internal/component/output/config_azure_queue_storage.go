package output

import "github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"

// AzureQueueStorageConfig contains configuration fields for the output Azure Queue Storage type.
type AzureQueueStorageConfig struct {
	StorageAccount          string             `json:"storage_account" yaml:"storage_account"`
	StorageAccessKey        string             `json:"storage_access_key" yaml:"storage_access_key"`
	StorageConnectionString string             `json:"storage_connection_string" yaml:"storage_connection_string"`
	QueueName               string             `json:"queue_name" yaml:"queue_name"`
	TTL                     string             `json:"ttl" yaml:"ttl"`
	MaxInFlight             int                `json:"max_in_flight" yaml:"max_in_flight"`
	Batching                batchconfig.Config `json:"batching" yaml:"batching"`
}

// NewAzureQueueStorageConfig creates a new Config with default values.
func NewAzureQueueStorageConfig() AzureQueueStorageConfig {
	return AzureQueueStorageConfig{
		StorageAccount:          "",
		StorageAccessKey:        "",
		StorageConnectionString: "",
		QueueName:               "",
		TTL:                     "",
		MaxInFlight:             64,
		Batching:                batchconfig.NewConfig(),
	}
}
