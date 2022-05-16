package input

// AzureQueueStorageConfig contains configuration fields for the AzureQueueStorage
// input type.
type AzureQueueStorageConfig struct {
	StorageAccount           string `json:"storage_account" yaml:"storage_account"`
	StorageAccessKey         string `json:"storage_access_key" yaml:"storage_access_key"`
	StorageSASToken          string `json:"storage_sas_token" yaml:"storage_sas_token"`
	StorageConnectionString  string `json:"storage_connection_string" yaml:"storage_connection_string"`
	QueueName                string `json:"queue_name" yaml:"queue_name"`
	DequeueVisibilityTimeout string `json:"dequeue_visibility_timeout" yaml:"dequeue_visibility_timeout"`
	MaxInFlight              int32  `json:"max_in_flight" yaml:"max_in_flight"`
	TrackProperties          bool   `json:"track_properties" yaml:"track_properties"`
}

// NewAzureQueueStorageConfig creates a new AzureQueueStorageConfig with default
// values.
func NewAzureQueueStorageConfig() AzureQueueStorageConfig {
	return AzureQueueStorageConfig{
		DequeueVisibilityTimeout: "30s",
		MaxInFlight:              10,
		TrackProperties:          false,
	}
}
