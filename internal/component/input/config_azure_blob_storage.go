package input

// AzureBlobStorageConfig contains configuration fields for the AzureBlobStorage
// input type.
type AzureBlobStorageConfig struct {
	StorageAccount          string `json:"storage_account" yaml:"storage_account"`
	StorageAccessKey        string `json:"storage_access_key" yaml:"storage_access_key"`
	StorageSASToken         string `json:"storage_sas_token" yaml:"storage_sas_token"`
	StorageConnectionString string `json:"storage_connection_string" yaml:"storage_connection_string"`
	Container               string `json:"container" yaml:"container"`
	Prefix                  string `json:"prefix" yaml:"prefix"`
	Codec                   string `json:"codec" yaml:"codec"`
	DeleteObjects           bool   `json:"delete_objects" yaml:"delete_objects"`
}

// NewAzureBlobStorageConfig creates a new AzureBlobStorageConfig with default
// values.
func NewAzureBlobStorageConfig() AzureBlobStorageConfig {
	return AzureBlobStorageConfig{
		Codec: "all-bytes",
	}
}
