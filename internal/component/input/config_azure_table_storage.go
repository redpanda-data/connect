package input

// AzureTableStorageConfig contains configuration fields for the AzureTableStorage
// input type.
type AzureTableStorageConfig struct {
	StorageAccount          string  `json:"storage_account" yaml:"storage_account"`
	StorageAccessKey        string  `json:"storage_access_key" yaml:"storage_access_key"`
	StorageSASToken         string  `json:"storage_sas_token" yaml:"storage_sas_token"`
	StorageConnectionString string  `json:"storage_connection_string" yaml:"storage_connection_string"`
	TableName               string  `json:"table_name" yaml:"table_name"`
	Filter                  *string `json:"filter" yaml:"filter"`
	Select                  *string `json:"select" yaml:"select"`
	PageSize                *int32  `json:"page_page" yaml:"page_size"`
}

// NewAzureTableStorageConfig creates a new AzureBlobStorageConfig with default values.
func NewAzureTableStorageConfig() AzureTableStorageConfig {
	return AzureTableStorageConfig{}
}
