package writer

//------------------------------------------------------------------------------

// AzureBlobStorageConfig contains configuration fields for the AzureBlobStorage output type.
type AzureBlobStorageConfig struct {
	StorageAccount   string `json:"storage_account" yaml:"storage_account"`
	StorageAccessKey string `json:"storage_access_key" yaml:"storage_access_key"`
	Container        string `json:"container" yaml:"container"`
	Path             string `json:"path" yaml:"path"`
	BlobType         string `json:"blob_type" yaml:"blob_type"`
	Timeout          string `json:"timeout" yaml:"timeout"`
	MaxInFlight      int    `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewAzureBlobStorageConfig creates a new Config with default values.
func NewAzureBlobStorageConfig() AzureBlobStorageConfig {
	return AzureBlobStorageConfig{
		StorageAccount:   "",
		StorageAccessKey: "",
		Container:        "",
		Path:             `${!count("files")}-${!timestamp_unix_nano()}.txt`,
		BlobType:         "BLOCK",
		Timeout:          "5s",
		MaxInFlight:      1,
	}
}

//------------------------------------------------------------------------------
