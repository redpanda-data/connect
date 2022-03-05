//go:build wasm
// +build wasm

package writer

import (
	"errors"

	"github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/internal/log"
)

// NewAzureQueueStorage creates a new Azure Queue Storage writer type.
func NewAzureQueueStorage(conf AzureQueueStorageConfig, log log.Modular, stats metrics.Type) (dummy, error) {
	return nil, errors.New("Azure blob storage is disabled in WASM builds")
}
