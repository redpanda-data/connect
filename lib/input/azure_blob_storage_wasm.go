//go:build wasm
// +build wasm

package input

import (
	"errors"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func newAzureBlobStorage(conf AzureBlobStorageConfig, log log.Modular, stats metrics.Type) (reader.Async, error) {
	return nil, errors.New("Azure blob storage is disabled in WASM builds")
}
