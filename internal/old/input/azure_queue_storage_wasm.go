//go:build wasm
// +build wasm

package input

import (
	"errors"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/old/input/reader"
)

func newAzureQueueStorage(conf AzureQueueStorageConfig, log log.Modular, stats metrics.Type) (reader.Async, error) {
	return nil, errors.New("Azure queue storage is disabled in WASM builds")
}
