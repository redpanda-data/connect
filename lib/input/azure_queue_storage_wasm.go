// +build wasm

package input

import (
	"errors"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func newAzureQueueStorage(conf AzureQueueStorageConfig, log log.Modular, stats metrics.Type) (reader.Async, error) {
	return nil, errors.New("Azure queue storage is disabled in WASM builds")
}
