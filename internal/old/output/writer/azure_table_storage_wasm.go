//go:build wasm
// +build wasm

package writer

import (
	"errors"

	"github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/internal/log"
)

//------------------------------------------------------------------------------

// NewAzureTableStorage returns an error as it is not supported in WASM builds.
func NewAzureTableStorage(
	conf AzureTableStorageConfig,
	log log.Modular,
	stats metrics.Type,
) (dummy, error) {
	return nil, errors.New("Azure table storage is disabled in WASM builds")
}

//------------------------------------------------------------------------------
