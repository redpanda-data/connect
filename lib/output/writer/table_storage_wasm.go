// +build wasm

package writer

import (
	"context"
	"errors"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

type dummy interface {
	Type
	ConnectWithContext(ctx context.Context) error
	WriteWithContext(ctx context.Context, msg types.Message) error
}

// NewAzureTableStorage returns an error as it is not supported in WASM builds.
func NewAzureTableStorage(
	conf AzureTableStorageConfig,
	log log.Modular,
	stats metrics.Type,
) (dummy, error) {
	return nil, errors.New("Azure table storage is disabled in WASM builds")
}

//------------------------------------------------------------------------------
