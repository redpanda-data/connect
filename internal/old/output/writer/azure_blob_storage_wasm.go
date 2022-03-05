//go:build wasm
// +build wasm

package writer

import (
	"context"
	"errors"

	"github.com/Jeffail/benthos/v3/internal/component/metrics"
	"github.com/Jeffail/benthos/v3/internal/log"
	"github.com/Jeffail/benthos/v3/internal/message"
)

//------------------------------------------------------------------------------

type dummy interface {
	Type
	ConnectWithContext(ctx context.Context) error
	WriteWithContext(ctx context.Context, msg *message.Batch) error
}

// NewAzureBlobStorage returns an error as it is not supported in WASM builds.
func NewAzureBlobStorage(
	conf AzureBlobStorageConfig,
	log log.Modular,
	stats metrics.Type,
) (dummy, error) {
	return nil, errors.New("Azure blob storage is disabled in WASM builds")
}

//------------------------------------------------------------------------------
