package pure

import (
	"context"

	"github.com/benthosdev/benthos/v4/public/service"
)

// batchedCache represents a cache where the underlying implementation is able
// to benefit from batched set requests. This interface is optional for caches
// and when implemented will automatically be utilised where possible.
// copied from `service.batchedCache`
type batchedCache interface {
	// SetMulti attempts to set multiple cache items in as few requests as
	// possible.
	SetMulti(ctx context.Context, keyValues ...service.CacheItem) error
}
