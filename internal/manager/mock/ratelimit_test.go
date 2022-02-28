package mock_test

import (
	"github.com/Jeffail/benthos/v3/internal/component/ratelimit"
	"github.com/Jeffail/benthos/v3/internal/manager/mock"
)

var _ ratelimit.V1 = mock.RateLimit(nil)
