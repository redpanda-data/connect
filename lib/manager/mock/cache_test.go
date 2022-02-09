package mock_test

import (
	"github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
)

var _ cache.V1 = &mock.Cache{}
