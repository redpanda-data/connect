package mock_test

import (
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

var _ cache.V1 = &mock.Cache{}
