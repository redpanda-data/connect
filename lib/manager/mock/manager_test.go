package mock_test

import (
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
)

var _ interop.Manager = &mock.Manager{}
