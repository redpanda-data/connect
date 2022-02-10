package mock_test

import (
	"github.com/Jeffail/benthos/v3/internal/component/input"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
)

var _ input.Streamed = &mock.Input{}
