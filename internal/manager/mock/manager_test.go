package mock_test

import (
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

var _ interop.Manager = &mock.Manager{}
