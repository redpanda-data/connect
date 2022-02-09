package mock_test

import (
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
)

var _ processor.V1 = mock.Processor(nil)
