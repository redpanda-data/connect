package mock_test

import (
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

var _ processor.V1 = mock.Processor(nil)
