package mock_test

import (
	"github.com/Jeffail/benthos/v3/internal/component/output"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
)

var _ output.Sync = mock.OutputWriter(nil)
