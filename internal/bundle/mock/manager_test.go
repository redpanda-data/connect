package mock_test

import (
	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/bundle/mock"
	"github.com/Jeffail/benthos/v3/internal/interop"
)

var _ interop.Manager = &mock.Manager{}
var _ bundle.NewManagement = &mock.Manager{}
