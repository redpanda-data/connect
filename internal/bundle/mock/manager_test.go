package mock_test

import (
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/bundle/mock"
	"github.com/benthosdev/benthos/v4/internal/interop"
)

var _ interop.Manager = &mock.Manager{}
var _ bundle.NewManagement = &mock.Manager{}
