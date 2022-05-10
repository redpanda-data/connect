package mock_test

import (
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
)

var _ bundle.NewManagement = &mock.Manager{}
