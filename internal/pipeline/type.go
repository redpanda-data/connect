package pipeline

import iprocessor "github.com/benthosdev/benthos/v4/internal/component/processor"

// Type is implemented by all pipeline implementations.
type Type interface {
	iprocessor.Pipeline
}
