package pipeline

import iprocessor "github.com/Jeffail/benthos/v3/internal/component/processor"

// Type is implemented by all pipeline implementations.
type Type interface {
	iprocessor.Pipeline
}
