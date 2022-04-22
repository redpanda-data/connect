// Package pure imports all component implementations that are pure, in that
// they do not interact with external systems. This includes all base component
// types such as brokers and is likely necessary as a base for all builds.
package pure

import (
	// Import only pure packages.
	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)
