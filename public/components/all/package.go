// Package all imports all component implementations that ship with the open
// source Benthos repo. This is a convenient way of importing every single
// connector at the cost of a larger dependency tree for your application.
package all

import (
	// Import old legacy packages.
	_ "github.com/Jeffail/benthos/v3/public/components/legacy"

	// Import new service packages.
	_ "github.com/Jeffail/benthos/v3/internal/service/gcp"
	_ "github.com/Jeffail/benthos/v3/internal/service/mongodb"
	_ "github.com/Jeffail/benthos/v3/internal/service/pulsar"
)
