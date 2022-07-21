// Package io contains component implementations that have a small dependency
// footprint (mostly standard library) and interact with external systems via
// the filesystem and/or network sockets.
//
// EXPERIMENTAL: The specific components excluded by this package may change
// outside of major version releases. This means we may choose to remove certain
// plugins if we determine that their dependencies are likely to interfere with
// the goals of this package.
package io

import (
	// Import only io packages.
	_ "github.com/benthosdev/benthos/v4/internal/impl/io"
)
