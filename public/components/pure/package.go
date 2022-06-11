// Package pure imports all component implementations that are pure, in that
// they do not interact with external systems. This includes all base component
// types such as brokers and is likely necessary as a base for all builds.
//
// EXPERIMENTAL: The specific components excluded by this package may change
// outside of major version releases. This means we may choose to remove certain
// plugins if we determine that their dependencies are likely to interfere with
// the goals of this package.
package pure

import (
	"github.com/benthosdev/benthos/v4/internal/template"

	// Import only pure packages.
	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func init() {
	_ = template.InitNativeTemplates()
}
