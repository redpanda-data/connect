//go:build !arm

package all

import (
	// Import all public sub-categories that do not compile on a 32-bit arch.
	_ "github.com/benthosdev/benthos/v4/public/components/snowflake"
)
