package bloblang

import (
	"github.com/Jeffail/benthos/v3/internal/bloblang/plugins"
)

func init() {
	if err := plugins.Register(); err != nil {
		panic(err)
	}
}
