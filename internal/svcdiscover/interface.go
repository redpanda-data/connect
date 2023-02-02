package svcdiscover

import (
	"github.com/benthosdev/benthos/v4/internal/log"
)

// ServiceDiscoverReg service discover register interface
type ServiceDiscoverReg interface {
	RegisterInstance(conf Config, httpAddr string, logger log.Modular) error
}
