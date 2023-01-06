package svcdiscover

import (
	"fmt"
	"github.com/benthosdev/benthos/v4/internal/log"
	"strings"
)

type SdManager struct {
	conf     Config
	httpAddr string
	Sds      []ServiceDiscoverReg
}

func NewSdManager(conf Config, httpAddr string) *SdManager {
	sdManager := &SdManager{conf: conf, httpAddr: httpAddr}
	sdManager.Sds = append(sdManager.Sds, NewNacos())
	return sdManager
}

func (sm *SdManager) Register(logger log.Modular) error {
	if len(sm.Sds) == 0 {
		return nil
	}
	errs := []string{}
	for _, sd := range sm.Sds {
		err := sd.RegisterInstance(sm.conf, sm.httpAddr, logger)
		if err != nil {
			logger.Errorf("Register Instance: %v", err)
			errs = append(errs, fmt.Sprintf("Register Instance: %v", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%v", strings.Join(errs, ", "))
	}
	return nil
}
