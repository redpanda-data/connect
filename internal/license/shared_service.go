// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package license

import (
	"errors"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// LoadFromResources attempts to access a license service from a provided
// resources handle and returns the current license it tracks. An error is
// returned if the license service cannot be accessed or cannot provide license
// information.
func LoadFromResources(res *service.Resources) (RedpandaLicense, error) {
	svc := getSharedService(res)
	if svc == nil {
		return RedpandaLicense{}, errors.New("unable to access license service")
	}

	l := svc.loadedLicense.Load()
	if l == nil {
		return RedpandaLicense{}, errors.New("unable to access license information")
	}

	return *l, nil
}

// CheckRunningEnterprise returns a non-nil error if the instance of Redpanda
// Connect is not operating with a valid enterprise license.
func CheckRunningEnterprise(res *service.Resources) error {
	l, err := LoadFromResources(res)
	if err != nil {
		return err
	}
	if !l.AllowsEnterpriseFeatures() {
		return errors.New("this feature requires a valid Redpanda Enterprise Edition license from https://redpanda.com/try-enterprise?origin=rpcn. For more information check out: https://docs.redpanda.com/current/get-started/licenses/")
	}
	return nil
}

type sharedServiceKeyType int

var sharedServiceKey sharedServiceKeyType

func setSharedService(res *service.Resources, svc *Service) {
	res.SetGeneric(sharedServiceKey, svc)
}

func getSharedService(res *service.Resources) *Service {
	reg, _ := res.GetGeneric(sharedServiceKey)
	if reg == nil {
		return nil
	}
	return reg.(*Service)
}
