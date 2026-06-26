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
	"github.com/redpanda-data/common-go/license"
)

// LoadFromResources attempts to access a license service from a provided
// resources handle and returns the current license it tracks. An error is
// returned if the license service cannot be accessed or cannot provide license
// information.
func LoadFromResources(res *service.Resources) (license.RedpandaLicense, error) {
	svc := getSharedService(res)
	if svc == nil {
		return nil, errors.New("unable to access license service")
	}

	l := svc.loadedLicense.Load()
	if l == nil {
		return nil, errors.New("unable to access license information")
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
	if !l.AllowsEnterpriseFeatures() || !l.IncludesProduct(license.ProductConnect) {
		return errors.New("this feature requires a valid Redpanda Enterprise Edition license that includes the Connect product. For more information check out: https://docs.redpanda.com/redpanda-connect/get-started/licensing/")
	}
	return nil
}

// WrapBatchOutput wraps output with throughput throttling when running under
// the embedded test license. Returns output unchanged under a production license
// or when no license service is registered.
func WrapBatchOutput(res *service.Resources, output service.BatchOutput) service.BatchOutput {
	svc := getSharedService(res)
	if svc == nil || !svc.isTestLicense {
		return output
	}
	t := getThrottler(res)
	if t == nil {
		return output
	}
	return &throttledBatchOutput{wrapped: output, throttler: t}
}

// RegisterServiceFrom copies the license service and throttler from src to dst.
// Use in agent mode so all per-stream Resources share one Service and Throttler
// rather than each getting an independent token bucket.
func RegisterServiceFrom(src, dst *service.Resources) {
	svc := getSharedService(src)
	if svc == nil {
		return
	}
	setSharedService(dst, svc)
	if t := getThrottler(src); t != nil {
		registerThrottler(dst, t)
	}
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

type throttlerKeyType int

var throttlerKey throttlerKeyType

func registerThrottler(res *service.Resources, t *Throttler) {
	res.SetGeneric(throttlerKey, t)
}

func getThrottler(res *service.Resources) *Throttler {
	v, _ := res.GetGeneric(throttlerKey)
	if v == nil {
		return nil
	}
	return v.(*Throttler)
}
