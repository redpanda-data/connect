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

// WrapBatchOutput wraps output with egress throttling when running under
// the embedded dev license. Returns output unchanged under a production license
// or when no license service is registered.
func WrapBatchOutput(res *service.Resources, output service.BatchOutput) service.BatchOutput {
	svc := getSharedService(res)
	if svc == nil || !svc.isTestLicense {
		return output
	}
	t := getEgressThrottler(res)
	if t == nil {
		return output
	}
	return &throttledBatchOutput{wrapped: output, throttler: t}
}

// WrapBatchInput wraps input with ingress throttling when running under
// the embedded dev license. Returns input unchanged under a production license
// or when no license service is registered.
func WrapBatchInput(res *service.Resources, input service.BatchInput) service.BatchInput {
	svc := getSharedService(res)
	if svc == nil || !svc.isTestLicense {
		return input
	}
	t := getIngressThrottler(res)
	if t == nil {
		return input
	}
	return &throttledBatchInput{wrapped: input, throttler: t}
}

// MustRegisterEnterpriseBatchOutput registers an enterprise batch output.
// The license check and dev-license egress throttle wrapping are applied
// automatically; the ctor must not call CheckRunningEnterprise or
// WrapBatchOutput itself.
func MustRegisterEnterpriseBatchOutput(
	name string,
	spec *service.ConfigSpec,
	ctor func(*service.ParsedConfig, *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error),
) {
	service.MustRegisterBatchOutput(name, spec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			if err := CheckRunningEnterprise(mgr); err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			out, bp, mif, err := ctor(conf, mgr)
			if err == nil {
				out = WrapBatchOutput(mgr, out)
			}
			return out, bp, mif, err
		})
}

// WrapInput wraps input with ingress throttling when running under
// the embedded dev license. Returns input unchanged under a production license
// or when no license service is registered.
func WrapInput(res *service.Resources, input service.Input) service.Input {
	svc := getSharedService(res)
	if svc == nil || !svc.isTestLicense {
		return input
	}
	t := getIngressThrottler(res)
	if t == nil {
		return input
	}
	return &throttledInput{wrapped: input, throttler: t}
}

// MustRegisterEnterpriseInput registers an enterprise input.
// The license check and dev-license ingress throttle wrapping are applied
// automatically; the ctor must not call CheckRunningEnterprise or
// WrapInput itself.
func MustRegisterEnterpriseInput(
	name string,
	spec *service.ConfigSpec,
	ctor func(*service.ParsedConfig, *service.Resources) (service.Input, error),
) {
	service.MustRegisterInput(name, spec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			if err := CheckRunningEnterprise(mgr); err != nil {
				return nil, err
			}
			in, err := ctor(conf, mgr)
			if err == nil {
				in = WrapInput(mgr, in)
			}
			return in, err
		})
}

// MustRegisterEnterpriseBatchInput registers an enterprise batch input.
// The license check and dev-license ingress throttle wrapping are applied
// automatically; the ctor must not call CheckRunningEnterprise or
// WrapBatchInput itself.
func MustRegisterEnterpriseBatchInput(
	name string,
	spec *service.ConfigSpec,
	ctor func(*service.ParsedConfig, *service.Resources) (service.BatchInput, error),
) {
	service.MustRegisterBatchInput(name, spec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			if err := CheckRunningEnterprise(mgr); err != nil {
				return nil, err
			}
			in, err := ctor(conf, mgr)
			if err == nil {
				in = WrapBatchInput(mgr, in)
			}
			return in, err
		})
}

// RegisterServiceFrom copies the license service and throttlers from src to dst.
// Use in agent mode so all per-stream Resources share one Service and Throttler
// rather than each getting an independent token bucket.
func RegisterServiceFrom(src, dst *service.Resources) {
	svc := getSharedService(src)
	if svc == nil {
		return
	}
	setSharedService(dst, svc)
	if t := getEgressThrottler(src); t != nil {
		registerEgressThrottler(dst, t)
	}
	if t := getIngressThrottler(src); t != nil {
		registerIngressThrottler(dst, t)
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

type (
	egressThrottlerKeyType  int
	ingressThrottlerKeyType int
)

var (
	egressThrottlerKey  egressThrottlerKeyType
	ingressThrottlerKey ingressThrottlerKeyType
)

func registerEgressThrottler(res *service.Resources, t *Throttler) {
	res.SetGeneric(egressThrottlerKey, t)
}

func getEgressThrottler(res *service.Resources) *Throttler {
	v, _ := res.GetGeneric(egressThrottlerKey)
	if v == nil {
		return nil
	}
	return v.(*Throttler)
}

func registerIngressThrottler(res *service.Resources, t *Throttler) {
	res.SetGeneric(ingressThrottlerKey, t)
}

func getIngressThrottler(res *service.Resources) *Throttler {
	v, _ := res.GetGeneric(ingressThrottlerKey)
	if v == nil {
		return nil
	}
	return v.(*Throttler)
}
