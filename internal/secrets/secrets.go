// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package secrets

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
)

// LookupFn defines the common closure that a secrets management client provides
// and is then fed into a Redpanda Connect cli constructor.
type LookupFn func(context.Context, string) (string, bool)

// ExistsFn checks secret presence without retrieving its secret value
type ExistsFn func(context.Context, string) bool

type secretProvider struct {
	lookupFn LookupFn
	existsFn ExistsFn
}

type lookupTiers []secretProvider

func (l lookupTiers) Lookup(ctx context.Context, key string) (string, bool) {
	for _, sp := range l {
		if v, ok := sp.lookupFn(ctx, key); ok {
			return v, ok
		}
		if ctx.Err() != nil {
			break
		}
	}
	return "", false
}

func (l lookupTiers) Exists(ctx context.Context, key string) bool {
	for _, sp := range l {
		if sp.existsFn(ctx, key) {
			return true
		}
		if ctx.Err() != nil {
			break
		}
	}
	return false
}

func constantExistsFn(result bool) ExistsFn {
	return func(ctx context.Context, key string) bool {
		return result
	}
}

func defaultExistsFn(lookupFn func(string) (string, bool)) ExistsFn {
	return func(_ context.Context, key string) bool {
		_, found := lookupFn(key)
		return found
	}
}

// ParseLookupURNs attempts to parse a series of secrets lookup solutions
// defined as URNs and returns a single lookup func for obtaining secrets from
// them in the order provided.
//
// A toggle can be provided that determines whether environment variables should
// be considered the last look up option, in which case if all others fail to
// provide a secret then an environment variable under the key is returned if
// found.
func ParseLookupURNs(ctx context.Context, logger *slog.Logger, secretsMgmtUrns ...string) (LookupFn, ExistsFn, error) {
	var tiers lookupTiers

	for _, urn := range secretsMgmtUrns {
		lookupFn, existsFn, err := parseSecretsLookupURN(ctx, logger, urn)
		if err != nil {
			return nil, nil, err
		}
		tiers = append(tiers, secretProvider{
			lookupFn: lookupFn,
			existsFn: existsFn,
		})
	}

	return tiers.Lookup, tiers.Exists, nil
}

func parseSecretsLookupURN(ctx context.Context, logger *slog.Logger, urn string) (LookupFn, ExistsFn, error) {
	u, err := url.Parse(urn)
	if err != nil {
		return nil, nil, err
	}

	switch u.Scheme {
	case "test":
		return func(ctx context.Context, key string) (string, bool) {
				return key + " " + u.Host, true
			},
			constantExistsFn(true), nil
	case "aws":
		return newSecretManager(ctx, logger, u, newAWSSecretsManager)
	case "gcp":
		return newSecretManager(ctx, logger, u, newGCPSecretsManager)
	case "az":
		return newSecretManager(ctx, logger, u, newAzSecretsManager)
	case "redis":
		return newRedisSecretsLookup(ctx, logger, u)
	case "env":
		return func(ctx context.Context, key string) (string, bool) {
			return os.LookupEnv(key)
		}, defaultExistsFn(os.LookupEnv), nil
	case "none":
		return func(ctx context.Context, key string) (string, bool) {
				return "", false
			},
			constantExistsFn(false), nil
	default:
		return nil, nil, fmt.Errorf("secrets scheme %v not recognized", u.Scheme)
	}
}
