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
	"strings"

	"github.com/redpanda-data/common-go/secrets"
)

const trimPrefixParam = "trimPrefix"

// LookupFn defines the common closure that a secrets management client provides
// and is then fed into a Redpanda Connect cli constructor.
type LookupFn func(context.Context, string) (string, bool)

type lookupTiers []LookupFn

func (l lookupTiers) Lookup(ctx context.Context, key string) (string, bool) {
	for _, fn := range l {
		if v, ok := fn(ctx, key); ok {
			return v, ok
		}
		if ctx.Err() != nil {
			break
		}
	}
	return "", false
}

// ParseLookupURNs attempts to parse a series of secrets lookup solutions
// defined as URNs and returns a single lookup func for obtaining secrets from
// them in the order provided.
//
// A toggle can be provided that determines whether environment variables should
// be considered the last look up option, in which case if all others fail to
// provide a secret then an environment variable under the key is returned if
// found.
func ParseLookupURNs(ctx context.Context, logger *slog.Logger, secretsMgmtUrns ...string) (LookupFn, error) {
	var tiers lookupTiers

	for _, urn := range secretsMgmtUrns {
		tier, err := parseSecretsLookupURN(ctx, logger, urn)
		if err != nil {
			return nil, err
		}
		tiers = append(tiers, tier)
	}

	return tiers.Lookup, nil
}

func parseSecretsLookupURN(ctx context.Context, logger *slog.Logger, urn string) (LookupFn, error) {
	u, err := url.Parse(urn)
	if err != nil {
		return nil, err
	}
	path := strings.TrimPrefix(u.Path, "/")

	switch u.Scheme {
	case "test":
		return func(ctx context.Context, key string) (string, bool) {
			return key + " " + u.Host, true
		}, nil
	case "redis":
		return newRedisSecretsLookup(ctx, logger, u)
	case "env":
		return func(ctx context.Context, key string) (string, bool) {
			return os.LookupEnv(key)
		}, nil
	case "aws":
		secretsManager, err := secrets.NewAWSSecretsManager(ctx, logger, u.Host, u.Query().Get("role"))
		if err != nil {
			return nil, err
		}
		return lookupFn(secrets.NewSecretProvider, secretsManager, path, u.Query().Get(trimPrefixParam))
	case "gcp":
		secretsManager, err := secrets.NewGCPSecretsManager(ctx, logger, u.Host)
		if err != nil {
			return nil, err
		}
		return lookupFn(secrets.NewSecretProvider, secretsManager, path, u.Query().Get(trimPrefixParam))
	case "az":
		secretsManager, err := secrets.NewAzSecretsManager(logger, "https://"+u.Host)
		if err != nil {
			return nil, err
		}
		return lookupFn(secrets.NewSecretProvider, secretsManager, path, u.Query().Get(trimPrefixParam))
	case "none":
		return func(ctx context.Context, key string) (string, bool) {
			return "", false
		}, nil
	default:
		return nil, fmt.Errorf("secrets scheme %v not recognized", u.Scheme)
	}
}

func lookupFn(providerFn secrets.SecretProviderFn, secretsManager secrets.SecretAPI, prefix, trimPrefix string) (LookupFn, error) {
	provider, err := providerFn(secretsManager, prefix, trimPrefix)
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, key string) (string, bool) {
		return provider.GetSecretValue(ctx, key)
	}, nil
}
