// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package cli

import (
	"context"
	"os"
)

type lookupTiers []func(context.Context, string) (string, bool)

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

func createSecretsLookupFn(ctx context.Context, secretsMgmtUrns []string, disableEnvLookup bool) (func(context.Context, string) (string, bool), error) {
	var tiers lookupTiers

	for _, urn := range secretsMgmtUrns {
		// TODO: Instanciate the secret manager and add a lookup tier
		_ = urn
	}

	if !disableEnvLookup {
		tiers = append(tiers, func(ctx context.Context, key string) (string, bool) {
			return os.LookupEnv(key)
		})
	}
	return tiers.Lookup, nil
}
