// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package serviceaccount

import (
	"context"
	"fmt"

	"github.com/a2aproject/a2a-go/a2aclient"
	"golang.org/x/oauth2"
)

// OAuth2BearerInterceptor adds OAuth2 Bearer tokens to outgoing A2A requests.
type OAuth2BearerInterceptor struct {
	a2aclient.PassthroughInterceptor
	TokenSource oauth2.TokenSource
}

// Before adds the OAuth2 Bearer token to the request metadata.
func (i *OAuth2BearerInterceptor) Before(ctx context.Context, req *a2aclient.Request) (context.Context, error) {
	token, err := i.TokenSource.Token()
	if err != nil {
		return ctx, fmt.Errorf("failed to get OAuth2 token: %w", err)
	}

	if req.Meta == nil {
		req.Meta = make(a2aclient.CallMeta)
	}
	req.Meta["Authorization"] = []string{"Bearer " + token.AccessToken}

	return ctx, nil
}
