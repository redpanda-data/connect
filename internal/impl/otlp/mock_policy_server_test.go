// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package otlp_test

import (
	"context"
	"net"
	"net/http"
	"testing"

	policymaterializerv1connect "buf.build/gen/go/redpandadata/common/connectrpc/go/redpanda/policymaterializer/v1/policymaterializerv1connect"
	policymaterializerv1 "buf.build/gen/go/redpandadata/common/protocolbuffers/go/redpanda/policymaterializer/v1"
	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/stretchr/testify/require"
)

// mockPolicyMaterializerServer streams policies from a channel until closed.
type mockPolicyMaterializerServer struct {
	policies chan *policymaterializerv1.DataplanePolicy
}

func (m *mockPolicyMaterializerServer) WatchPolicy(
	ctx context.Context,
	_ *connect.Request[policymaterializerv1.WatchPolicyRequest],
	stream *connect.ServerStream[policymaterializerv1.WatchPolicyResponse],
) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case p, ok := <-m.policies:
			if !ok {
				return nil
			}
			if err := stream.Send(&policymaterializerv1.WatchPolicyResponse{Policy: p}); err != nil {
				return err
			}
		}
	}
}

// startMockPolicyEndpoint starts an h2c Connect policy materializer server and
// returns its base URL. The server is shut down via t.Cleanup.
func startMockPolicyEndpoint(t *testing.T, svc policymaterializerv1connect.PolicyMaterializerServiceHandler) string {
	t.Helper()
	mux := http.NewServeMux()
	path, handler := policymaterializerv1connect.NewPolicyMaterializerServiceHandler(svc)
	mux.Handle(path, handler)

	lis, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := &http.Server{Handler: h2c.NewHandler(mux, &http2.Server{})}
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(func() { srv.Close() })

	return "http://" + lis.Addr().String()
}

// allowAllDataplanePolicy returns a policy granting all given permissions to
// a principal, scoped to the given resource name.
func allowAllDataplanePolicy(permissions []string, principal, resourceName string) *policymaterializerv1.DataplanePolicy {
	perms := make([]string, len(permissions))
	copy(perms, permissions)
	return &policymaterializerv1.DataplanePolicy{
		Roles: []*policymaterializerv1.DataplaneRole{
			{Id: "allow-all", Permissions: perms},
		},
		Bindings: []*policymaterializerv1.DataplaneRoleBinding{
			{RoleId: "allow-all", Principal: principal, Scope: resourceName},
		},
	}
}
