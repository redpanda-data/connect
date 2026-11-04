// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package turbopuffer

import (
	"context"

	tpuf "github.com/turbopuffer/turbopuffer-go/v2"
)

type client interface {
	CheckConnection(context.Context) error
	Write(context.Context, string, tpuf.NamespaceWriteParams) error
}

type sdkClient struct {
	client tpuf.Client
}

func (c *sdkClient) CheckConnection(ctx context.Context) error {
	_, err := c.client.Namespaces(ctx, tpuf.NamespacesParams{PageSize: tpuf.Int(1)})
	return err
}

func (c *sdkClient) Write(ctx context.Context, namespace string, params tpuf.NamespaceWriteParams) error {
	ns := c.client.Namespace(namespace)
	_, err := ns.Write(ctx, params)
	return err
}
