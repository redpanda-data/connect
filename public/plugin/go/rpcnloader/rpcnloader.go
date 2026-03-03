// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package rpcnloader provides utilities for discovering and registering
// YAML-manifest-based Redpanda Connect RPC plugins at startup.
package rpcnloader

import (
	"io/fs"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/internal/rpcplugin"
)

// DiscoverAndRegisterPlugins discovers YAML plugin manifests from the given
// paths (glob patterns evaluated against the provided filesystem), reads them,
// and registers the described plugins with the provided service environment.
//
// Use this to load and register your own YAML-manifest-based RPC plugins at
// startup, before running a Redpanda Connect pipeline.
func DiscoverAndRegisterPlugins(fsys fs.FS, env *service.Environment, paths []string) error {
	return rpcplugin.DiscoverAndRegisterPlugins(fsys, env, paths)
}
