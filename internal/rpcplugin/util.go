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

package rpcplugin

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const retryCount = 3
const maxStartupTime = 30 * time.Second

func newUnixSocketAddr() (string, error) {
	dir, err := os.MkdirTemp(os.TempDir(), "rpcn_plugin_*")
	if err != nil {
		return "", fmt.Errorf("unable to create temp dir: %w", err)
	}
	socketPath := filepath.Join(dir, "plugin.sock")
	return "unix:" + socketPath, nil
}
