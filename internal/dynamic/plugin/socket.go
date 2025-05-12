/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package plugin

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
