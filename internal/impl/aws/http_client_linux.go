//go:build linux

// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aws

import (
	"fmt"
	"net"
	"net/http"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// newHTTPClientWithTCPUserTimeout creates an HTTP client with configurable TCP_USER_TIMEOUT on Linux.
// If tcpUserTimeout is 0, returns nil then use Go's default HTTP client.
func newHTTPClientWithTCPUserTimeout(tcpUserTimeout time.Duration) (*http.Client, error) {
	if tcpUserTimeout == 0 {
		return http.DefaultClient, nil
	}

	// KeepAlive MUST be HIGHER than TCP_USER_TIMEOUT per RFC 5482 (https://www.rfc-editor.org/rfc/rfc5482.html).
	// If this is set, then we set keepalive to 3x the TCP_USER_TIMEOUT time.
	keepAlive := tcpUserTimeout * 3

	// Create custom dialer with custom values defined by the user.
	dialer := &net.Dialer{
		Timeout:   tcpUserTimeout,
		KeepAlive: keepAlive,
	}

	// Assign the control function to the dialer.
	dialer.Control = func(_, _ string, c syscall.RawConn) error {
		var sockErr error
		controlErr := c.Control(func(fd uintptr) {
			// Setting TCP_USER_TIMEOUT
			sockErr = unix.SetsockoptInt(
				int(fd),
				unix.IPPROTO_TCP,
				unix.TCP_USER_TIMEOUT,
				int(tcpUserTimeout.Milliseconds()),
			)
		})

		if controlErr != nil {
			return fmt.Errorf("failed to set TCP_USER_TIMEOUT in control function: %w", controlErr)
		}
		if sockErr != nil {
			return fmt.Errorf("failed to set TCP_USER_TIMEOUT: %w", sockErr)
		}
		return nil
	}

	// Create an http.Transport with the custom dialer.
	transport := &http.Transport{
		DialContext: dialer.DialContext,
	}

	// Create and return the HTTP client.
	return &http.Client{
		Transport: transport,
	}, nil
}
