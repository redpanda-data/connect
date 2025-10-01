package netclient

import (
	"fmt"
	"net"
	"syscall"
	"time"
)

// Config contains TCP socket configuration options
type Config struct {
	TCPUserTimeout time.Duration
	KeepAlive      time.Duration
}

// Validate ensures the config matches the recommendation of having keep alives time > tcp_user_timeout, when used.
// KeepAlive MUST be HIGHER than TCP_USER_TIMEOUT per RFC 5482 (https://www.rfc-editor.org/rfc/rfc5482.html).
// If no value is provided, then we set keepalive to 3x the TCP_USER_TIMEOUT time.
func (c *Config) Validate() error {
	if c.TCPUserTimeout > 0 {
		if c.KeepAlive == 0 {
			c.KeepAlive = c.TCPUserTimeout * 3
		}
		if c.KeepAlive <= c.TCPUserTimeout {
			return fmt.Errorf("keep_alive (%s) must be greater than tcp_user_timeout (%s)",
				c.KeepAlive, c.TCPUserTimeout)
		}
	}
	return nil
}

// MergeWith returns a new Config with values from 'override' taking precedence
func (c Config) MergeWith(override Config) Config {
	result := c

	if override.TCPUserTimeout > 0 {
		result.TCPUserTimeout = override.TCPUserTimeout
	}
	if override.KeepAlive > 0 {
		result.KeepAlive = override.KeepAlive
	}

	return result
}

// NewDialer creates a net.Dialer with the configured TCP options
func (c *Config) NewDialer() *net.Dialer {
	dialer := &net.Dialer{
		KeepAlive: c.KeepAlive,
	}

	if c.TCPUserTimeout > 0 {
		dialer.Control = c.applyTCPUserTimeout
	}

	return dialer
}

// applyTCPUserTimeout applies TCP_USER_TIMEOUT socket option by
// using the appropriate file to set this option, client_linux.go.
func (c *Config) applyTCPUserTimeout(network, address string, conn syscall.RawConn) error {

	var setErr error

	err := conn.Control(func(fd uintptr) {
		setErr = c.setTCPUserTimeout(int(fd))
	})

	if err != nil {
		return fmt.Errorf("failed to access raw connection: %w", err)
	}

	return setErr
}
