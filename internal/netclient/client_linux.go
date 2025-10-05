//go:build linux

package netclient

import (
	"fmt"
	"syscall"
)

// Linux specific constant that is used to reference
// the tcp_user_timeout option.
const TCP_USER_TIMEOUT = 18

// setTCPUserTimeout sets the TCP_USER_TIMEOUT socket option on Linux
func (c *Config) setTCPUserTimeout(fd int) error {

	timeoutMs := int(c.TCPUserTimeout.Milliseconds())

	err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, TCP_USER_TIMEOUT, timeoutMs)
	if err != nil {
		return fmt.Errorf("failed to set TCP_USER_TIMEOUT: %w", err)
	}

	return nil
}
