//go:build !linux

package netclient

// setTCPUserTimeout does not apply to non linux systems
// since it is not supported on other platforms.
// errors and warnings will be silently ignored.
func (c *Config) setTCPUserTimeout(_ int) error {
	return nil
}
