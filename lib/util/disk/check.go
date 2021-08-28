//go:build !windows
// +build !windows

package disk

import "syscall"

// TotalRemaining returns the space remaining on the disk in bytes.
func TotalRemaining(path string) uint64 {
	var stat syscall.Statfs_t
	syscall.Statfs(path, &stat)

	return stat.Bfree * uint64(stat.Bsize)
}
