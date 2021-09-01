//go:build windows
// +build windows

package disk

import (
	"syscall"
	"unsafe"
)

// TotalRemaining returns the space remaining on the disk in bytes.
func TotalRemaining(path string) uint64 {
	var freeBytes, totalBytes, availableBytes int64

	h := syscall.MustLoadDLL("kernel32.dll")
	c := h.MustFindProc("GetDiskFreeSpaceExW")

	c.Call(
		uintptr(unsafe.Pointer(syscall.StringToUTF16Ptr(path))),
		uintptr(unsafe.Pointer(&freeBytes)),
		uintptr(unsafe.Pointer(&totalBytes)),
		uintptr(unsafe.Pointer(&availableBytes)))

	return uint64(freeBytes)
}
