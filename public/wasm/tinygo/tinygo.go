//go:build tinygo

package tinygo

import (
	"reflect"
	"unsafe"
)

// _msg_set_bytes is a WebAssembly import which writes the contents of the
// message being processed to a byte array (linear memory offset, byteCount).
//
// Note: In TinyGo "//export" on a func is actually an import!
//
//go:wasm-module benthos_wasm
//export v0_msg_set_bytes
func _v0_msg_set_bytes(ptr, size uint32)

// SetMsgBytes sets the contents of the message currently being processed to the
// value provided.
func SetMsgBytes(b []byte) error {
	resP, resS := bytesToPtr(b)
	_v0_msg_set_bytes(resP, resS)
	return nil
}

// _msg_as_bytes is a WebAssembly import which obtains the contents of the
// message being processed as a byte array (linear memory offset, byteCount).
//
// Note: This uses a uint64 instead of two result values for compatibility with
// WebAssembly 1.0.
//
// Note: In TinyGo "//export" on a func is actually an import!
//
//go:wasm-module benthos_wasm
//export v0_msg_as_bytes
func _v0_msg_as_bytes() (ptrSize uint64)

// GetMsgAsBytes returns the contents of the message currently being processed
// as bytes.
func GetMsgAsBytes() ([]byte, error) {
	ptrSize := _v0_msg_as_bytes()
	contentPtr := uint32(ptrSize >> 32)
	contentSize := uint32(ptrSize)
	return ptrToBytes(contentPtr, contentSize), nil
}

// ptrToBytes returns a byte slice from WebAssembly compatible numeric types
// representing its pointer and length.
func ptrToBytes(ptr, size uint32) []byte {
	// Get a slice view of the underlying bytes in the stream. We use SliceHeader, not StringHeader
	// as it allows us to fix the capacity to what was allocated.
	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: uintptr(ptr),
		Len:  uintptr(size), // Tinygo requires these as uintptrs even if they are int fields.
		Cap:  uintptr(size), // ^^ See https://github.com/tinygo-org/tinygo/issues/1284
	}))
}

// bytesToPtr returns a pointer and size pair for the given byte slice in a way
// compatible with WebAssembly numeric types.
func bytesToPtr(buf []byte) (uint32, uint32) {
	ptr := &buf[0]
	unsafePtr := uintptr(unsafe.Pointer(ptr))
	return uint32(unsafePtr), uint32(len(buf))
}
