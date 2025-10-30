//go:build darwin || freebsd || linux || netbsd

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

package impl

import "github.com/ebitengine/purego"

// SharedLibrary an abstraction around a platform specific shared library
type SharedLibrary struct {
	handle uintptr
}

// OpenSharedLibrary opens a new sharedLibrary from a path to a file.
func OpenSharedLibrary(path string) (*SharedLibrary, error) {
	h, err := purego.Dlopen(path, purego.RTLD_GLOBAL|purego.RTLD_LAZY)
	if err != nil {
		return nil, err
	}
	return &SharedLibrary{h}, nil
}

// LookupSymbol returns the symbol or an error for the named symbol.
func (so *SharedLibrary) LookupSymbol(name string) (uintptr, error) {
	return purego.Dlsym(so.handle, name)
}

// Close releases the dynamically loaded library from this process.
func (so *SharedLibrary) Close() error {
	return purego.Dlclose(so.handle)
}

// registerFunc registers the given function at the address
func registerFunc(fnPtr any, addr uintptr) {
	purego.RegisterFunc(fnPtr, addr)
}
