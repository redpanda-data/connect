//go:build windows

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

import (
	"github.com/ebitengine/purego"
	"golang.org/x/sys/windows"
)

type SharedLibrary struct {
	handle windows.Handle
}

func OpenSharedLibrary(path string) (*SharedLibrary, error) {
	h, err := windows.LoadLibrary(path)
	if err != nil {
		return nil, err
	}
	return &SharedLibrary{h}, nil
}

func (so *SharedLibrary) LookupSymbol(name string) (uintptr, error) {
	return windows.GetProcAddress(so.handle, name)
}

func (so *SharedLibrary) Close() error {
	return windows.FreeLibrary(so.handle)
}

func registerFunc(fnPtr any, addr uintptr) {
	purego.RegisterFunc(fnPtr, addr)
}
