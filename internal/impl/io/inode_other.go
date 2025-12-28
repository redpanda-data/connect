// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !unix

package io

import "os"

// inodeOf extracts the inode from a FileInfo on non-Unix systems
// On Windows and other non-Unix systems, inodes are not available
// Returns (0, false) to indicate inode support is not available
// Rotation detection will fall back to size-based heuristics
func inodeOf(_ os.FileInfo) (uint64, bool) {
	return 0, false
}
