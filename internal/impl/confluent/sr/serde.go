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

package sr

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// ExtractID extracts the schema ID from a raw message.
func ExtractID(msg []byte) (int, error) {
	if len(msg) < 5 {
		return 0, errors.New("message is empty or too small")
	}
	if msg[0] != 0 {
		return 0, fmt.Errorf("serialization format version number %v not supported", msg[0])
	}
	return int(binary.BigEndian.Uint32(msg[1:5])), nil
}

// UpdateID updates the schema ID in a raw message.
func UpdateID(msg []byte, id int) error {
	if len(msg) < 5 {
		return errors.New("message is empty or too small")
	}
	if msg[0] != 0 {
		return fmt.Errorf("serialization format version number %v not supported", msg[0])
	}

	binary.BigEndian.PutUint32(msg[1:5], uint32(id))

	return nil
}
