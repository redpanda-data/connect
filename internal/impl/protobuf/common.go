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

package protobuf

import (
	"fmt"

	//nolint:staticcheck // Ignore SA1019 "github.com/jhump/protoreflect/desc/protoparse" is deprecated warning
	"github.com/jhump/protoreflect/desc/protoparse"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// RegistriesFromMap attempts to parse a map of filenames (relative to import
// directories) and their contents out into a registry of protobuf files and
// protobuf types. These registries can then be used as a mechanism for
// dynamically (un)marshalling the definitions within.
func RegistriesFromMap(filesMap map[string]string) (*protoregistry.Files, *protoregistry.Types, error) {
	var parser protoparse.Parser
	parser.Accessor = protoparse.FileContentsFromMap(filesMap)

	names := make([]string, 0, len(filesMap))
	for k := range filesMap {
		names = append(names, k)
	}

	fds, err := parser.ParseFiles(names...)
	if err != nil {
		return nil, nil, err
	}

	files, types := &protoregistry.Files{}, &protoregistry.Types{}
	for _, v := range fds {
		if err := files.RegisterFile(v.UnwrapFile()); err != nil {
			return nil, nil, fmt.Errorf("failed to register file '%v': %w", v.GetName(), err)
		}
		for _, t := range v.GetMessageTypes() {
			if err := types.RegisterMessage(dynamicpb.NewMessageType(t.UnwrapMessage())); err != nil {
				return nil, nil, fmt.Errorf("failed to register type '%v': %w", t.GetName(), err)
			}
			for _, nt := range t.GetNestedMessageTypes() {
				if err := types.RegisterMessage(dynamicpb.NewMessageType(nt.UnwrapMessage())); err != nil {
					return nil, nil, fmt.Errorf("failed to register type '%v': %w", nt.GetFullyQualifiedName(), err)
				}
			}
		}
	}
	return files, types, nil
}
