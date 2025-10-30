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

package common

import (
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	//nolint:staticcheck // Ignore SA1019 "github.com/jhump/protoreflect/desc" is deprecated warning
	"github.com/jhump/protoreflect/desc"
	//nolint:staticcheck // Ignore SA1019 "github.com/jhump/protoreflect/desc/protoparse" is deprecated warning
	"github.com/jhump/protoreflect/desc/protoparse"
)

// RegistriesFromMap attempts to parse a map of filenames (relative to import
// directories) and their contents out into a registry of protobuf files and
// protobuf types. These registries can then be used as a mechanism for
// dynamically (un)marshalling the definitions within.
func RegistriesFromMap(filesMap map[string]string) (*protoregistry.Files, *protoregistry.Types, error) {
	fds, err := ParseProtos(filesMap)
	if err != nil {
		return nil, nil, err
	}
	return BuildRegistries(fds)
}

// ParseFromFS loads a bunch of `.proto` files found in importPaths using the specified filesystem.
func ParseFromFS(fsys fs.FS, importPaths []string) (*descriptorpb.FileDescriptorSet, error) {
	files := map[string]string{}
	for _, importPath := range importPaths {
		if err := fs.WalkDir(fsys, importPath, func(path string, info fs.DirEntry, ferr error) error {
			if ferr != nil || info.IsDir() {
				return ferr
			}
			if filepath.Ext(info.Name()) == ".proto" && !strings.HasPrefix(info.Name(), ".") {
				rPath, ferr := filepath.Rel(importPath, path)
				if ferr != nil {
					return fmt.Errorf("failed to get relative path: %v", ferr)
				}
				content, ferr := fs.ReadFile(fsys, path)
				if ferr != nil {
					return fmt.Errorf("failed to read import %v: %v", path, ferr)
				}
				files[rPath] = string(content)
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return ParseProtos(files)
}

// ParseProtos dynamically parses protobuf files from a map of import path to proto file contents,
// and loads them as a FileDescriptorSet, which can be used to dynamically (un)marshal protos.
func ParseProtos(filesMap map[string]string) (*descriptorpb.FileDescriptorSet, error) {
	var parser protoparse.Parser
	parser.Accessor = protoparse.FileContentsFromMap(filesMap)

	names := make([]string, 0, len(filesMap))
	for k := range filesMap {
		names = append(names, k)
	}

	fds, err := parser.ParseFiles(names...)
	if err != nil {
		return nil, err
	}
	var files []*descriptorpb.FileDescriptorProto
	seen := map[string]bool{}
	var toProto func([]*desc.FileDescriptor)
	toProto = func(fds []*desc.FileDescriptor) {
		for _, fd := range fds {
			if seen[fd.GetFullyQualifiedName()] {
				continue
			}
			files = append(files, fd.AsFileDescriptorProto())
			seen[fd.GetFullyQualifiedName()] = true
			toProto(fd.GetDependencies())
		}
	}
	toProto(fds)
	return &descriptorpb.FileDescriptorSet{File: files}, nil
}

// BuildRegistries converts a FileDescriptorSet into a registry that is able to
// resolve types and lookup protos by name.
func BuildRegistries(descriptors *descriptorpb.FileDescriptorSet) (*protoregistry.Files, *protoregistry.Types, error) {
	files, err := protodesc.NewFiles(descriptors)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to register proto files: %w", err)
	}
	types := &protoregistry.Types{}
	var register func(mds protoreflect.MessageDescriptors) error
	register = func(mds protoreflect.MessageDescriptors) error {
		for i := range mds.Len() {
			msg := mds.Get(i)
			if err := types.RegisterMessage(dynamicpb.NewMessageType(msg)); err != nil {
				return fmt.Errorf("failed to register type %q: %w", msg.FullName(), err)
			}
			if err := register(msg.Messages()); err != nil {
				return err
			}
		}
		return nil
	}
	for file := range files.RangeFiles {
		if err := register(file.Messages()); err != nil {
			return nil, nil, err
		}
	}
	return files, types, nil
}
