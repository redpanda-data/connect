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

// This file contains code originally licensed under the MIT License:

// Copyright (c) 2024-present Bento contributors

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package protobuf

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"buf.build/gen/go/bufbuild/reflect/connectrpc/go/buf/reflect/v1beta1/reflectv1beta1connect"
	connectrpc "connectrpc.com/connect"
	"github.com/bufbuild/prototransform"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const watcherTimeout = 10 * time.Second

type multiModuleWatcher struct {
	bsrClients map[string]*prototransform.SchemaWatcher
}

var _ prototransform.Resolver = &multiModuleWatcher{}

func newMultiModuleWatcher(bsrModules []*service.ParsedConfig) (*multiModuleWatcher, error) {
	if len(bsrModules) == 0 {
		return nil, errors.New("no modules provided")
	}
	multiModuleWatcher := &multiModuleWatcher{}

	// Initialise one client for each module
	multiModuleWatcher.bsrClients = make(map[string]*prototransform.SchemaWatcher)
	for _, bsrModule := range bsrModules {
		var bsrURL string
		bsrURL, err := bsrModule.FieldString(fieldBSRUrl)
		if err != nil {
			return nil, err
		}

		var bsrAPIKey string
		if bsrAPIKey, err = bsrModule.FieldString(fieldBSRAPIKey); err != nil {
			return nil, err
		}

		var module string
		if module, err = bsrModule.FieldString(fieldBSRModule); err != nil {
			return nil, err
		}

		var version string
		if version, err = bsrModule.FieldString(fieldBSRVersion); err != nil {
			return nil, err
		}

		watcher, err := newSchemaWatcher(context.Background(), bsrURL, bsrAPIKey, module, version)
		if err != nil {
			return nil, err
		}
		multiModuleWatcher.bsrClients[module] = watcher
	}

	return multiModuleWatcher, nil
}

func newSchemaWatcher(ctx context.Context, bsrURL string, bsrAPIKey string, module string, version string) (*prototransform.SchemaWatcher, error) {
	// If no BSR URL provided, extract from module
	if bsrURL == "" {
		segments := strings.Split(module, "/")
		if len(segments) != 3 {
			return nil, fmt.Errorf("could not parse module %s, expected three segments e.g. 'buf.build/exampleco/mymodule'", module)
		}
		bsrURL = "https://" + segments[0]
	}

	opts := []connectrpc.ClientOption{
		connectrpc.WithHTTPGet(),
		connectrpc.WithHTTPGetMaxURLSize(8192, true)}

	if bsrAPIKey != "" {
		opts = append(opts, connectrpc.WithInterceptors(prototransform.NewAuthInterceptor(bsrAPIKey)))
	}
	client := reflectv1beta1connect.NewFileDescriptorSetServiceClient(http.DefaultClient, bsrURL, opts...)

	cfg := &prototransform.SchemaWatcherConfig{
		SchemaPoller: prototransform.NewSchemaPoller(client, module, version),
		Jitter:       0.2,
	}
	watcher, err := prototransform.NewSchemaWatcher(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create schema watcher: %w", err)
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, watcherTimeout)
	defer cancel()
	if err = watcher.AwaitReady(ctxWithTimeout); err != nil {
		return nil, fmt.Errorf("schema watcher never became ready: %w", err)
	}

	return watcher, nil
}

func (w *multiModuleWatcher) FindExtensionByName(field protoreflect.FullName) (protoreflect.ExtensionType, error) {
	for _, schemaWatcher := range w.bsrClients {
		extensionType, err := schemaWatcher.FindExtensionByName(field)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return extensionType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", field)
}

func (w *multiModuleWatcher) FindExtensionByNumber(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	for _, schemaWatcher := range w.bsrClients {
		extensionType, err := schemaWatcher.FindExtensionByNumber(message, field)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return extensionType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", message)
}

func (w *multiModuleWatcher) FindMessageByName(message protoreflect.FullName) (protoreflect.MessageType, error) {
	for _, schemaWatcher := range w.bsrClients {
		messageType, err := schemaWatcher.FindMessageByName(message)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return messageType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", message)
}

func (w *multiModuleWatcher) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	for _, schemaWatcher := range w.bsrClients {
		messageType, err := schemaWatcher.FindMessageByURL(url)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return messageType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", url)
}

func (w *multiModuleWatcher) FindEnumByName(enum protoreflect.FullName) (protoreflect.EnumType, error) {
	for _, schemaWatcher := range w.bsrClients {
		enumType, err := schemaWatcher.FindEnumByName(enum)
		if err != nil {
			if errors.Is(err, protoregistry.NotFound) {
				continue
			}
			return nil, err
		}
		return enumType, nil
	}
	return nil, fmt.Errorf("could not find %s in any loaded modules", enum)
}
