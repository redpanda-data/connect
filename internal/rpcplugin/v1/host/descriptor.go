// Copyright 2026 Redpanda Data, Inc.
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

package host

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"

	runtimepb "github.com/redpanda-data/connect/v4/internal/rpcplugin/v1/runtimepb"
)

// protoToServiceSpec walks the wire-form ConfigSpec returned from a
// plugin's ListComponents call and builds a real *service.ConfigSpec
// using the public benthos builder. This is the host-side mirror of
// the SDK's own descriptorToServiceSpec — having two copies avoids a
// cross-package coupling between public/plugin/go/rpcn/v1 and
// internal/rpcplugin/v1/host while the descriptor surface is still
// being shaken out.
func protoToServiceSpec(pb *runtimepb.ConfigSpec) (*service.ConfigSpec, error) {
	spec := service.NewConfigSpec()
	if pb == nil {
		return spec, nil
	}
	if pb.GetSummary() != "" {
		spec = spec.Summary(pb.GetSummary())
	}
	if pb.GetDescription() != "" {
		spec = spec.Description(pb.GetDescription())
	}
	if pb.GetVersion() != "" {
		spec = spec.Version(pb.GetVersion())
	}
	if len(pb.GetCategories()) > 0 {
		spec = spec.Categories(pb.GetCategories()...)
	}
	for _, f := range pb.GetFields() {
		sf, err := protoToServiceField(f)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", f.GetName(), err)
		}
		spec = spec.Field(sf)
	}
	return spec, nil
}

func protoToServiceField(pb *runtimepb.ConfigField) (*service.ConfigField, error) {
	if pb == nil {
		return nil, errors.New("nil field descriptor")
	}

	var sf *service.ConfigField
	switch pb.GetKind() {
	case runtimepb.FieldKind_FIELD_KIND_SCALAR, runtimepb.FieldKind_FIELD_KIND_UNSPECIFIED:
		switch pb.GetType() {
		case runtimepb.FieldType_FIELD_TYPE_STRING:
			sf = service.NewStringField(pb.GetName())
		case runtimepb.FieldType_FIELD_TYPE_INT:
			sf = service.NewIntField(pb.GetName())
		case runtimepb.FieldType_FIELD_TYPE_FLOAT:
			sf = service.NewFloatField(pb.GetName())
		case runtimepb.FieldType_FIELD_TYPE_BOOL:
			sf = service.NewBoolField(pb.GetName())
		case runtimepb.FieldType_FIELD_TYPE_OBJECT:
			children := make([]*service.ConfigField, 0, len(pb.GetChildren()))
			for _, c := range pb.GetChildren() {
				cs, err := protoToServiceField(c)
				if err != nil {
					return nil, err
				}
				children = append(children, cs)
			}
			sf = service.NewObjectField(pb.GetName(), children...)
		default:
			return nil, fmt.Errorf("unsupported field type %v", pb.GetType())
		}
	default:
		return nil, fmt.Errorf("unsupported field kind %v (host adapter supports scalar + object only)", pb.GetKind())
	}

	if pb.GetDescription() != "" {
		sf = sf.Description(pb.GetDescription())
	}
	if pb.GetOptional() {
		sf = sf.Optional()
	}
	if pb.GetAdvanced() {
		sf = sf.Advanced()
	}
	if pb.GetDefaultJson() != "" {
		var v any
		if err := json.Unmarshal([]byte(pb.GetDefaultJson()), &v); err != nil {
			return nil, fmt.Errorf("decoding default for field %q: %w", pb.GetName(), err)
		}
		sf = sf.Default(v)
	}
	return sf, nil
}
