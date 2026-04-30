// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/schema"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/iceberg/icebergx"
)

// typeResolver resolves Iceberg types for new columns using a three-stage pipeline:
//  1. Default Go runtime type inference (existing behavior)
//  2. schema_metadata override (schema.Common from message metadata)
//  3. Bloblang new_column_type_mapping override
type typeResolver struct {
	schemaMetadataKey    string
	newColumnTypeMapping *bloblang.Executor
	caseSensitive        bool
	logger               *service.Logger
}

func newTypeResolver(schemaMetadataKey string, newColumnTypeMapping *bloblang.Executor, caseSensitive bool, logger *service.Logger) *typeResolver {
	return &typeResolver{
		schemaMetadataKey:    schemaMetadataKey,
		newColumnTypeMapping: newColumnTypeMapping,
		caseSensitive:        caseSensitive,
		logger:               logger,
	}
}

// resolveTypeForAddColumn resolves the Iceberg type for a new column being added via schema evolution.
func (r *typeResolver) resolveTypeForAddColumn(
	field *UnknownFieldError,
	msg *service.Message,
	namespace, table string,
) (iceberg.Type, error) {
	// Stage 1: Default inference. Use the resolver's case sensitivity so that
	// new struct columns whose value carries case-only-duplicate keys are
	// rejected at the inference step rather than producing an iceberg struct
	// that would later collide with itself.
	inferredType, err := inferIcebergTypeForAddColumn(field.Value(), r.caseSensitive)
	if err != nil {
		return nil, fmt.Errorf("inferring type for field %q: %w", field.FieldName(), err)
	}

	// Stage 2: schema_metadata override
	if r.schemaMetadataKey != "" {
		if metaType, err := r.resolveFromSchemaMetadata(msg, field.FullPath(), newTypeInferrer(r.caseSensitive)); err != nil {
			return nil, fmt.Errorf("resolving type from schema metadata for field %v: %w", field.FullPath(), err)
		} else if metaType != nil {
			// If the metatype was not found then just stick with the default inferred type
			inferredType = metaType
		}
	}

	// Stage 3: Bloblang mapping override (only for primitive/leaf types)
	if r.newColumnTypeMapping != nil && isPrimitiveType(inferredType) {
		mappedType, err := r.applyTypeMapping(field, inferredType, msg, namespace, table)
		if err != nil {
			return nil, fmt.Errorf("applying type mapping for field %q: %w", field.FieldName(), err)
		}
		inferredType = mappedType
	}

	return inferredType, nil
}

// resolveTypeForCreateTable resolves the Iceberg type for a field during initial table creation.
// ti is a shared field ID allocator so that nested struct field IDs are unique across the entire
// schema, including IDs assigned by the schema_metadata override path. If stage 2 or 3 overrides
// replace a nested struct, IDs allocated during stage 1 inference become harmless gaps.
func (r *typeResolver) resolveTypeForCreateTable(
	fieldName string,
	value any,
	msg *service.Message,
	namespace, table string,
	ti *typeInferrer,
) (iceberg.Type, error) {
	// Stage 1: Default inference using shared allocator
	inferredType, err := ti.inferType(value)
	if err != nil {
		return nil, err
	}
	if inferredType == nil {
		return nil, nil // nil value, skip
	}

	// Stage 2: schema_metadata override (uses shared ti so override structs share the ID space)
	if r.schemaMetadataKey != "" {
		path := icebergx.Path{{Kind: icebergx.PathField, Name: fieldName}}
		if metaType, err := r.resolveFromSchemaMetadata(msg, path, ti); err != nil {
			return nil, fmt.Errorf("resolving type from schema metadata for field %q: %w", fieldName, err)
		} else if metaType != nil {
			// If the metatype was not found then just stick with the default inferred type
			inferredType = metaType
		}
	}

	// Stage 3: Bloblang mapping override (only for primitive/leaf types)
	if r.newColumnTypeMapping != nil && isPrimitiveType(inferredType) {
		field := NewUnknownFieldError(nil, fieldName, value)
		mappedType, err := r.applyTypeMapping(field, inferredType, msg, namespace, table)
		if err != nil {
			return nil, fmt.Errorf("applying type mapping for field %q: %w", field.FieldName(), err)
		}
		inferredType = mappedType
	}

	return inferredType, nil
}

// resolveFromSchemaMetadata looks up the type for a field path in the schema.Common
// structure found in message metadata. ti is the field ID allocator used when the
// resolved type is a struct or list; pass the schema's shared allocator to keep IDs
// unique across the whole schema, or a fresh one when the result is single-column.
func (r *typeResolver) resolveFromSchemaMetadata(msg *service.Message, fieldPath icebergx.Path, ti *typeInferrer) (iceberg.Type, error) {
	metaAny, exists := msg.MetaGetMut(r.schemaMetadataKey)
	if !exists {
		if r.logger != nil {
			r.logger.Warnf("Schema metadata key %q not found on message, falling back to type inference", r.schemaMetadataKey)
		}
		return nil, nil
	}

	commonSchema, err := schema.ParseFromAny(metaAny)
	if err != nil {
		return nil, fmt.Errorf("parsing schema metadata: %w", err)
	}

	field, found := findCommonField(commonSchema, fieldPath, r.caseSensitive)
	if !found {
		return nil, nil
	}

	return commonTypeToIcebergType(field, ti)
}

// findCommonField walks a schema.Common tree to find the field at the given
// path. When caseSensitive is false, name comparisons fold case so a metadata
// schema authored with iceberg-canonical (typically lowercase) names still
// matches paths derived from arbitrarily-cased input keys.
func findCommonField(root schema.Common, path icebergx.Path, caseSensitive bool) (*schema.Common, bool) {
	names := make([]string, 0, len(path))
	for _, seg := range path {
		if seg.Kind == icebergx.PathField {
			names = append(names, seg.Name)
		}
	}
	if len(names) == 0 {
		return nil, false
	}

	nameMatch := func(a, b string) bool {
		if caseSensitive {
			return a == b
		}
		return strings.EqualFold(a, b)
	}

	current := &root
	for _, name := range names {
		children := current.Children
		// For arrays with a single object child, descend into that child's fields
		if current.Type == schema.Array && len(children) == 1 && children[0].Type == schema.Object {
			children = children[0].Children
		} else if current.Type != schema.Object {
			return nil, false
		}

		found := false
		for i := range children {
			if nameMatch(children[i].Name, name) {
				current = &children[i]
				found = true
				break
			}
		}
		if !found {
			return nil, false
		}
	}
	return current, true
}

// commonTypeToIcebergType converts a schema.Common to an iceberg.Type using ti to
// allocate field IDs for any nested struct/list elements it produces.
func commonTypeToIcebergType(c *schema.Common, ti *typeInferrer) (iceberg.Type, error) {
	return commonTypeToIcebergTypeRec(c, ti)
}

func commonTypeToIcebergTypeRec(c *schema.Common, ti *typeInferrer) (iceberg.Type, error) {
	switch c.Type {
	case schema.Boolean:
		return iceberg.BooleanType{}, nil
	case schema.Int32:
		return iceberg.Int32Type{}, nil
	case schema.Int64:
		return iceberg.Int64Type{}, nil
	case schema.Float32:
		return iceberg.Float32Type{}, nil
	case schema.Float64:
		return iceberg.Float64Type{}, nil
	case schema.String:
		return iceberg.StringType{}, nil
	case schema.ByteArray:
		return iceberg.BinaryType{}, nil
	case schema.Timestamp:
		return iceberg.TimestampTzType{}, nil
	case schema.Decimal:
		if c.Logical == nil || c.Logical.Decimal == nil {
			return nil, fmt.Errorf("decimal field %q is missing precision/scale", c.Name)
		}
		return iceberg.DecimalTypeOf(int(c.Logical.Decimal.Precision), int(c.Logical.Decimal.Scale)), nil
	case schema.BigDecimal:
		return nil, fmt.Errorf("field %q is BigDecimal which has no fixed precision/scale; cast or coerce upstream before iceberg", c.Name)
	case schema.Object:
		return commonObjectToIcebergStruct(c, ti)
	case schema.Array:
		return commonArrayToIcebergList(c, ti)
	case schema.Any, schema.Null:
		return iceberg.StringType{}, nil
	default:
		return nil, fmt.Errorf("unsupported common schema type: %v", c.Type)
	}
}

func commonObjectToIcebergStruct(c *schema.Common, ti *typeInferrer) (*iceberg.StructType, error) {
	// Mirror the auto-inference path: when case-insensitive matching is in
	// effect, refuse a metadata-supplied struct that contains two children
	// differing only in case rather than producing an iceberg struct that
	// would collide with itself under iceberg's case-folded uniqueness.
	if !ti.caseSensitive {
		seen := make(map[string]string, len(c.Children))
		for _, child := range c.Children {
			lower := strings.ToLower(child.Name)
			if existing, ok := seen[lower]; ok {
				return nil, fmt.Errorf("ambiguous struct fields in schema metadata for %q: %q and %q differ only in case", c.Name, existing, child.Name)
			}
			seen[lower] = child.Name
		}
	}

	fields := make([]iceberg.NestedField, 0, len(c.Children))
	for _, child := range c.Children {
		childType, err := commonTypeToIcebergTypeRec(&child, ti)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", child.Name, err)
		}
		fields = append(fields, iceberg.NestedField{
			ID:       ti.allocateFieldID(),
			Name:     child.Name,
			Type:     childType,
			Required: !child.Optional,
		})
	}
	return &iceberg.StructType{FieldList: fields}, nil
}

func commonArrayToIcebergList(c *schema.Common, ti *typeInferrer) (*iceberg.ListType, error) {
	if len(c.Children) != 1 {
		return nil, fmt.Errorf("array type must have exactly one child, got %d", len(c.Children))
	}
	elemType, err := commonTypeToIcebergTypeRec(&c.Children[0], ti)
	if err != nil {
		return nil, fmt.Errorf("array element: %w", err)
	}
	return &iceberg.ListType{
		ElementID:       ti.allocateFieldID(),
		Element:         elemType,
		ElementRequired: false,
	}, nil
}

// applyTypeMapping runs the Bloblang new_column_type_mapping.
func (r *typeResolver) applyTypeMapping(
	field *UnknownFieldError,
	inferredType iceberg.Type,
	msg *service.Message,
	namespace, table string,
) (iceberg.Type, error) {
	path := field.FullPath()
	pathParts := make([]string, 0, len(path))
	for _, seg := range path {
		if seg.Kind == icebergx.PathField {
			pathParts = append(pathParts, seg.Name)
		}
	}
	pathStr := strings.Join(pathParts, ".")

	original, err := msg.AsStructured()
	if err != nil {
		return nil, err
	}

	input := map[string]any{
		"name":          field.FieldName(),
		"path":          pathStr,
		"value":         field.Value(),
		"inferred_type": inferredType.Type(),
		"message":       original,
		"namespace":     namespace,
		"table":         table,
	}

	tmpMsg := msg.Copy()
	tmpMsg.SetStructuredMut(input)

	resultMsg, err := tmpMsg.BloblangQuery(r.newColumnTypeMapping)
	if err != nil {
		return nil, fmt.Errorf("executing type mapping: %w", err)
	}
	if resultMsg == nil {
		return nil, errors.New("type mapping must not filter the message")
	}

	v, err := resultMsg.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("extracting type mapping result: %w", err)
	}

	return parseIcebergTypeString(string(v))
}

// isPrimitiveType returns true if the type is a primitive (not struct/list/map).
func isPrimitiveType(t iceberg.Type) bool {
	switch t.(type) {
	case *iceberg.StructType, *iceberg.ListType, *iceberg.MapType:
		return false
	default:
		return true
	}
}

var (
	decimalRegex = regexp.MustCompile(`(?i)^decimal\(\s*(\d+)\s*,\s*(\d+)\s*\)$`)
	fixedRegex   = regexp.MustCompile(`(?i)^fixed\[\s*(\d+)\s*\]$`)
)

// parseIcebergTypeString parses an Iceberg type string into an iceberg.Type.
// Supports: boolean, int, long, float, double, string, binary, date, time,
// timestamp, timestamptz, uuid, decimal(p,s), fixed[n].
func parseIcebergTypeString(s string) (iceberg.Type, error) {
	s = strings.TrimSpace(s)
	lower := strings.ToLower(s)

	switch lower {
	case "boolean":
		return iceberg.BooleanType{}, nil
	case "int":
		return iceberg.Int32Type{}, nil
	case "long":
		return iceberg.Int64Type{}, nil
	case "float":
		return iceberg.Float32Type{}, nil
	case "double":
		return iceberg.Float64Type{}, nil
	case "string":
		return iceberg.StringType{}, nil
	case "binary":
		return iceberg.BinaryType{}, nil
	case "date":
		return iceberg.DateType{}, nil
	case "time":
		return iceberg.TimeType{}, nil
	case "timestamp":
		return iceberg.TimestampType{}, nil
	case "timestamptz":
		return iceberg.TimestampTzType{}, nil
	case "uuid":
		return iceberg.UUIDType{}, nil
	}

	if m := decimalRegex.FindStringSubmatch(s); m != nil {
		p, _ := strconv.Atoi(m[1])
		sc, _ := strconv.Atoi(m[2])
		return iceberg.DecimalTypeOf(p, sc), nil
	}
	if m := fixedRegex.FindStringSubmatch(s); m != nil {
		n, _ := strconv.Atoi(m[1])
		return iceberg.FixedTypeOf(n), nil
	}

	return nil, fmt.Errorf("unrecognized iceberg type: %q", s)
}
