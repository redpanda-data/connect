// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package cdc

import (
	"context"
	"fmt"
	"math/big"
	"slices"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// ---------------------------------------------------------------------------
// Tier 1: $jsonSchema validator conversion
// ---------------------------------------------------------------------------

// fetchCollectionSchema queries the collection's $jsonSchema validator via
// listCollections and converts it to a serialised schema.Common. Returns
// (nil, nil, nil) when no validator is configured.
func fetchCollectionSchema(ctx context.Context, db *mongo.Database, collectionName string) (any, []string, error) {
	cursor, err := db.ListCollections(ctx, bson.M{"name": collectionName})
	if err != nil {
		return nil, nil, fmt.Errorf("listing collections: %w", err)
	}
	defer cursor.Close(ctx)

	if !cursor.Next(ctx) {
		return nil, nil, nil // collection not found
	}
	var info bson.M
	if err := cursor.Decode(&info); err != nil {
		return nil, nil, fmt.Errorf("decoding collection info: %w", err)
	}

	opts, _ := info["options"].(bson.M)
	if opts == nil {
		return nil, nil, nil
	}
	validator, _ := opts["validator"].(bson.M)
	if validator == nil {
		return nil, nil, nil
	}
	jsonSchema, _ := validator["$jsonSchema"].(bson.M)
	if jsonSchema == nil {
		return nil, nil, nil
	}

	s, keys, err := schemaFromJSONSchema(collectionName, jsonSchema)
	if err != nil {
		return nil, nil, fmt.Errorf("converting $jsonSchema: %w", err)
	}
	return s, keys, nil
}

// schemaFromJSONSchema converts a MongoDB $jsonSchema validator to a serialised
// schema.Common. Returns (nil, nil, nil) if the validator cannot be converted
// (e.g. only uses combinators with no properties).
func schemaFromJSONSchema(collectionName string, jsonSchema bson.M) (any, []string, error) {
	props, _ := jsonSchema["properties"].(bson.M)
	if props == nil {
		// Top-level validator with no properties (e.g. pure oneOf/anyOf) —
		// fall back to Tier 2.
		return nil, nil, nil
	}

	requiredSet := map[string]bool{}
	if reqArr, ok := jsonSchema["required"].(bson.A); ok {
		for _, r := range reqArr {
			if s, ok := r.(string); ok {
				requiredSet[s] = true
			}
		}
	}

	children, keys := jsonSchemaPropsToChildren(props, requiredSet)

	// $jsonSchema validators almost never declare _id, but every document has
	// it. Without _id the key-set fingerprint will always mismatch on the
	// first real document and the Tier 1 schema will be discarded immediately.
	// Inject _id as an optional String field when it is not already present.
	if !slices.Contains(keys, "_id") {
		children = slices.Insert(children, 0, schema.Common{Name: "_id", Type: schema.String, Optional: true})
		keys = slices.Insert(keys, 0, "_id")
	}

	c := schema.Common{
		Name:     collectionName,
		Type:     schema.Object,
		Optional: false,
		Children: children,
	}
	return c.ToAny(), keys, nil
}

// jsonSchemaPropsToChildren converts a $jsonSchema properties map to sorted
// schema.Common children and returns the sorted key list.
func jsonSchemaPropsToChildren(props bson.M, requiredSet map[string]bool) ([]schema.Common, []string) {
	keys := sortedMapKeys(props)
	children := make([]schema.Common, 0, len(keys))
	for _, name := range keys {
		fieldSchema, ok := props[name].(bson.M)
		if !ok {
			children = append(children, schema.Common{
				Name:     name,
				Type:     schema.Any,
				Optional: !requiredSet[name],
			})
			continue
		}
		children = append(children, jsonSchemaFieldToCommon(name, fieldSchema, requiredSet[name]))
	}
	return children, keys
}

// jsonSchemaFieldToCommon converts a single $jsonSchema field definition to a
// schema.Common.
func jsonSchemaFieldToCommon(name string, fieldSchema bson.M, required bool) schema.Common {
	// Check for combinators that we can't convert — map to Any.
	for _, combinator := range []string{"oneOf", "anyOf", "allOf", "not"} {
		if _, hasCombinator := fieldSchema[combinator]; hasCombinator {
			return schema.Common{Name: name, Type: schema.Any, Optional: !required}
		}
	}

	bsonType, optional := resolveBsonType(fieldSchema)
	ct := bsonTypeStringToCommon(bsonType)

	c := schema.Common{
		Name:     name,
		Type:     ct,
		Optional: !required || optional,
	}

	if ct == schema.Object {
		if nestedProps, ok := fieldSchema["properties"].(bson.M); ok {
			nestedRequired := map[string]bool{}
			if reqArr, ok := fieldSchema["required"].(bson.A); ok {
				for _, r := range reqArr {
					if s, ok := r.(string); ok {
						nestedRequired[s] = true
					}
				}
			}
			c.Children, _ = jsonSchemaPropsToChildren(nestedProps, nestedRequired)
		}
	}

	if ct == schema.Array {
		if items, ok := fieldSchema["items"].(bson.M); ok {
			itemType, _ := resolveBsonType(items)
			c.Children = []schema.Common{
				{Name: "element", Type: bsonTypeStringToCommon(itemType), Optional: true},
			}
		}
	}

	return c
}

// resolveBsonType extracts the effective bsonType string from a field schema.
// It handles bsonType as a string or an array (union type). Returns the
// resolved type string and whether "null" was present in a union.
func resolveBsonType(fieldSchema bson.M) (string, bool) {
	raw := fieldSchema["bsonType"]
	switch v := raw.(type) {
	case string:
		return v, false
	case bson.A:
		var nonNull []string
		hasNull := false
		for _, elem := range v {
			s, ok := elem.(string)
			if !ok {
				continue
			}
			if s == "null" {
				hasNull = true
			} else {
				nonNull = append(nonNull, s)
			}
		}
		if len(nonNull) == 1 {
			return nonNull[0], hasNull
		}
		// Multiple non-null types or empty — fall back to Any.
		return "", hasNull
	default:
		return "", false
	}
}

// bsonTypeStringToCommon maps a $jsonSchema bsonType string to a
// schema.CommonType.
//
// MongoDB Decimal128 has per-value precision/scale, not a column-level
// declaration, so it maps to BigDecimal — an arbitrary-precision decimal
// transported as a canonical decimal string. Downstream converters that
// need a fixed (p, s) (Avro/Parquet/Iceberg) reject BigDecimal explicitly;
// JSON Schema accepts it as a permissive string pattern.
func bsonTypeStringToCommon(bsonType string) schema.CommonType {
	switch bsonType {
	case "bool":
		return schema.Boolean
	case "int":
		return schema.Int32
	case "long":
		return schema.Int64
	case "double":
		return schema.Float64
	case "string":
		return schema.String
	case "binData":
		return schema.ByteArray
	case "date":
		return schema.Timestamp
	case "timestamp":
		return schema.Timestamp
	case "objectId":
		return schema.String
	case "decimal":
		return schema.BigDecimal
	case "object":
		return schema.Object
	case "array":
		return schema.Array
	default:
		return schema.Any
	}
}

// ---------------------------------------------------------------------------
// Tier 2: Document inference
// ---------------------------------------------------------------------------

// inferSchemaFromDocument infers a schema.Common from a bson.M document and
// returns the serialised form (via ToAny()) along with sorted top-level keys.
func inferSchemaFromDocument(collectionName string, doc bson.M) (any, []string) {
	keys := sortedMapKeys(doc)
	children := make([]schema.Common, 0, len(keys))
	for _, k := range keys {
		children = append(children, inferField(k, doc[k]))
	}
	c := schema.Common{
		Name:     collectionName,
		Type:     schema.Object,
		Optional: false,
		Children: children,
	}
	return c.ToAny(), keys
}

// inferField maps a single Go value (from BSON decoding) to a schema.Common.
func inferField(name string, val any) schema.Common {
	c := schema.Common{
		Name:     name,
		Type:     inferType(val),
		Optional: true,
	}

	switch v := val.(type) {
	case bson.M:
		keys := sortedMapKeys(v)
		children := make([]schema.Common, 0, len(keys))
		for _, k := range keys {
			children = append(children, inferField(k, v[k]))
		}
		c.Children = children
	case bson.D:
		m := make(bson.M, len(v))
		for _, elem := range v {
			m[elem.Key] = elem.Value
		}
		keys := sortedMapKeys(m)
		children := make([]schema.Common, 0, len(keys))
		for _, k := range keys {
			children = append(children, inferField(k, m[k]))
		}
		c.Children = children
	case bson.A:
		if len(v) > 0 {
			elemType := inferType(v[0])
			// If mixed types, fall back to Any.
			for _, elem := range v[1:] {
				if inferType(elem) != elemType {
					elemType = schema.Any
					break
				}
			}
			c.Children = []schema.Common{
				{Name: "element", Type: elemType, Optional: true},
			}
		}
	}

	return c
}

// inferType maps a Go value (from BSON decoding with DefaultDocumentM=true) to
// a schema.CommonType.
func inferType(val any) schema.CommonType {
	switch val.(type) {
	case bool:
		return schema.Boolean
	case int32:
		return schema.Int32
	case int64:
		return schema.Int64
	case float64:
		return schema.Float64
	case string:
		return schema.String
	case bson.Binary:
		return schema.ByteArray
	case []byte:
		return schema.ByteArray
	case bson.DateTime:
		return schema.Timestamp
	case time.Time:
		return schema.Timestamp
	case bson.Timestamp:
		return schema.Timestamp
	case bson.ObjectID:
		return schema.String
	case bson.Decimal128:
		return schema.BigDecimal
	case bson.M:
		return schema.Object
	case bson.D:
		return schema.Object
	case bson.A:
		return schema.Array
	case nil:
		return schema.Any
	default:
		return schema.Any
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// sortedMapKeys returns the keys of a bson.M sorted alphabetically.
func sortedMapKeys(m bson.M) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// normaliseDecimal128 walks a BSON document and replaces every
// bson.Decimal128 value with its canonical decimal string, so that the
// final JSON-marshalled body matches the schema.BigDecimal value contract
// (a plain JSON string, not the {"$numberDecimal": "..."} ExtJSON wrapper).
//
// The walk mutates bson.M and bson.A values in place. bson.D is rebuilt
// element-wise because its values aren't directly addressable through the
// keyed-pair shape.
func normaliseDecimal128(value any) any {
	switch v := value.(type) {
	case bson.Decimal128:
		canonical, err := canonicaliseDecimal128(v)
		if err != nil {
			return v
		}
		return canonical
	case bson.M:
		for k, child := range v {
			v[k] = normaliseDecimal128(child)
		}
		return v
	case bson.D:
		for i, elem := range v {
			v[i].Value = normaliseDecimal128(elem.Value)
		}
		return v
	case bson.A:
		for i, child := range v {
			v[i] = normaliseDecimal128(child)
		}
		return v
	default:
		return value
	}
}

// canonicaliseDecimal128 converts a bson.Decimal128 to a canonical decimal
// string. Uses BigInt() to get the unscaled significand and exponent
// directly, sidestepping the driver's String() output which can emit
// scientific notation for extreme magnitudes.
//
// Decimal128 represents value = significand × 10^exponent. For
// non-negative exponents the value is an integer (the unscaled int times
// 10^exponent at scale 0). For negative exponents the magnitude of the
// exponent is the scale.
func canonicaliseDecimal128(d bson.Decimal128) (string, error) {
	unscaled, exp, err := d.BigInt()
	if err != nil {
		return "", err
	}
	if exp >= 0 {
		shifted := new(big.Int).Mul(unscaled, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(exp)), nil))
		return schema.FormatBigDecimal(shifted, 0)
	}
	return schema.FormatBigDecimal(unscaled, int32(-exp))
}
