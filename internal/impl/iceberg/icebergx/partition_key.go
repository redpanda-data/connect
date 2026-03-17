/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package icebergx

import (
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"
	"unicode"

	"github.com/apache/iceberg-go"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
)

const (
	// maxKeyValueLength is the maximum length of a single partition key value.
	// AWS S3 path size limit is 1024 bytes, we allow a single key to be up to 64 bytes.
	maxKeyValueLength = 64
	// maxPathLength is the maximum total length of the partition path.
	maxPathLength = 512
)

// PartitionKey holds the partition values as iceberg Literals.
type PartitionKey []iceberg.Optional[iceberg.Literal]

// Compare compares two partition keys lexicographically.
// Returns -1 if pk < other, 0 if pk == other, 1 if pk > other.
func (pk PartitionKey) Compare(other PartitionKey) int {
	minLen := min(len(other), len(pk))

	for i := range minLen {
		cmp := compareOptionalLiteral(pk[i], other[i])
		if cmp != 0 {
			return cmp
		}
	}

	// If all compared elements are equal, shorter slice is less
	if len(pk) < len(other) {
		return -1
	} else if len(pk) > len(other) {
		return 1
	}
	return 0
}

// NewPartitionKey creates a PartitionKey from parquet values based on the partition spec and schema.
// The parquet values should be raw (untransformed) values matching the source field types.
// Transforms are applied automatically.
func NewPartitionKey(spec iceberg.PartitionSpec, schema *iceberg.Schema, values []parquet.Value) (PartitionKey, error) {
	if spec.NumFields() != len(values) {
		return nil, fmt.Errorf("partition key/spec mismatch: key has %d fields, but spec has %d fields",
			len(values), spec.NumFields())
	}

	if spec.NumFields() == 0 {
		return PartitionKey{}, nil
	}

	key := make(PartitionKey, spec.NumFields())
	for i := 0; i < spec.NumFields(); i++ {
		value := values[i]
		field := spec.Field(i)

		if value.IsNull() {
			key[i] = field.Transform.Apply(iceberg.Optional[iceberg.Literal]{Valid: false})
			continue
		}

		// Get the source field type from the schema
		sourceField, ok := schema.FindFieldByID(field.SourceID)
		if !ok {
			return nil, fmt.Errorf("source field %d not found in schema for partition field %q", field.SourceID, field.Name)
		}

		lit, err := parquetValueToLiteral(sourceField.Type, value)
		if err != nil {
			return nil, fmt.Errorf("converting partition value for field %q: %w", field.Name, err)
		}

		key[i] = field.Transform.Apply(iceberg.Optional[iceberg.Literal]{Val: lit, Valid: true})
	}

	return key, nil
}

// parquetValueToLiteral converts a parquet value to an iceberg Literal based on the result type.
func parquetValueToLiteral(resultType iceberg.Type, value parquet.Value) (iceberg.Literal, error) {
	switch resultType.(type) {
	case iceberg.BooleanType:
		return iceberg.BoolLiteral(value.Boolean()), nil

	case iceberg.Int32Type:
		return iceberg.Int32Literal(value.Int32()), nil

	case iceberg.Int64Type:
		return iceberg.Int64Literal(value.Int64()), nil

	case iceberg.Float32Type:
		return iceberg.Float32Literal(value.Float()), nil

	case iceberg.Float64Type:
		return iceberg.Float64Literal(value.Double()), nil

	case iceberg.DateType:
		return iceberg.DateLiteral(iceberg.Date(value.Int32())), nil

	case iceberg.TimeType:
		return iceberg.TimeLiteral(iceberg.Time(value.Int64())), nil

	case iceberg.TimestampType, iceberg.TimestampTzType:
		return iceberg.TimestampLiteral(iceberg.Timestamp(value.Int64())), nil

	case iceberg.StringType:
		b := value.ByteArray()
		return iceberg.StringLiteral(string(b)), nil

	case iceberg.UUIDType:
		b := value.ByteArray()
		u, err := uuid.FromBytes(b)
		if err != nil {
			return nil, fmt.Errorf("invalid UUID bytes: %w", err)
		}
		return iceberg.UUIDLiteral(u), nil

	case iceberg.BinaryType:
		return iceberg.BinaryLiteral(value.ByteArray()), nil

	case iceberg.FixedType:
		return iceberg.FixedLiteral(value.ByteArray()), nil

	case iceberg.DecimalType:
		// Decimal can be stored as int32, int64, or fixed depending on precision
		switch value.Kind() {
		case parquet.Int32:
			return iceberg.Int32Literal(value.Int32()), nil
		case parquet.Int64:
			return iceberg.Int64Literal(value.Int64()), nil
		default:
			return iceberg.FixedLiteral(value.ByteArray()), nil
		}

	default:
		return nil, fmt.Errorf("unsupported iceberg type: %v", resultType)
	}
}

// PartitionKeyToPath converts a partition key into a path in remote storage.
//
// The path is constructed by concatenating partition fields in the form: <field_name>=<field_value>
// with subsequent fields separated by '/'.
//
// Returned path elements are URL-encoded. If the total path exceeds maxPathLength, it is truncated.
//
// See: https://github.com/redpanda-data/redpanda/blob/dev/src/v/datalake/partition_key_path.h
func PartitionKeyToPath(spec iceberg.PartitionSpec, key PartitionKey) (string, error) {
	if spec.NumFields() != len(key) {
		return "", fmt.Errorf("partition key/spec mismatch: key has %d fields, but spec has %d fields",
			len(key), spec.NumFields())
	}

	if spec.NumFields() == 0 {
		return "", nil
	}

	segments := make([]string, 0, spec.NumFields())
	totalLength := 0

	for i := 0; i < spec.NumFields(); i++ {
		field := spec.Field(i)
		opt := key[i]

		var valueStr string
		if !opt.Valid {
			valueStr = "null"
		} else {
			valueStr = formatLiteralValue(field.Transform, opt.Val)
		}

		segment := fmt.Sprintf("%s=%s", url.PathEscape(field.Name), url.PathEscape(valueStr))

		// Check if adding this segment would exceed max path length.
		// Account for the '/' separator (except for the first segment).
		segmentLen := len(segment)
		if len(segments) > 0 {
			segmentLen++ // for the '/' separator
		}

		if totalLength+segmentLen > maxPathLength {
			// Path would exceed max length, truncate here.
			break
		}

		totalLength += segmentLen
		segments = append(segments, segment)
	}

	return path.Join(segments...), nil
}

// formatLiteralValue formats an iceberg Literal using the transform's ToHumanStr method.
// It handles truncation for string/binary values.
func formatLiteralValue(transform iceberg.Transform, lit iceberg.Literal) string {
	val := lit.Any()

	// Handle truncation for string/binary values before formatting
	switch v := val.(type) {
	case string:
		if len(v) > maxKeyValueLength {
			val = v[:maxKeyValueLength]
		}
	case []byte:
		if len(v) > maxKeyValueLength {
			val = v[:maxKeyValueLength]
		}
	}

	return transform.ToHumanStr(val)
}

// ParsePartitionSpec parses a Spark-like DDL expression string into an iceberg PartitionSpec.
//
// Supported syntax:
//   - Optional parentheses: "(field1, field2)" or "field1, field2"
//   - Identity transform: "col" or "identity(col)"
//   - Time transforms: "year(col)", "month(col)", "day(col)", "hour(col)"
//   - Other transforms: "void(col)", "bucket(n, col)", "truncate(width, col)"
//   - Optional alias: "transform(col) as name"
//   - Backtick-quoted identifiers: "`special col`"
//   - Nested column names: "foo.bar.baz"
//
// See: https://github.com/redpanda-data/redpanda/blob/dev/src/v/datalake/partition_spec_parser.cc
func ParsePartitionSpec(input string, schema *iceberg.Schema) (iceberg.PartitionSpec, error) {
	p := &partitionSpecParser{
		input:  input,
		pos:    0,
		schema: schema,
	}
	return p.parse()
}

// partitionSpecParser implements a recursive descent parser for partition specs.
type partitionSpecParser struct {
	input  string
	pos    int
	schema *iceberg.Schema
}

// parse is the main entry point.
func (p *partitionSpecParser) parse() (iceberg.PartitionSpec, error) {
	p.skipWhitespace()

	// Handle empty input
	if p.pos >= len(p.input) {
		return iceberg.NewPartitionSpec(), nil
	}

	// Check for optional opening parenthesis
	hasParens := p.peek() == '('
	if hasParens {
		p.advance()
		p.skipWhitespace()
	}

	// Handle empty spec: "()" or "( )"
	if hasParens && p.peek() == ')' {
		p.advance()
		p.skipWhitespace()
		if p.pos < len(p.input) {
			return iceberg.PartitionSpec{}, p.errorf("unexpected characters after ')'")
		}
		return iceberg.NewPartitionSpec(), nil
	}

	// Handle empty input after whitespace
	if p.pos >= len(p.input) {
		return iceberg.NewPartitionSpec(), nil
	}

	// Parse fields
	fields, err := p.parseFields()
	if err != nil {
		return iceberg.PartitionSpec{}, err
	}

	// Check for closing parenthesis if we had an opening one
	if hasParens {
		p.skipWhitespace()
		if p.pos >= len(p.input) || p.peek() != ')' {
			return iceberg.PartitionSpec{}, p.errorf("expected ')'")
		}
		p.advance()
	}

	p.skipWhitespace()
	if p.pos < len(p.input) {
		return iceberg.PartitionSpec{}, p.errorf("unexpected characters after partition spec")
	}

	return iceberg.NewPartitionSpec(fields...), nil
}

// parseFields parses a comma-separated list of partition fields.
func (p *partitionSpecParser) parseFields() ([]iceberg.PartitionField, error) {
	var fields []iceberg.PartitionField
	fieldID := 1000 // Starting field ID for partition fields

	for {
		p.skipWhitespace()
		if p.pos >= len(p.input) || p.peek() == ')' {
			break
		}

		field, err := p.parseField(fieldID)
		if err != nil {
			return nil, err
		}
		fields = append(fields, field)
		fieldID++

		p.skipWhitespace()
		if p.peek() == ',' {
			p.advance()
			continue
		}
		break
	}

	return fields, nil
}

// parseField parses a single partition field: transform(col) as alias, or just col.
func (p *partitionSpecParser) parseField(fieldID int) (iceberg.PartitionField, error) {
	p.skipWhitespace()

	// Try to parse as a transform expression
	transform, colRef, err := p.parseTransformExpr()
	if err != nil {
		return iceberg.PartitionField{}, err
	}

	// Parse optional alias
	p.skipWhitespace()
	var alias string
	if p.matchKeyword("as") {
		p.skipWhitespace()
		alias, err = p.parseIdentifier()
		if err != nil {
			return iceberg.PartitionField{}, p.errorf("expected identifier after 'as'")
		}
	}

	// Resolve column reference to field ID
	sourceID, err := p.resolveColumnRef(colRef)
	if err != nil {
		return iceberg.PartitionField{}, err
	}

	// Generate name if no alias - just use the column name
	name := alias
	if name == "" {
		name = generatePartitionFieldName(colRef)
	}

	return iceberg.PartitionField{
		SourceID:  sourceID,
		FieldID:   fieldID,
		Name:      name,
		Transform: transform,
	}, nil
}

// parseTransformExpr parses a transform expression: transform(col) or just col.
func (p *partitionSpecParser) parseTransformExpr() (iceberg.Transform, string, error) {
	p.skipWhitespace()

	// Parse the first identifier
	ident, err := p.parseIdentifier()
	if err != nil {
		return nil, "", err
	}

	p.skipWhitespace()

	// Check if this is a transform function
	if p.peek() == '(' {
		// It's a transform function
		transform, colRef, err := p.parseTransformCall(ident)
		if err != nil {
			return nil, "", err
		}
		return transform, colRef, nil
	}

	// It might be a dotted column reference (identity transform)
	colRef := ident
	for p.peek() == '.' {
		p.advance()
		nextIdent, err := p.parseIdentifier()
		if err != nil {
			return nil, "", p.errorf("expected identifier after '.'")
		}
		colRef = colRef + "." + nextIdent
	}

	return iceberg.IdentityTransform{}, colRef, nil
}

// parseTransformCall parses a transform function call: transform(args).
func (p *partitionSpecParser) parseTransformCall(transformName string) (iceberg.Transform, string, error) {
	// Consume '('
	if p.peek() != '(' {
		return nil, "", p.errorf("expected '('")
	}
	p.advance()
	p.skipWhitespace()

	lowerName := strings.ToLower(transformName)

	switch lowerName {
	case "identity":
		colRef, err := p.parseColumnRef()
		if err != nil {
			return nil, "", err
		}
		if err := p.expectChar(')'); err != nil {
			return nil, "", err
		}
		return iceberg.IdentityTransform{}, colRef, nil

	case "year":
		colRef, err := p.parseColumnRef()
		if err != nil {
			return nil, "", err
		}
		if err := p.expectChar(')'); err != nil {
			return nil, "", err
		}
		return iceberg.YearTransform{}, colRef, nil

	case "month":
		colRef, err := p.parseColumnRef()
		if err != nil {
			return nil, "", err
		}
		if err := p.expectChar(')'); err != nil {
			return nil, "", err
		}
		return iceberg.MonthTransform{}, colRef, nil

	case "day":
		colRef, err := p.parseColumnRef()
		if err != nil {
			return nil, "", err
		}
		if err := p.expectChar(')'); err != nil {
			return nil, "", err
		}
		return iceberg.DayTransform{}, colRef, nil

	case "hour":
		colRef, err := p.parseColumnRef()
		if err != nil {
			return nil, "", err
		}
		if err := p.expectChar(')'); err != nil {
			return nil, "", err
		}
		return iceberg.HourTransform{}, colRef, nil

	case "void":
		colRef, err := p.parseColumnRef()
		if err != nil {
			return nil, "", err
		}
		if err := p.expectChar(')'); err != nil {
			return nil, "", err
		}
		return iceberg.VoidTransform{}, colRef, nil

	case "bucket":
		// bucket(n, col)
		n, err := p.parseInt()
		if err != nil {
			return nil, "", p.errorf("expected bucket count: %w", err)
		}
		if n < 0 {
			return nil, "", p.errorf("bucket count must be non-negative")
		}
		p.skipWhitespace()
		if err := p.expectChar(','); err != nil {
			return nil, "", err
		}
		p.skipWhitespace()
		colRef, err := p.parseColumnRef()
		if err != nil {
			return nil, "", err
		}
		if err := p.expectChar(')'); err != nil {
			return nil, "", err
		}
		return iceberg.BucketTransform{NumBuckets: n}, colRef, nil

	case "truncate":
		// truncate(width, col)
		width, err := p.parseInt()
		if err != nil {
			return nil, "", p.errorf("expected truncate width: %w", err)
		}
		if width < 0 {
			return nil, "", p.errorf("truncate width must be non-negative")
		}
		p.skipWhitespace()
		if err := p.expectChar(','); err != nil {
			return nil, "", err
		}
		p.skipWhitespace()
		colRef, err := p.parseColumnRef()
		if err != nil {
			return nil, "", err
		}
		if err := p.expectChar(')'); err != nil {
			return nil, "", err
		}
		return iceberg.TruncateTransform{Width: width}, colRef, nil

	default:
		return nil, "", p.errorf("unknown transform: %s", transformName)
	}
}

// parseColumnRef parses a column reference (possibly dotted).
func (p *partitionSpecParser) parseColumnRef() (string, error) {
	p.skipWhitespace()
	ident, err := p.parseIdentifier()
	if err != nil {
		return "", err
	}

	colRef := ident
	for {
		p.skipWhitespace()
		if p.peek() != '.' {
			break
		}
		p.advance()
		nextIdent, err := p.parseIdentifier()
		if err != nil {
			return "", p.errorf("expected identifier after '.'")
		}
		colRef = colRef + "." + nextIdent
	}

	p.skipWhitespace()
	return colRef, nil
}

// parseIdentifier parses an identifier (plain or backtick-quoted).
func (p *partitionSpecParser) parseIdentifier() (string, error) {
	if p.pos >= len(p.input) {
		return "", p.errorf("expected identifier")
	}

	if p.peek() == '`' {
		return p.parseQuotedIdentifier()
	}

	return p.parsePlainIdentifier()
}

// parsePlainIdentifier parses a plain identifier [a-zA-Z_][a-zA-Z0-9_]*.
func (p *partitionSpecParser) parsePlainIdentifier() (string, error) {
	start := p.pos
	if p.pos >= len(p.input) {
		return "", p.errorf("expected identifier")
	}

	ch := p.peek()
	if !isIdentStart(ch) {
		return "", p.errorf("expected identifier, got '%c'", ch)
	}

	for p.pos < len(p.input) && isIdentChar(p.input[p.pos]) {
		p.pos++
	}

	return p.input[start:p.pos], nil
}

// parseQuotedIdentifier parses a backtick-quoted identifier.
func (p *partitionSpecParser) parseQuotedIdentifier() (string, error) {
	if p.peek() != '`' {
		return "", p.errorf("expected '`'")
	}
	p.advance()

	var result []byte
	for p.pos < len(p.input) {
		ch := p.input[p.pos]
		if ch == '`' {
			p.advance()
			// Check for escaped backtick (doubled)
			if p.pos < len(p.input) && p.input[p.pos] == '`' {
				result = append(result, '`')
				p.advance()
				continue
			}
			// End of quoted identifier
			return string(result), nil
		}
		result = append(result, ch)
		p.advance()
	}

	return "", p.errorf("unterminated quoted identifier")
}

// parseInt parses a non-negative integer.
func (p *partitionSpecParser) parseInt() (int, error) {
	p.skipWhitespace()
	start := p.pos

	for p.pos < len(p.input) && isDigit(p.input[p.pos]) {
		p.pos++
	}

	if start == p.pos {
		return 0, p.errorf("expected number")
	}

	numStr := p.input[start:p.pos]
	n, err := strconv.Atoi(numStr)
	if err != nil {
		return 0, p.errorf("invalid number %q: %v", numStr, err)
	}

	return n, nil
}

// resolveColumnRef resolves a column reference to a source field ID.
func (p *partitionSpecParser) resolveColumnRef(colRef string) (int, error) {
	if p.schema == nil {
		return 0, fmt.Errorf("schema is required to resolve column reference: %s", colRef)
	}

	// Handle dotted path
	parts := splitColumnRef(colRef)
	field, ok := p.schema.FindFieldByName(parts[0])
	if !ok {
		return 0, fmt.Errorf("field not found: %s", parts[0])
	}

	fieldID := field.ID

	// Navigate nested fields
	for i := 1; i < len(parts); i++ {
		st, ok := field.Type.(*iceberg.StructType)
		if !ok {
			return 0, fmt.Errorf("cannot navigate into non-struct field: %s", parts[i-1])
		}

		found := false
		for _, f := range st.FieldList {
			if f.Name == parts[i] {
				field = f
				fieldID = f.ID
				found = true
				break
			}
		}
		if !found {
			return 0, fmt.Errorf("field not found: %s", parts[i])
		}
	}

	return fieldID, nil
}

// Helper functions

func (p *partitionSpecParser) peek() byte {
	if p.pos >= len(p.input) {
		return 0
	}
	return p.input[p.pos]
}

func (p *partitionSpecParser) advance() {
	if p.pos < len(p.input) {
		p.pos++
	}
}

func (p *partitionSpecParser) skipWhitespace() {
	for p.pos < len(p.input) && isWhitespace(p.input[p.pos]) {
		p.pos++
	}
}

func (p *partitionSpecParser) expectChar(ch byte) error {
	p.skipWhitespace()
	if p.pos >= len(p.input) || p.input[p.pos] != ch {
		return p.errorf("expected '%c'", ch)
	}
	p.advance()
	return nil
}

func (p *partitionSpecParser) matchKeyword(keyword string) bool {
	end := p.pos + len(keyword)
	if end > len(p.input) {
		return false
	}

	if !strings.EqualFold(p.input[p.pos:end], keyword) {
		return false
	}

	// Make sure it's not followed by an identifier character
	if end < len(p.input) && isIdentChar(p.input[end]) {
		return false
	}

	p.pos = end
	return true
}

func (p *partitionSpecParser) errorf(format string, args ...any) error {
	return fmt.Errorf("col %d: "+format, append([]any{p.pos + 1}, args...)...)
}

func isWhitespace(ch byte) bool {
	return unicode.IsSpace(rune(ch))
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentChar(ch byte) bool {
	return isIdentStart(ch) || isDigit(ch)
}

func isDigit(ch byte) bool {
	return unicode.IsDigit(rune(ch))
}

func splitColumnRef(colRef string) []string {
	return strings.Split(colRef, ".")
}

func generatePartitionFieldName(colRef string) string {
	return strings.ReplaceAll(colRef, ".", "_")
}
