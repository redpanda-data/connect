package docs

// JSONSchema serializes a field spec into a JSON schema structure.
func (f FieldSpec) JSONSchema() any {
	spec := map[string]any{}
	switch f.Kind {
	case Kind2DArray:
		innerField := f
		innerField.Kind = KindArray
		spec["type"] = "array"
		spec["items"] = innerField.JSONSchema()
	case KindArray:
		innerField := f
		innerField.Kind = KindScalar
		spec["type"] = "array"
		spec["items"] = innerField.JSONSchema()
	case KindMap:
		innerField := f
		innerField.Kind = KindScalar
		spec["type"] = "object"
		spec["patternProperties"] = map[string]any{
			".": innerField.JSONSchema(),
		}
	default:
		switch f.Type {
		case FieldTypeBool:
			spec["type"] = "boolean"
		case FieldTypeString:
			spec["type"] = "string"
		case FieldTypeInt:
			spec["type"] = "number"
		case FieldTypeFloat:
			spec["type"] = "number"
		case FieldTypeObject:
			spec["type"] = "object"
			spec["properties"] = f.Children.JSONSchema()
			var required []string
			for _, child := range f.Children {
				if !child.IsOptional && child.Default == nil {
					required = append(required, child.Name)
				}
			}
			if len(required) > 0 {
				spec["required"] = required
			}
			spec["additionalProperties"] = false
		case FieldTypeInput:
			spec["$ref"] = "#/definitions/input"
		case FieldTypeBuffer:
			spec["$ref"] = "#/definitions/buffer"
		case FieldTypeCache:
			spec["$ref"] = "#/definitions/cache"
		case FieldTypeProcessor:
			spec["$ref"] = "#/definitions/processor"
		case FieldTypeRateLimit:
			spec["$ref"] = "#/definitions/rate_limit"
		case FieldTypeOutput:
			spec["$ref"] = "#/definitions/output"
		case FieldTypeMetrics:
			spec["$ref"] = "#/definitions/metrics"
		case FieldTypeTracer:
			spec["$ref"] = "#/definitions/tracer"
		case FieldTypeScanner:
			spec["$ref"] = "#/definitions/scanner"
		}
	}
	return spec
}

// JSONSchema serializes a field spec into a JSON schema structure.
func (f FieldSpecs) JSONSchema() map[string]any {
	spec := map[string]any{}
	for _, field := range f {
		spec[field.Name] = field.JSONSchema()
	}
	return spec
}
