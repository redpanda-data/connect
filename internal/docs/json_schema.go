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
			spec["$ref"] = "#/$defs/input"
		case FieldTypeBuffer:
			spec["$ref"] = "#/$defs/buffer"
		case FieldTypeCache:
			spec["$ref"] = "#/$defs/cache"
		case FieldTypeProcessor:
			spec["$ref"] = "#/$defs/processor"
		case FieldTypeRateLimit:
			spec["$ref"] = "#/$defs/rate_limit"
		case FieldTypeOutput:
			spec["$ref"] = "#/$defs/output"
		case FieldTypeMetrics:
			spec["$ref"] = "#/$defs/metrics"
		case FieldTypeTracer:
			spec["$ref"] = "#/$defs/tracer"
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
