package docs

//------------------------------------------------------------------------------

// FieldSpec describes a component config field.
type FieldSpec struct {
	// Description of the field purpose (in markdown).
	Description string

	// Advanced is true for optional fields that will not be present in most
	// configs.
	Advanced bool

	// Deprecated is true for fields that are deprecated and only exist for
	// backwards compatibility reasons.
	Deprecated bool

	// Examples is a slice of optional example values for a field.
	Examples []interface{}
}

// FieldAdvanced returns a field spec for an advanced field.
func FieldAdvanced(description string, examples ...interface{}) FieldSpec {
	return FieldSpec{
		Description: description,
		Advanced:    true,
		Examples:    examples,
	}
}

// FieldCommon returns a field spec for a common field.
func FieldCommon(description string, examples ...interface{}) FieldSpec {
	return FieldSpec{
		Description: description,
		Examples:    examples,
	}
}

// FieldDeprecated returns a field spec for a deprecated field.
func FieldDeprecated() FieldSpec {
	return FieldSpec{
		Description: "DEPRECATED: Do not use.",
		Deprecated:  true,
	}
}

//------------------------------------------------------------------------------

// FieldSpecs is a map of field specs for a component.
type FieldSpecs map[string]FieldSpec

// ConfigCommon takes a sanitised configuration of a component, a map of field
// docs, and removes all fields that aren't common or are deprecated.
func (f FieldSpecs) ConfigCommon(config map[string]interface{}) map[string]interface{} {
	newMap := map[string]interface{}{}
	for k, v := range config {
		if spec, exists := f[k]; exists {
			if spec.Advanced || spec.Deprecated {
				continue
			}
		}
		newMap[k] = v
	}
	return newMap
}

// ConfigAdvanced takes a sanitised configuration of a component, a map of field
// docs, and removes all fields that are deprecated.
func (f FieldSpecs) ConfigAdvanced(config map[string]interface{}) map[string]interface{} {
	newMap := map[string]interface{}{}
	for k, v := range config {
		if f[k].Deprecated {
			continue
		}
		newMap[k] = v
	}
	return newMap
}

//------------------------------------------------------------------------------
