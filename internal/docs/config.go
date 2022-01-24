package docs

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/Jeffail/gabs/v2"
)

const labelExpression = `^[a-z0-9_]+$`

var (
	labelRe = regexp.MustCompile(labelExpression)

	// ErrBadLabel is returned when creating a component with a bad label.
	ErrBadLabel = fmt.Errorf("should match the regular expression /%v/ and must not start with an underscore", labelExpression)
)

// ValidateLabel attempts to validate the contents of a component label.
func ValidateLabel(label string) error {
	if strings.HasPrefix(label, "_") {
		return ErrBadLabel
	}
	if !labelRe.MatchString(label) {
		return ErrBadLabel
	}
	return nil
}

var labelField = FieldString(
	"label", "An optional label to use as an identifier for observability data such as metrics and logging.",
).OmitWhen(func(field, parent interface{}) (string, bool) {
	gObj := gabs.Wrap(parent)
	if typeStr, exists := gObj.S("type").Data().(string); exists && typeStr == "resource" {
		return "label field should be omitted when pointing to a resource", true
	}
	if resourceStr, exists := gObj.S("resource").Data().(string); exists && resourceStr != "" {
		return "label field should be omitted when pointing to a resource", true
	}
	return "", false
}).AtVersion("3.44.0").Linter(func(ctx LintContext, line, col int, v interface{}) []Lint {
	l, _ := v.(string)
	if l == "" {
		return nil
	}
	if err := ValidateLabel(l); err != nil {
		return []Lint{
			NewLintError(line, fmt.Sprintf("Invalid label '%v': %v", l, err)),
		}
	}
	prevLine, exists := ctx.LabelsToLine[l]
	if exists {
		return []Lint{
			NewLintError(line, fmt.Sprintf("Label '%v' collides with a previously defined label at line %v", l, prevLine)),
		}
	}
	ctx.LabelsToLine[l] = line
	return nil
})

func reservedFieldsByType(t Type) map[string]FieldSpec {
	m := map[string]FieldSpec{
		"type":   FieldString("type", ""),
		"plugin": FieldCommon("plugin", "").HasType(FieldTypeObject),
	}
	if t == TypeInput || t == TypeOutput {
		m["processors"] = FieldCommon("processors", "").Array().HasType(FieldTypeProcessor).OmitWhen(func(field, _ interface{}) (string, bool) {
			if arr, ok := field.([]interface{}); ok && len(arr) == 0 {
				return "field processors is empty and can be removed", true
			}
			return "", false
		})
	}
	if _, isLabelType := map[Type]struct{}{
		TypeInput:     {},
		TypeProcessor: {},
		TypeOutput:    {},
		TypeCache:     {},
		TypeRateLimit: {},
	}[t]; isLabelType {
		m["label"] = labelField
	}
	return m
}

// GetInferenceCandidate checks a generic config structure for a component and
// returns either the inferred type name or an error if one cannot be inferred.
func GetInferenceCandidate(docProvider Provider, t Type, defaultType string, raw interface{}) (string, ComponentSpec, error) {
	m, ok := raw.(map[string]interface{})
	if !ok {
		return "", ComponentSpec{}, fmt.Errorf("invalid config value %T, expected object", raw)
	}

	if tStr, ok := m["type"].(string); ok {
		spec, exists := GetDocs(docProvider, tStr, t)
		if !exists {
			return "", ComponentSpec{}, fmt.Errorf("%v type '%v' was not recognised", string(t), tStr)
		}
		return tStr, spec, nil
	}

	var keys []string
	for k := range m {
		keys = append(keys, k)
	}

	return getInferenceCandidateFromList(docProvider, t, defaultType, keys)
}

func getInferenceCandidateFromList(docProvider Provider, t Type, defaultType string, l []string) (string, ComponentSpec, error) {
	ignore := reservedFieldsByType(t)

	var candidates []string
	var inferred string
	var inferredSpec ComponentSpec
	for _, k := range l {
		if _, exists := ignore[k]; exists {
			continue
		}
		candidates = append(candidates, k)
		if spec, exists := GetDocs(docProvider, k, t); exists {
			if len(inferred) > 0 {
				candidates = []string{inferred, k}
				sort.Strings(candidates)
				return "", ComponentSpec{}, fmt.Errorf(
					"unable to infer %v type, multiple candidates '%v' and '%v'", string(t), candidates[0], candidates[1],
				)
			}
			inferred = k
			inferredSpec = spec
		}
	}

	if len(candidates) == 0 && len(defaultType) > 0 {
		// A totally empty component config results in the default.
		// TODO: V4 Disable this
		if spec, exists := GetDocs(docProvider, defaultType, t); exists {
			return defaultType, spec, nil
		}
	}

	if inferred == "" {
		sort.Strings(candidates)
		return "", ComponentSpec{}, fmt.Errorf("unable to infer %v type, candidates were: %v", string(t), candidates)
	}
	return inferred, inferredSpec, nil
}

// SanitiseConfig contains fields describing the desired behaviour of the config
// sanitiser such as removing certain fields.
type SanitiseConfig struct {
	RemoveTypeField  bool
	RemoveDeprecated bool
	ForExample       bool
	Filter           FieldFilter
	DocsProvider     Provider
}

// GetDocs attempts to obtain documentation for a component implementation from
// a docs provider in the config, or if omitted uses the global provider.
func (c SanitiseConfig) GetDocs(name string, ctype Type) (ComponentSpec, bool) {
	return GetDocs(c.DocsProvider, name, ctype)
}
