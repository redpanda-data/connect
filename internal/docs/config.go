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
).OmitWhen(func(field, parent any) (string, bool) {
	gObj := gabs.Wrap(parent)
	if typeStr, exists := gObj.S("type").Data().(string); exists && typeStr == "resource" {
		return "label field should be omitted when pointing to a resource", true
	}
	if resourceStr, exists := gObj.S("resource").Data().(string); exists && resourceStr != "" {
		return "label field should be omitted when pointing to a resource", true
	}
	return "", false
}).AtVersion("3.44.0").LinterFunc(func(ctx LintContext, line, col int, v any) []Lint {
	l, _ := v.(string)
	if l == "" {
		return nil
	}
	if err := ValidateLabel(l); err != nil {
		return []Lint{
			NewLintError(line, LintBadLabel, fmt.Sprintf("Invalid label '%v': %v", l, err)),
		}
	}
	prevLine, exists := ctx.LabelsToLine[l]
	if exists {
		return []Lint{
			NewLintError(line, LintDuplicateLabel, fmt.Sprintf("Label '%v' collides with a previously defined label at line %v", l, prevLine)),
		}
	}
	ctx.LabelsToLine[l] = line
	return nil
}).HasDefault("")

// ReservedFieldsByType returns a map of fields for a specific type.
func ReservedFieldsByType(t Type) map[string]FieldSpec {
	m := map[string]FieldSpec{
		"type":   FieldString("type", ""),
		"plugin": FieldObject("plugin", ""),
	}
	if t == TypeInput || t == TypeOutput {
		m["processors"] = FieldProcessor("processors", "").Array().OmitWhen(func(field, _ any) (string, bool) {
			if arr, ok := field.([]any); ok && len(arr) == 0 {
				return "field processors is empty and can be removed", true
			}
			return "", false
		})
	}
	if t == TypeMetrics {
		m["mapping"] = MetricsMappingFieldSpec("mapping")
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

func defaultTypeByType(docProvider Provider, t Type) string {
	switch t {
	case TypeBuffer:
		return "none"
	case TypeInput:
		return "stdin"
	case TypeMetrics:
		// If prometheus isn't imported then fall back to none
		if _, exists := docProvider.GetDocs("prometheus", TypeMetrics); exists {
			return "prometheus"
		}
		return "none"
	case TypeOutput:
		return "stdout"
	case TypeTracer:
		return "none"
	// No defaults for the following
	case TypeCache:
		return ""
	case TypeProcessor:
		return ""
	case TypeRateLimit:
		return ""
	}
	return ""
}

// DefaultTypeOf returns the standard default implementation of a given
// component type, which is the implementation used in a stream when no config
// for the component is present. Only some component types have a default, for
// those that do not an empty string is returned.
func DefaultTypeOf(t Type) string {
	return defaultTypeByType(DeprecatedProvider, t)
}

func getInferenceCandidateFromList(docProvider Provider, t Type, l []string) (string, ComponentSpec, error) {
	ignore := ReservedFieldsByType(t)

	var candidates []string
	var inferred string
	var inferredSpec ComponentSpec
	for _, k := range l {
		if _, exists := ignore[k]; exists {
			continue
		}
		candidates = append(candidates, k)
		if spec, exists := docProvider.GetDocs(k, t); exists {
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

	if len(candidates) == 0 {
		defaultType := defaultTypeByType(docProvider, t)
		if spec, exists := docProvider.GetDocs(defaultType, t); exists {
			return defaultType, spec, nil
		}
		if inferred == "" {
			return "", ComponentSpec{}, fmt.Errorf("an explicit %v type must be specified", string(t))
		}
	}

	if inferred == "" {
		sort.Strings(candidates)
		return "", ComponentSpec{}, fmt.Errorf("unable to infer %v type from candidates: %v", string(t), candidates)
	}
	return inferred, inferredSpec, nil
}

// SanitiseConfig contains fields describing the desired behaviour of the config
// sanitiser such as removing certain fields.
type SanitiseConfig struct {
	RemoveTypeField  bool
	RemoveDeprecated bool
	ScrubSecrets     bool
	ForExample       bool
	Filter           FieldFilter
	DocsProvider     Provider
}

// NewSanitiseConfig creates a new sanitise config.
func NewSanitiseConfig() SanitiseConfig {
	return SanitiseConfig{
		DocsProvider: DeprecatedProvider,
	}
}
