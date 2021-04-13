package docs

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/interop/plugins"
	"github.com/Jeffail/gabs/v2"
	"gopkg.in/yaml.v3"
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

var labelField = FieldCommon(
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
	prevLine, exists := ctx.Labels[l]
	if exists {
		return []Lint{
			NewLintError(line, fmt.Sprintf("Label '%v' collides with a previously defined label at line %v", l, prevLine)),
		}
	}
	ctx.Labels[l] = line
	return nil
})

func reservedFieldsByType(t Type) map[string]FieldSpec {
	m := map[string]FieldSpec{
		"type":   FieldCommon("type", "").HasType(FieldString),
		"plugin": FieldCommon("plugin", "").HasType(FieldObject),
	}
	if t == TypeInput || t == TypeOutput {
		m["processors"] = FieldCommon("processors", "").Array().HasType(FieldProcessor).OmitWhen(func(field, _ interface{}) (string, bool) {
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

func refreshOldPlugins() {
	plugins.FlushNameTypes(func(nt [2]string) {
		RegisterDocs(ComponentSpec{
			Name:   nt[0],
			Type:   Type(nt[1]),
			Status: StatusPlugin,
		})
	})
}

// GetInferenceCandidate checks a generic config structure for a component and
// returns either the inferred type name or an error if one cannot be inferred.
func GetInferenceCandidate(t Type, defaultType string, raw interface{}) (string, ComponentSpec, error) {
	refreshOldPlugins()

	m, ok := raw.(map[string]interface{})
	if !ok {
		return "", ComponentSpec{}, fmt.Errorf("invalid config value %T, expected object", raw)
	}

	if tStr, ok := m["type"].(string); ok {
		spec, exists := GetDocs(tStr, t)
		if !exists {
			return "", ComponentSpec{}, fmt.Errorf("%v type '%v' was not recognised", string(t), tStr)
		}
		return tStr, spec, nil
	}

	var keys []string
	for k := range m {
		keys = append(keys, k)
	}

	return getInferenceCandidateFromList(t, defaultType, keys)
}

// GetInferenceCandidateFromNode checks a yaml node config structure for a
// component and returns either the inferred type name or an error if one cannot
// be inferred.
func GetInferenceCandidateFromNode(t Type, defaultType string, node *yaml.Node) (string, ComponentSpec, error) {
	refreshOldPlugins()

	if node.Kind != yaml.MappingNode {
		return "", ComponentSpec{}, fmt.Errorf("invalid type %v, expected object", node.Kind)
	}

	var keys []string
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == "type" {
			tStr := node.Content[i+1].Value
			spec, exists := GetDocs(tStr, t)
			if !exists {
				return "", ComponentSpec{}, fmt.Errorf("%v type '%v' was not recognised", string(t), tStr)
			}
			return tStr, spec, nil
		}
		keys = append(keys, node.Content[i].Value)
	}

	return getInferenceCandidateFromList(t, defaultType, keys)
}

func getInferenceCandidateFromList(t Type, defaultType string, l []string) (string, ComponentSpec, error) {
	ignore := reservedFieldsByType(t)

	var candidates []string
	var inferred string
	var inferredSpec ComponentSpec
	for _, k := range l {
		if _, exists := ignore[k]; exists {
			continue
		}
		candidates = append(candidates, k)
		if spec, exists := GetDocs(k, t); exists {
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
		if spec, exists := GetDocs(defaultType, t); exists {
			return defaultType, spec, nil
		}
	}

	if len(inferred) == 0 {
		sort.Strings(candidates)
		return "", ComponentSpec{}, fmt.Errorf("unable to infer %v type, candidates were: %v", string(t), candidates)
	}
	return inferred, inferredSpec, nil
}

// TODO: V4 Remove this.
func sanitiseConditionConfig(raw interface{}, removeDeprecated bool) error {
	// This is a nasty hack until Benthos v4.
	m, ok := raw.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expected object configuration type, found: %T", raw)
	}
	typeStr, ok := m["type"]
	if !ok {
		return nil
	}
	for k := range m {
		if k == typeStr || k == "type" || k == "plugin" {
			continue
		}
		delete(m, k)
	}
	return nil
}

// TODO: V4 Remove this.
func sanitiseConditionConfigNode(node *yaml.Node) error {
	// This is a nasty hack until Benthos v4.
	newNodes := []*yaml.Node{}

	var name string
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == "type" {
			name = node.Content[i+1].Value
			newNodes = append(newNodes, node.Content[i])
			newNodes = append(newNodes, node.Content[i+1])
			break
		}
	}

	if name == "" {
		return nil
	}

	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == name {
			newNodes = append(newNodes, node.Content[i])
			newNodes = append(newNodes, node.Content[i+1])
			break
		}
	}

	node.Content = newNodes
	return nil
}

// SanitiseComponentConfig reduces a raw component configuration into only the
// fields for the component name configured.
func SanitiseComponentConfig(componentType Type, raw interface{}, filter FieldFilter) error {
	if componentType == "condition" {
		return sanitiseConditionConfig(raw, false)
	}

	name, spec, err := GetInferenceCandidate(componentType, "", raw)
	if err != nil {
		return err
	}

	m, ok := raw.(map[string]interface{})
	if !ok {
		return fmt.Errorf("expected object configuration type, found: %T", raw)
	}

	if componentConfRaw, exists := m[name]; exists {
		spec.Config.sanitise(componentConfRaw, filter)
	}

	reservedFields := reservedFieldsByType(componentType)
	for k, v := range m {
		if k == name {
			continue
		}
		spec, exists := reservedFields[k]
		if !exists {
			delete(m, k)
		}
		if _, omit := spec.shouldOmit(v, m); omit {
			delete(m, k)
		}
	}

	for name, fieldSpec := range reservedFields {
		fieldSpec.sanitise(m[name], filter)
	}
	return nil
}

// SanitiseConfig contains fields describing the desired behaviour of the config
// sanitiser such as removing certain fields.
type SanitiseConfig struct {
	RemoveTypeField  bool
	RemoveDeprecated bool
	Filter           FieldFilter
}

// SanitiseNode takes a yaml.Node and a config spec and sorts the fields of the
// node according to the spec. Also optionally removes the `type` field from
// this and all nested components.
func SanitiseNode(cType Type, node *yaml.Node, conf SanitiseConfig) error {
	if cType == "condition" {
		return sanitiseConditionConfigNode(node)
	}

	newNodes := []*yaml.Node{}

	var name string
	var keys []string
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == "label" {
			if _, omit := labelField.shouldOmitNode(node.Content[i+1], node); !omit {
				newNodes = append(newNodes, node.Content[i])
				newNodes = append(newNodes, node.Content[i+1])
			}
			break
		}
	}
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == "type" {
			name = node.Content[i+1].Value
			if !conf.RemoveTypeField {
				newNodes = append(newNodes, node.Content[i])
				newNodes = append(newNodes, node.Content[i+1])
			}
			break
		} else {
			keys = append(keys, node.Content[i].Value)
		}
	}
	if len(name) == 0 {
		if len(node.Content) == 0 {
			return nil
		}
		var err error
		if name, _, err = getInferenceCandidateFromList(cType, "", keys); err != nil {
			return err
		}
	}

	cSpec, exists := GetDocs(name, cType)
	if !exists {
		return fmt.Errorf("failed to obtain docs for %v type %v", cType, name)
	}

	nameFound := false
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == name {
			nameFound = true
			if err := cSpec.Config.SanitiseNode(node.Content[i+1], conf); err != nil {
				return err
			}
			newNodes = append(newNodes, node.Content[i])
			newNodes = append(newNodes, node.Content[i+1])
			break
		}
	}

	// If the type field was omitted but we didn't see a config under the name
	// then we need to add it back in as it cannot be inferred.
	if !nameFound && conf.RemoveTypeField {
		for i := 0; i < len(node.Content)-1; i += 2 {
			if node.Content[i].Value == "type" {
				newNodes = append(newNodes, node.Content[i])
				newNodes = append(newNodes, node.Content[i+1])
				break
			}
		}
	}

	reservedFields := reservedFieldsByType(cType)
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == name || node.Content[i].Value == "type" || node.Content[i].Value == "label" {
			continue
		}
		if spec, exists := reservedFields[node.Content[i].Value]; exists {
			if _, omit := spec.shouldOmitNode(node.Content[i+1], node); omit {
				continue
			}
			if err := spec.SanitiseNode(node.Content[i+1], conf); err != nil {
				return err
			}
			newNodes = append(newNodes, node.Content[i])
			newNodes = append(newNodes, node.Content[i+1])
		}
	}

	node.Content = newNodes
	return nil
}

// LintNode takes a yaml.Node and a config spec and returns a list of linting
// errors found in the config.
func LintNode(ctx LintContext, cType Type, node *yaml.Node) []Lint {
	if cType == "condition" {
		return nil
	}

	var lints []Lint

	var name string
	var keys []string
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == "type" {
			name = node.Content[i+1].Value
			break
		} else {
			keys = append(keys, node.Content[i].Value)
		}
	}
	if len(name) == 0 {
		if len(node.Content) == 0 {
			return nil
		}
		var err error
		if name, _, err = getInferenceCandidateFromList(cType, "", keys); err != nil {
			lints = append(lints, NewLintWarning(node.Line, "unable to infer component type"))
			return lints
		}
	}

	cSpec, exists := GetDocs(name, cType)
	if !exists {
		lints = append(lints, NewLintWarning(node.Line, fmt.Sprintf("failed to obtain docs for %v type %v", cType, name)))
		return lints
	}

	lintTarget := name
	if cSpec.Status == StatusPlugin {
		lintTarget = "plugin"
	}
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == lintTarget {
			lints = append(lints, cSpec.Config.lintNode(ctx, node.Content[i+1])...)
			break
		}
	}

	reservedFields := reservedFieldsByType(cType)
	for i := 0; i < len(node.Content)-1; i += 2 {
		if node.Content[i].Value == name || node.Content[i].Value == "type" {
			continue
		}
		spec, exists := reservedFields[node.Content[i].Value]
		if exists {
			lints = append(lints, lintFromOmit(spec, node, node.Content[i+1])...)
			lints = append(lints, spec.lintNode(ctx, node.Content[i+1])...)
		} else {
			lints = append(lints, NewLintError(
				node.Content[i].Line,
				fmt.Sprintf("field %v is invalid when the component type is %v (%v)", node.Content[i].Value, name, cType),
			))
		}
	}

	return lints
}
