package docs

import (
	"strings"

	"github.com/Jeffail/gabs/v2"
)

// FieldSpecCtx provides a field spec and rendered extras for documentation
// templates to use.
type FieldSpecCtx struct {
	Spec FieldSpec

	// FullName describes the full dot path name of the field relative to
	// the root of the documented component.
	FullName string

	// ExamplesMarshalled is a list of examples marshalled into YAML format.
	ExamplesMarshalled []string

	// DefaultMarshalled is a marshalled string of the default value, if there is one.
	DefaultMarshalled string
}

// FieldsTemplate returns a Go template for rendering markdown field
// documentation. The context should have a field `.Fields` of the type
// `[]FieldSpecCtx`.
func FieldsTemplate(lintableExamples bool) string {
	exampleHint := "yml"
	if lintableExamples {
		exampleHint = "yaml"
	}
	return `{{define "field_docs" -}}
{{range $i, $field := .Fields -}}
### ` + "`{{$field.FullName}}`" + `

{{$field.Spec.Description}}
{{if $field.Spec.IsSecret -}}
:::warning Secret
This field contains sensitive information that usually shouldn't be added to a config directly, read our [secrets page for more info](/docs/configuration/secrets).
:::
{{end -}}
{{if $field.Spec.Interpolated -}}
This field supports [interpolation functions](/docs/configuration/interpolation#bloblang-queries).
{{end}}

Type: {{if eq $field.Spec.Kind "array"}}list of {{end}}{{if eq $field.Spec.Kind "map"}}map of {{end}}` + "`{{$field.Spec.Type}}`" + `  
{{if gt (len $field.DefaultMarshalled) 0}}Default: ` + "`{{$field.DefaultMarshalled}}`" + `  
{{end -}}
{{if gt (len $field.Spec.Version) 0}}Requires version {{$field.Spec.Version}} or newer  
{{end -}}
{{if gt (len $field.Spec.AnnotatedOptions) 0}}
| Option | Summary |
|---|---|
{{range $j, $option := $field.Spec.AnnotatedOptions -}}` + "| `" + `{{index $option 0}}` + "` |" + ` {{index $option 1}} |
{{end}}
{{else if gt (len $field.Spec.Options) 0}}Options: {{range $j, $option := $field.Spec.Options -}}
{{if ne $j 0}}, {{end}}` + "`" + `{{$option}}` + "`" + `{{end}}.
{{end}}
{{if gt (len $field.Spec.Examples) 0 -}}
` + "```" + exampleHint + `
# Examples

{{range $j, $example := $field.ExamplesMarshalled -}}
{{if ne $j 0}}
{{end}}{{$example}}{{end -}}
` + "```" + `

{{end -}}
{{end -}}
{{end -}}`
}

// FlattenChildrenForDocs converts the children of a field into a flat list,
// where the names contain hints as to their position in a structured hierarchy.
// This makes it easier to list the fields in documentation.
func (f FieldSpec) FlattenChildrenForDocs() []FieldSpecCtx {
	flattenedFields := []FieldSpecCtx{}
	var walkFields func(path string, f FieldSpecs)
	walkFields = func(path string, f FieldSpecs) {
		for _, v := range f {
			if v.IsDeprecated {
				continue
			}
			newV := FieldSpecCtx{
				Spec: v,
			}
			newV.FullName = newV.Spec.Name
			if len(path) > 0 {
				newV.FullName = path + newV.Spec.Name
			}
			if len(v.Examples) > 0 {
				newV.ExamplesMarshalled = make([]string, len(v.Examples))
				for i, e := range v.Examples {
					exampleBytes, err := marshalYAML(map[string]any{
						v.Name: e,
					})
					if err == nil {
						newV.ExamplesMarshalled[i] = string(exampleBytes)
					}
				}
			}
			if v.Default != nil {
				newV.DefaultMarshalled = gabs.Wrap(*v.Default).String()
			}
			newV.Spec.Description = strings.TrimSpace(v.Description)
			if newV.Spec.Description == "" {
				newV.Spec.Description = "Sorry! This field is missing documentation."
			}

			flattenedFields = append(flattenedFields, newV)
			if len(v.Children) > 0 {
				newPath := path + v.Name
				switch newV.Spec.Kind {
				case KindArray:
					newPath += "[]"
				case Kind2DArray:
					newPath += "[][]"
				case KindMap:
					newPath += ".<name>"
				}
				walkFields(newPath+".", v.Children)
			}
		}
	}
	rootPath := ""
	switch f.Kind {
	case KindArray:
		rootPath = "[]."
	case KindMap:
		rootPath = "<name>."
	}
	walkFields(rootPath, f.Children)
	return flattenedFields
}
