package service

import (
	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

// StreamConfigMarshaller provides utilities for marshalling stream configs,
// allowing you to print sanitised, redacted or hydrated configs.
type StreamConfigMarshaller struct {
	env       *Environment
	spec      docs.FieldSpecs
	sanitConf docs.SanitiseConfig
}

// NewStreamConfigMarshaller creates a component for marshalling stream configs,
// allowing you to print sanitised, redacted or hydrated configs in various
// formats.
func (s *ConfigSchema) NewStreamConfigMarshaller() *StreamConfigMarshaller {
	sanitConf := docs.NewSanitiseConfig(s.env.internal)
	sanitConf.RemoveTypeField = true

	return &StreamConfigMarshaller{
		env:       s.env,
		spec:      s.fields,
		sanitConf: sanitConf,
	}
}

// SetScrubSecrets sets whether fields that contain secrets should be scrubbed
// when they contain raw values.
func (s *StreamConfigMarshaller) SetScrubSecrets(v bool) *StreamConfigMarshaller {
	s.sanitConf.ScrubSecrets = v
	return s
}

// SetHydrateExamples sets whether to recurse and hydrate the config with
// default or example values when marshalling. This is useful for generating
// example configs.
func (s *StreamConfigMarshaller) SetHydrateExamples(v bool) *StreamConfigMarshaller {
	s.sanitConf.ForExample = v
	return s
}

// SetOmitDeprecated sets whether deprecated fields should be omitted from the
// marshalled result.
func (s *StreamConfigMarshaller) SetOmitDeprecated(v bool) *StreamConfigMarshaller {
	s.sanitConf.RemoveDeprecated = v
	return s
}

// FieldView provides a way to analyse a config field.
type FieldView struct {
	f docs.FieldSpec
}

// IsAdvanced returns true if the field is advanced.
func (f *FieldView) IsAdvanced() bool {
	return f.f.IsAdvanced
}

// SetFieldFilter sets a closure function to be used when preparing the
// marshalled config which determines whether the field is kept in the config,
// otherwise it is removed.
func (s *StreamConfigMarshaller) SetFieldFilter(fn func(view *FieldView, value any) bool) *StreamConfigMarshaller {
	s.sanitConf.Filter = func(spec docs.FieldSpec, v any) bool {
		return fn(&FieldView{f: spec}, v)
	}
	return s
}

// AnyToYAML attempts to parse a config expressed as a structured value
// according to a stream config schema and then marshals it into a YAML string.
func (s *StreamConfigMarshaller) AnyToYAML(v any) (yamlStr string, err error) {
	var pConf *docs.ParsedConfig
	if pConf, err = s.spec.ParsedConfigFromAny(v); err != nil {
		return
	}

	var node yaml.Node
	if err = node.Encode(pConf.Raw()); err != nil {
		return
	}
	if err = s.spec.SanitiseYAML(&node, s.sanitConf); err != nil {
		return
	}
	var configYAML []byte
	if configYAML, err = docs.MarshalYAML(node); err != nil {
		return
	}
	yamlStr = string(configYAML)
	return
}
