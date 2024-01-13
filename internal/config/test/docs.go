package test

import (
	"bytes"
	"fmt"
	"text/template"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/docs"

	_ "embed"
)

const fieldTests = "tests"

//go:embed docs.md
var testDocs string

type testContext struct {
	Fields []docs.FieldSpecCtx
}

// DocsMarkdown returns a markdown document for the templates documentation.
func DocsMarkdown() ([]byte, error) {
	testDocsTemplate := docs.FieldsTemplate(false) + testDocs

	var buf bytes.Buffer
	err := template.Must(template.New("tests").Parse(testDocsTemplate)).Execute(&buf, testContext{
		Fields: docs.FieldObject("", "").WithChildren(ConfigSpec()).FlattenChildrenForDocs(),
	})

	return buf.Bytes(), err
}

// ConfigSpec returns a configuration spec for a template.
func ConfigSpec() docs.FieldSpec {
	return docs.FieldObject(fieldTests, "A list of one or more unit tests to execute.").Array().WithChildren(caseFields()...).Optional()
}

func FromAny(v any) ([]Case, error) {
	if t, ok := v.(*yaml.Node); ok {
		var tmp struct {
			Tests []yaml.Node
		}
		if err := t.Decode(&tmp); err != nil {
			return nil, err
		}
		var cases []Case
		for i, v := range tmp.Tests {
			pConf, err := caseFields().ParsedConfigFromAny(&v)
			if err != nil {
				return nil, fmt.Errorf("%v: %w", i, err)
			}
			c, err := CaseFromParsed(pConf)
			if err != nil {
				return nil, fmt.Errorf("%v: %w", i, err)
			}
			cases = append(cases, c)
		}
		return cases, nil
	}

	pConf, err := ConfigSpec().ParsedConfigFromAny(v)
	if err != nil {
		return nil, err
	}
	return FromParsed(pConf)
}

func FromParsed(pConf *docs.ParsedConfig) ([]Case, error) {
	if !pConf.Contains(fieldTests) {
		return nil, nil
	}

	oList, err := pConf.FieldObjectList(fieldTests)
	if err != nil {
		return nil, err
	}

	var cases []Case
	for i, pc := range oList {
		c, err := CaseFromParsed(pc)
		if err != nil {
			return nil, fmt.Errorf("%v: %w", i, err)
		}
		cases = append(cases, c)
	}
	return cases, nil
}
