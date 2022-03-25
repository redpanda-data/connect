package processor

import (
	"context"
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/xml"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	Constructors[TypeXML] = TypeSpec{
		constructor: func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (processor.V1, error) {
			p, err := newXML(conf.XML, mgr)
			if err != nil {
				return nil, err
			}
			return processor.NewV2ToV1Processor("xml", p, mgr.Metrics()), nil
		},
		Status: docs.StatusBeta,
		Categories: []string{
			"Parsing",
		},
		Summary: `
Parses messages as an XML document, performs a mutation on the data, and then
overwrites the previous contents with the new value.`,
		Description: `
## Operators

### ` + "`to_json`" + `

Converts an XML document into a JSON structure, where elements appear as keys of
an object according to the following rules:

- If an element contains attributes they are parsed by prefixing a hyphen,
  ` + "`-`" + `, to the attribute label.
- If the element is a simple element and has attributes, the element value
  is given the key ` + "`#text`" + `.
- XML comments, directives, and process instructions are ignored.
- When elements are repeated the resulting JSON value is an array.

For example, given the following XML:

` + "```xml" + `
<root>
  <title>This is a title</title>
  <description tone="boring">This is a description</description>
  <elements id="1">foo1</elements>
  <elements id="2">foo2</elements>
  <elements>foo3</elements>
</root>
` + "```" + `

The resulting JSON structure would look like this:

` + "```json" + `
{
  "root":{
    "title":"This is a title",
    "description":{
      "#text":"This is a description",
      "-tone":"boring"
    },
    "elements":[
      {"#text":"foo1","-id":"1"},
      {"#text":"foo2","-id":"2"},
      "foo3"
    ]
  }
}
` + "```" + `

With cast set to true, the resulting JSON structure would look like this:

` + "```json" + `
{
  "root":{
    "title":"This is a title",
    "description":{
      "#text":"This is a description",
      "-tone":"boring"
    },
    "elements":[
      {"#text":"foo1","-id":1},
      {"#text":"foo2","-id":2},
      "foo3"
    ]
  }
}
` + "```" + ``,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("operator", "An XML [operation](#operators) to apply to messages.").HasOptions("to_json"),
			docs.FieldBool("cast", "Whether to try to cast values that are numbers and booleans to the right type. Default: all values are strings."),
		),
	}
}

//------------------------------------------------------------------------------

// XMLConfig contains configuration fields for the XML processor.
type XMLConfig struct {
	Operator string `json:"operator" yaml:"operator"`
	Cast     bool   `json:"cast" yaml:"cast"`
}

// NewXMLConfig returns a XMLConfig with default values.
func NewXMLConfig() XMLConfig {
	return XMLConfig{
		Operator: "",
		Cast:     false,
	}
}

//------------------------------------------------------------------------------

type xmlProc struct {
	log  log.Modular
	cast bool
}

func newXML(conf XMLConfig, mgr interop.Manager) (*xmlProc, error) {
	if conf.Operator != "to_json" {
		return nil, fmt.Errorf("operator not recognised: %v", conf.Operator)
	}
	j := &xmlProc{
		log:  mgr.Logger(),
		cast: conf.Cast,
	}
	return j, nil
}

func (p *xmlProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	newPart := msg.Copy()
	root, err := xml.ToMap(newPart.Get(), p.cast)
	if err != nil {
		p.log.Debugf("Failed to parse part as XML: %v", err)
		return nil, err
	}
	newPart.SetJSON(root)
	return []*message.Part{newPart}, nil
}

func (p *xmlProc) Close(ctx context.Context) error {
	return nil
}
