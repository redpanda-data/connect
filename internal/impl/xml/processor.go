package xml

import (
	"context"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	pFieldOperator = "operator"
	pFieldCast     = "cast"
)

func xmlProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Parsing").
		Beta().
		Summary(`Parses messages as an XML document, performs a mutation on the data, and then overwrites the previous contents with the new value.`).
		Description(`
== Operators

=== `+"`to_json`"+`

Converts an XML document into a JSON structure, where elements appear as keys of an object according to the following rules:

- If an element contains attributes they are parsed by prefixing a hyphen, `+"`-`"+`, to the attribute label.
- If the element is a simple element and has attributes, the element value is given the key `+"`#text`"+`.
- XML comments, directives, and process instructions are ignored.
- When elements are repeated the resulting JSON value is an array.

For example, given the following XML:

`+"```xml"+`
<root>
  <title>This is a title</title>
  <description tone="boring">This is a description</description>
  <elements id="1">foo1</elements>
  <elements id="2">foo2</elements>
  <elements>foo3</elements>
</root>
`+"```"+`

The resulting JSON structure would look like this:

`+"```json"+`
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
`+"```"+`

With cast set to true, the resulting JSON structure would look like this:

`+"```json"+`
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
`+"```").
		Fields(
			service.NewStringEnumField(pFieldOperator, "to_json").
				Description("An XML <<operators, operation>> to apply to messages.").
				Default(""),
			service.NewBoolField(pFieldCast).
				Description("Whether to try to cast values that are numbers and booleans to the right type. Default: all values are strings.").
				Default(false),
		)
}

func init() {
	err := service.RegisterProcessor(
		"xml", xmlProcSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return xmlProcFromParsed(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type xmlProc struct {
	log  *service.Logger
	cast bool
}

func xmlProcFromParsed(pConf *service.ParsedConfig, mgr *service.Resources) (*xmlProc, error) {
	operator, err := pConf.FieldString(pFieldOperator)
	if err != nil {
		return nil, err
	}
	if operator != "to_json" {
		return nil, fmt.Errorf("operator not recognised: %v", operator)
	}

	cast, err := pConf.FieldBool(pFieldCast)
	if err != nil {
		return nil, err
	}

	j := &xmlProc{
		log:  mgr.Logger(),
		cast: cast,
	}
	return j, nil
}

func (p *xmlProc) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	mBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	root, err := ToMap(mBytes, p.cast)
	if err != nil {
		p.log.Debugf("Failed to parse part as XML: %v", err)
		return nil, err
	}
	msg.SetStructuredMut(root)
	return service.MessageBatch{msg}, nil
}

func (p *xmlProc) Close(ctx context.Context) error {
	return nil
}
