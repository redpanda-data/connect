// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package xml

import (
	"context"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	pFieldOperator          = "operator"
	pFieldCast              = "cast"
	pFieldPreserveNamespace = "preserve_namespaces"
)

func xmlProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Parsing").
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
`+"```"+`

== Preserving XML namespaces

By default namespace prefixes on elements and attributes are dropped during conversion (e.g. `+"`<dc:title>`"+` becomes the key `+"`title`"+`), which makes the original XML impossible to reconstruct from the resulting JSON. Set `+"`preserve_namespaces`"+` to `+"`true`"+` to retain prefixes on element and attribute keys and to keep `+"`xmlns:*`"+` declarations as attributes.

For example, given the following XML:

`+"```xml"+`
<root xmlns:dc="http://my.namespace/dc" xmlns:ot="http://my.namespace/ot">
  <dc:title>This is a title</dc:title>
  <dc:description tone="boring">This is a description</dc:description>
  <ot:elements id="1">foo1</ot:elements>
  <ot:elements id="2">foo2</ot:elements>
  <ot:elements>foo3</ot:elements>
</root>
`+"```"+`

With `+"`preserve_namespaces: true`"+` the resulting JSON structure would look like this:

`+"```json"+`
{
  "root":{
    "-xmlns:dc":"http://my.namespace/dc",
    "-xmlns:ot":"http://my.namespace/ot",
    "dc:title":"This is a title",
    "dc:description":{
      "#text":"This is a description",
      "-tone":"boring"
    },
    "ot:elements":[
      {"#text":"foo1","-id":"1"},
      {"#text":"foo2","-id":"2"},
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
			service.NewBoolField(pFieldPreserveNamespace).
				Description("Whether to preserve XML namespace prefixes on element and attribute keys, and retain xmlns declarations as attributes. When disabled, namespace prefixes are stripped.").
				Default(false),
		)
}

func init() {
	service.MustRegisterProcessor(
		"xml", xmlProcSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return xmlProcFromParsed(conf, mgr)
		})
}

type xmlProc struct {
	log              *service.Logger
	cast             bool
	preserveNSPrefix bool
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

	preserveNS, err := pConf.FieldBool(pFieldPreserveNamespace)
	if err != nil {
		return nil, err
	}

	j := &xmlProc{
		log:              mgr.Logger(),
		cast:             cast,
		preserveNSPrefix: preserveNS,
	}
	return j, nil
}

func (p *xmlProc) Process(_ context.Context, msg *service.Message) (service.MessageBatch, error) {
	mBytes, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	var root map[string]any
	if p.preserveNSPrefix {
		root, err = ToMapPreserveNS(mBytes, p.cast)
	} else {
		root, err = ToMap(mBytes, p.cast)
	}
	if err != nil {
		p.log.Debugf("Failed to parse part as XML: %v", err)
		return nil, err
	}
	msg.SetStructuredMut(root)
	return service.MessageBatch{msg}, nil
}

func (*xmlProc) Close(context.Context) error {
	return nil
}
