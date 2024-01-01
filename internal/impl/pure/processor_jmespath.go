package pure

import (
	"context"
	"encoding/json"
	"fmt"

	jmespath "github.com/jmespath/go-jmespath"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	jmpFieldQuery = "query"
)

func jmpProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Mapping").
		Stable().
		Summary("Executes a [JMESPath query](http://jmespath.org/) on JSON documents and replaces the message with the resulting document.").
		Description(`
:::note Try out Bloblang
For better performance and improved capabilities try out native Benthos mapping with the [`+"`mapping`"+` processor](/docs/components/processors/mapping).
:::
`).
		Example("Mapping", `
When receiving JSON documents of the form:

`+"```json"+`
{
  "locations": [
    {"name": "Seattle", "state": "WA"},
    {"name": "New York", "state": "NY"},
    {"name": "Bellevue", "state": "WA"},
    {"name": "Olympia", "state": "WA"}
  ]
}
`+"```"+`

We could collapse the location names from the state of Washington into a field `+"`Cities`"+`:

`+"```json"+`
{"Cities": "Bellevue, Olympia, Seattle"}
`+"```"+`

With the following config:`,
			`
pipeline:
  processors:
    - jmespath:
        query: "locations[?state == 'WA'].name | sort(@) | {Cities: join(', ', @)}"
`,
		).
		Field(service.NewStringField(jmpFieldQuery).
			Description("The JMESPath query to apply to messages."))
}

func init() {
	err := service.RegisterBatchProcessor(
		"jmespath", jmpProcSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			queryStr, err := conf.FieldString(jmpFieldQuery)
			if err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			p, err := newJMESPath(queryStr, mgr)
			if err != nil {
				return nil, err
			}

			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedProcessor("jmespath", p, mgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

type jmespathProc struct {
	query *jmespath.JMESPath
	log   log.Modular
}

func newJMESPath(queryStr string, mgr bundle.NewManagement) (processor.AutoObserved, error) {
	query, err := jmespath.Compile(queryStr)
	if err != nil {
		return nil, fmt.Errorf("failed to compile JMESPath query: %v", err)
	}
	j := &jmespathProc{
		query: query,
		log:   mgr.Logger(),
	}
	return j, nil
}

func safeSearch(part any, j *jmespath.JMESPath) (res any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("jmespath panic: %v", r)
		}
	}()
	return j.Search(part)
}

// JMESPath doesn't like json.Number so we walk the tree and replace them.
func clearNumbers(v any) (any, bool) {
	switch t := v.(type) {
	case map[string]any:
		for k, v := range t {
			if nv, ok := clearNumbers(v); ok {
				t[k] = nv
			}
		}
	case []any:
		for i, v := range t {
			if nv, ok := clearNumbers(v); ok {
				t[i] = nv
			}
		}
	case json.Number:
		f, err := t.Float64()
		if err != nil {
			if i, err := t.Int64(); err == nil {
				return i, true
			}
		}
		return f, true
	}
	return nil, false
}

func (p *jmespathProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	jsonPart, err := msg.AsStructuredMut()
	if err != nil {
		p.log.Debugf("Failed to parse part into json: %v\n", err)
		return nil, err
	}
	if v, replace := clearNumbers(jsonPart); replace {
		jsonPart = v
	}

	var result any
	if result, err = safeSearch(jsonPart, p.query); err != nil {
		p.log.Debugf("Failed to search json: %v\n", err)
		return nil, err
	}

	msg.SetStructuredMut(result)
	return []*message.Part{msg}, nil
}

func (p *jmespathProc) Close(context.Context) error {
	return nil
}
