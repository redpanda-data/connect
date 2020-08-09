package mapping

import (
	"errors"
	"testing"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssignments(t *testing.T) {
	type part struct {
		Content string
		Meta    map[string]string
	}

	metaKey := func(k string) *string {
		return &k
	}

	initFunc := func(name string, args ...interface{}) query.Function {
		t.Helper()
		fn, err := query.InitFunction(name, args...)
		require.NoError(t, err)
		return fn
	}

	tests := map[string]struct {
		index   int
		input   []part
		mapping *Executor
		output  *part
		err     error
	}{
		"simple json map": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment("foo"), query.NewFieldFunction("bar")),
				NewStatement(nil, NewJSONAssignment("bar"), query.NewLiteralFunction("test2")),
				NewStatement(nil, NewJSONAssignment("zed"), query.NewLiteralFunction(query.Delete(nil))),
			),
			input:  []part{{Content: `{"bar":"test1","zed":"gone"}`}},
			output: &part{Content: `{"bar":"test2","foo":"test1"}`},
		},
		"map to root": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment(), query.NewLiteralFunction("bar")),
			),
			input:  []part{{Content: `{"bar":"test1","zed":"gone"}`}},
			output: &part{Content: `bar`},
		},
		"delete root": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment(), query.NewLiteralFunction(query.Delete(nil))),
			),
			input:  []part{{Content: `{"bar":"test1","zed":"gone"}`}},
			output: nil,
		},
		"no mapping to root": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment(), query.NewLiteralFunction(query.Nothing(nil))),
			),
			input:  []part{{Content: `{"bar":"test1","zed":"gone"}`}},
			output: &part{Content: `{"bar":"test1","zed":"gone"}`},
		},
		"variable error DNE": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment("foo"), query.NewVarFunction("doesnt exist")),
			),
			input: []part{{Content: `{}`}},
			err:   errors.New("failed to execute mapping query at line 0: variable 'doesnt exist' undefined"),
		},
		"variable assignment": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewVarAssignment("foo"), query.NewLiteralFunction("does exist")),
				NewStatement(nil, NewJSONAssignment("foo"), query.NewVarFunction("foo")),
			),
			input:  []part{{Content: `{}`}},
			output: &part{Content: `{"foo":"does exist"}`},
		},
		"meta query error": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewJSONAssignment("foo"), initFunc("meta", "foo")),
			),
			input: []part{{Content: `{}`}},
			err:   errors.New("failed to execute mapping query at line 0: metadata value not found"),
		},
		"meta assignment": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewMetaAssignment(metaKey("foo")), query.NewLiteralFunction("exists now")),
			),
			input: []part{{Content: `{}`}},
			output: &part{
				Content: `{}`,
				Meta: map[string]string{
					"foo": "exists now",
				},
			},
		},
		"meta deletion": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewMetaAssignment(metaKey("and")), query.NewLiteralFunction(query.Delete(nil))),
			),
			input: []part{{
				Content: `{}`,
				Meta: map[string]string{
					"ignore": "me",
					"and":    "delete me",
				},
			}},
			output: &part{
				Content: `{}`,
				Meta: map[string]string{
					"ignore": "me",
				},
			},
		},
		"meta set all error wrong type": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewMetaAssignment(nil), query.NewLiteralFunction("foo")),
			),
			input: []part{{Content: `{}`}},
			err:   errors.New("failed to assign query result at line 0: setting root meta object requires object value, received: string"),
		},
		"meta set all": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewMetaAssignment(nil), query.NewLiteralFunction(map[string]interface{}{
					"new1": "value1",
					"new2": "value2",
				})),
			),
			input: []part{{
				Content: `{}`,
				Meta: map[string]string{
					"foo": "first",
					"bar": "second",
				},
			}},
			output: &part{
				Content: `{}`,
				Meta: map[string]string{
					"new1": "value1",
					"new2": "value2",
				},
			},
		},
		"meta delete all": {
			mapping: NewExecutor(nil, nil,
				NewStatement(nil, NewMetaAssignment(nil), query.NewLiteralFunction(query.Delete(nil))),
			),
			input: []part{{
				Content: `{}`,
				Meta: map[string]string{
					"foo": "first",
					"bar": "second",
				},
			}},
			output: &part{Content: `{}`},
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			msg := message.New(nil)
			for _, p := range test.input {
				part := message.NewPart([]byte(p.Content))
				for k, v := range p.Meta {
					part.Metadata().Set(k, v)
				}
				msg.Append(part)
			}

			resPart, err := test.mapping.MapPart(test.index, msg)
			if test.err != nil {
				assert.EqualError(t, err, test.err.Error())
				return
			}

			require.NoError(t, err)

			if test.output != nil {
				if test.output.Meta == nil {
					test.output.Meta = map[string]string{}
				}

				newPart := part{
					Content: string(resPart.Get()),
					Meta:    map[string]string{},
				}
				resPart.Metadata().Iter(func(k, v string) error {
					newPart.Meta[k] = v
					return nil
				})

				assert.Equal(t, *test.output, newPart)
			} else {
				assert.Nil(t, resPart)
			}
		})
	}
}
