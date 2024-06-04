package changelog

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func Test_Diff__shouldReturnDiff(t *testing.T) {
	cases := []diffArgs{
		{
			"should detect creation",
			nil,
			map[string]any{"summary": "a"},
			[]map[string]any{
				{"Type": "create", "Path": []string{"summary"}, "From": nil, "To": "a"},
			},
		},
		{
			"should detect creation of empty array",
			map[string]any{"summary": nil},
			map[string]any{"summary": []string{}},
			[]map[string]any{
				{"Type": "update", "Path": []string{"summary"}, "From": nil, "To": []string{}},
			},
		},
		{
			"should detect creation of pre-filled array",
			map[string]any{"summary": nil},
			map[string]any{"summary": []string{"a", "b"}},
			[]map[string]any{
				{"Type": "update", "Path": []string{"summary"}, "From": nil, "To": []string{"a", "b"}},
			},
		},
		{
			"should detect creation of empty object",
			map[string]any{"summary": nil},
			map[string]any{"summary": map[string]any{}},
			[]map[string]any{
				{"Type": "update", "Path": []string{"summary"}, "From": nil, "To": map[string]any{}},
			},
		},
		{
			"should detect creation of pre-filled object",
			map[string]any{"summary": nil},
			map[string]any{"summary": map[string]any{"a": "b"}},
			[]map[string]any{
				{"Type": "update", "Path": []string{"summary"}, "From": nil, "To": map[string]any{"a": "b"}},
			},
		},

		{
			"should detect change",
			map[string]any{"summary": "a"},
			map[string]any{"summary": "b"},
			[]map[string]any{
				{"Type": "update", "Path": []string{"summary"}, "From": "a", "To": "b"},
			},
		},
		{
			"should detect add to array",
			map[string]any{"summary": []string{"a"}},
			map[string]any{"summary": []string{"a", "b"}},
			[]map[string]any{
				{"Type": "create", "Path": []string{"summary", "1"}, "From": nil, "To": "b"},
			},
		},
		{
			"should detect remove from array",
			map[string]any{"summary": []string{"a", "b"}},
			map[string]any{"summary": []string{"a"}},
			[]map[string]any{
				{"Type": "delete", "Path": []string{"summary", "1"}, "From": "b", "To": nil},
			},
		},
		{
			"should detect add to object",
			map[string]any{"summary": map[string]any{"a": "b"}},
			map[string]any{"summary": map[string]any{"a": "b", "c": "d"}},
			[]map[string]any{
				{"Type": "create", "Path": []string{"summary", "c"}, "From": nil, "To": "d"},
			},
		},
		{
			"should detect remove from object",
			map[string]any{"summary": map[string]any{"a": "b", "c": "d"}},
			map[string]any{"summary": map[string]any{"a": "b"}},
			[]map[string]any{
				{"Type": "delete", "Path": []string{"summary", "c"}, "From": "d", "To": nil},
			},
		},

		{
			"should detect removal",
			map[string]any{"summary": "a"},
			nil,
			[]map[string]any{
				{"Type": "delete", "Path": []string{"summary"}, "From": "a", "To": nil},
			},
		},
	}

	for _, c := range cases {
		t.Run(c.Label, func(t *testing.T) {
			runDiff(t, c)
		})
	}
	// Output: {"new_summary":"meowquackwoof","reversed":["spuz","jen","olaf","pixie","denny"]}
}

type diffArgs struct {
	Label   string
	Before  map[string]any `json:"before"`
	After   map[string]any `json:"after"`
	Outcome any            `json:"outcome"`
}

func runDiff(t *testing.T, arg diffArgs) {
	mapping := `
root = this.before.diff(this.after)
`

	exe, err := bloblang.Parse(mapping)
	if err != nil {
		panic(err)
	}

	res, err := exe.Query(map[string]any{
		"before": arg.Before,
		"after":  arg.After,
	})
	if err != nil {
		panic(err)
	}

	jsonBytes, err := json.Marshal(res)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(jsonBytes))

	assert.Equal(t, arg.Outcome, res)
}

func Test_Patch(t *testing.T) {
	cases := []patchArgs{
		{
			"should patch creation",
			[]map[string]any{
				{"Type": "create", "Path": []string{"summary"}, "From": nil, "To": "a"},
			},
			map[string]any{},
			map[string]any{"summary": "a"},
		},
		{
			"should patch creation of empty array",
			[]map[string]any{
				{"Type": "update", "Path": []string{"summary"}, "From": nil, "To": []string{}},
			},
			map[string]any{"summary": nil},
			map[string]any{"summary": []string{}},
		},
		{
			"should patch creation of pre-filled array",
			[]map[string]any{
				{"Type": "update", "Path": []string{"summary"}, "From": nil, "To": []string{"a", "b"}},
			},
			map[string]any{"summary": nil},
			map[string]any{"summary": []string{"a", "b"}},
		},
		{
			"should patch creation of empty object",
			[]map[string]any{
				{"Type": "update", "Path": []string{"summary"}, "From": nil, "To": map[string]any{}},
			},
			map[string]any{"summary": nil},
			map[string]any{"summary": map[string]any{}},
		},
		{
			"should patch creation of pre-filled object",
			[]map[string]any{
				{"Type": "update", "Path": []string{"summary"}, "From": nil, "To": map[string]any{"a": "b"}},
			},
			map[string]any{"summary": nil},
			map[string]any{"summary": map[string]any{"a": "b"}},
		},
		{
			"should patch change",
			[]map[string]any{
				{"Type": "update", "Path": []string{"summary"}, "From": "a", "To": "b"},
			},
			map[string]any{"summary": "a"},
			map[string]any{"summary": "b"},
		},
		{
			"should patch add to object",
			[]map[string]any{
				{"Type": "create", "Path": []string{"summary", "c"}, "From": nil, "To": "d"},
			},
			map[string]any{"summary": map[string]any{"a": "b"}},
			map[string]any{"summary": map[string]any{"a": "b", "c": "d"}},
		},
		{
			"should patch remove from object",
			[]map[string]any{
				{"Type": "delete", "Path": []string{"summary", "c"}, "From": "d", "To": nil},
			},
			map[string]any{"summary": map[string]any{"a": "b", "c": "d"}},
			map[string]any{"summary": map[string]any{"a": "b"}},
		},

		{
			"should patch removal",
			[]map[string]any{
				{"Type": "delete", "Path": []string{"summary"}, "From": "a", "To": nil},
			},
			map[string]any{"summary": "a"},
			map[string]any{},
		},
	}

	for _, c := range cases {
		t.Run(c.Label, func(t *testing.T) {
			runPatch(t, c)
		})
	}
}

type patchArgs struct {
	Label     string
	Changelog []map[string]any
	Input     map[string]any
	Expected  map[string]any
}

func runPatch(t *testing.T, arg patchArgs) {
	mapping := `
root = this.input.patch(this.changelog)
`

	exe, err := bloblang.Parse(mapping)
	if err != nil {
		panic(err)
	}

	res, err := exe.Query(map[string]any{
		"input":     arg.Input,
		"changelog": arg.Changelog,
	})
	if err != nil {
		panic(err)
	}

	jsonBytes, err := json.Marshal(res)
	if err != nil {
		panic(err)
	}

	fmt.Println(string(jsonBytes))

	assert.Equal(t, arg.Expected, res)
}
