package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/urfave/cli/v2"
)

//------------------------------------------------------------------------------

func listableStatus(s docs.Status) bool {
	switch s {
	case "": // Empty status is the equivalent of stable.
		return true
	case docs.StatusStable:
		return true
	case docs.StatusBeta:
		return true
	}
	return false
}

func listComponents(c *cli.Context) {
	jsonFmt := c.String("format") == "json"

	ofType := c.Args().Slice()
	whitelist := map[string]struct{}{}
	for _, k := range ofType {
		whitelist[k] = struct{}{}
	}

	var buf bytes.Buffer
	obj := map[string]interface{}{}

	components := []string{}
	printAll := func(title string) {
		typeStr := strings.ReplaceAll(strings.ToLower(title), " ", "-")
		if _, exists := whitelist[typeStr]; len(ofType) > 0 && !exists {
			components = nil
			return
		}
		sort.Strings(components)
		if jsonFmt {
			cCopy := make([]string, len(components))
			copy(cCopy, components)
			obj[typeStr] = cCopy
		} else {
			if buf.Len() > 0 {
				fmt.Fprintln(&buf, "")
			}
			fmt.Fprintf(&buf, "%v:\n", title)
			for _, t := range components {
				fmt.Fprintf(&buf, "  - %v\n", t)
			}
		}
		components = nil
	}
	defer func() {
		if jsonFmt {
			b, err := json.Marshal(obj)
			if err != nil {
				panic(err)
			}
			fmt.Println(string(b))
		} else {
			fmt.Print(buf.String())
		}
	}()

	for _, c := range bundle.AllInputs.Docs() {
		if listableStatus(c.Status) {
			components = append(components, c.Name)
		}
	}
	printAll("Inputs")

	for _, c := range bundle.AllProcessors.Docs() {
		if listableStatus(c.Status) {
			components = append(components, c.Name)
		}
	}
	printAll("Processors")

	for t := range condition.Constructors {
		components = append(components, t)
	}
	printAll("Conditions")

	for _, c := range bundle.AllOutputs.Docs() {
		if listableStatus(c.Status) {
			components = append(components, c.Name)
		}
	}
	printAll("Outputs")

	for _, c := range bundle.AllCaches.Docs() {
		if listableStatus(c.Status) {
			components = append(components, c.Name)
		}
	}
	printAll("Caches")

	for _, c := range bundle.AllRateLimits.Docs() {
		if listableStatus(c.Status) {
			components = append(components, c.Name)
		}
	}
	printAll("Rate Limits")

	for _, c := range bundle.AllBuffers.Docs() {
		if listableStatus(c.Status) {
			components = append(components, c.Name)
		}
	}
	printAll("Buffers")

	for _, c := range bundle.AllMetrics.Docs() {
		if listableStatus(c.Status) {
			components = append(components, c.Name)
		}
	}
	printAll("Metrics")

	for _, c := range bundle.AllTracers.Docs() {
		if listableStatus(c.Status) {
			components = append(components, c.Name)
		}
	}
	printAll("Tracers")

	components = query.ListFunctions()
	printAll("Bloblang Functions")

	components = query.ListMethods()
	printAll("Bloblang Methods")
}

//------------------------------------------------------------------------------
