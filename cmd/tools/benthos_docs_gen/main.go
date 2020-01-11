package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"path"
	"sort"

	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/util/config"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func create(t, path string, resBytes []byte) {
	if err := ioutil.WriteFile(path, resBytes, 0644); err != nil {
		panic(err)
	}
	fmt.Printf("Generated '%v' doc at: %v\n", t, path)
}

func render(dir string, confSanit interface{}, spec docs.ComponentSpec) {
	var segment interface{}
	switch t := confSanit.(type) {
	case map[string]interface{}:
		segment = t[spec.Name]
	case config.Sanitised:
		segment = t[spec.Name]
	default:
		panic(fmt.Sprintf("Failed to generate docs for '%v': sanitised config wrong type: %T", spec.Name, confSanit))
	}

	mdSpec, err := spec.AsMarkdown(segment)
	if err != nil {
		panic(fmt.Sprintf("Failed to generate docs for '%v': %v", spec.Name, err))
	}

	create(spec.Name, dir, mdSpec)
}

func main() {
	docsDir := "./website/docs"
	flag.StringVar(&docsDir, "dir", docsDir, "The directory to write docs to")
	flag.Parse()

	links := struct {
		Inputs []string
	}{}

	inputs := []string{}
	for k := range input.Constructors {
		inputs = append(inputs, k)
	}
	sort.Strings(inputs)
	for _, k := range inputs {
		v := input.Constructors[k]
		if v.Deprecated {
			continue
		}
		spec := docs.ComponentSpec{
			Type:        "input",
			Name:        k,
			Description: v.Description,
			Fields:      v.FieldSpecs,
		}

		conf := input.NewConfig()
		conf.Type = k
		confSanit, err := input.SanitiseConfig(conf)
		if err != nil {
			panic(fmt.Sprintf("Failed to generate docs for '%v': %v", k, err))
		}

		render(path.Join(docsDir, "./inputs", k+".md"), confSanit, spec)
		links.Inputs = append(links.Inputs, "inputs/"+k+".md")
	}
}

//------------------------------------------------------------------------------
