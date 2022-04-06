package main

import (
	"fmt"

	"cuelang.org/go/cue/format"
	"github.com/benthosdev/benthos/v4/internal/cuegen"
	_ "github.com/benthosdev/benthos/v4/public/components/all"
)

func main() {
	cueAST, err := cuegen.GenerateSchemaAST()
	if err != nil {
		panic(err)
	}

	source, err := format.Node(cueAST)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", source)
}
