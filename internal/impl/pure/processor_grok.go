package pure

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Jeffail/gabs/v2"
	"github.com/Jeffail/grok"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/filepath"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	gpFieldExpressions        = "expressions"
	gpFieldRemoveEmpty        = "remove_empty_values"
	gpFieldNamedOnly          = "named_captures_only"
	gpFieldUseDefaults        = "use_default_patterns"
	gpFieldPatternPaths       = "pattern_paths"
	gpFieldPatternDefinitions = "pattern_definitions"
)

func grokProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Parsing").
		Stable().
		Summary("Parses messages into a structured format by attempting to apply a list of Grok expressions, the first expression to result in at least one value replaces the original message with a JSON object containing the values.").
		Description(`
Type hints within patterns are respected, therefore with the pattern `+"`%{WORD:first},%{INT:second:int}`"+` and a payload of `+"`foo,1`"+` the resulting payload would be `+"`{\"first\":\"foo\",\"second\":1}`"+`.

### Performance

This processor currently uses the [Go RE2](https://golang.org/s/re2syntax) regular expression engine, which is guaranteed to run in time linear to the size of the input. However, this property often makes it less performant than PCRE based implementations of grok. For more information see [https://swtch.com/~rsc/regexp/regexp1.html](https://swtch.com/~rsc/regexp/regexp1.html).`).
		Footnotes(`
## Default Patterns

A summary of the default patterns on offer can be [found here](https://github.com/Jeffail/grok/blob/master/patterns.go#L5).`).
		Example("VPC Flow Logs", `
Grok can be used to parse unstructured logs such as VPC flow logs that look like this:

`+"```text"+`
2 123456789010 eni-1235b8ca123456789 172.31.16.139 172.31.16.21 20641 22 6 20 4249 1418530010 1418530070 ACCEPT OK
`+"```"+`

Into structured objects that look like this:

`+"```json"+`
{"accountid":"123456789010","action":"ACCEPT","bytes":4249,"dstaddr":"172.31.16.21","dstport":22,"end":1418530070,"interfaceid":"eni-1235b8ca123456789","logstatus":"OK","packets":20,"protocol":6,"srcaddr":"172.31.16.139","srcport":20641,"start":1418530010,"version":2}
`+"```"+`

With the following config:`,
			`
pipeline:
  processors:
    - grok:
        expressions:
          - '%{VPCFLOWLOG}'
        pattern_definitions:
          VPCFLOWLOG: '%{NUMBER:version:int} %{NUMBER:accountid} %{NOTSPACE:interfaceid} %{NOTSPACE:srcaddr} %{NOTSPACE:dstaddr} %{NOTSPACE:srcport:int} %{NOTSPACE:dstport:int} %{NOTSPACE:protocol:int} %{NOTSPACE:packets:int} %{NOTSPACE:bytes:int} %{NUMBER:start:int} %{NUMBER:end:int} %{NOTSPACE:action} %{NOTSPACE:logstatus}'
`,
		).
		Fields(
			service.NewStringListField(gpFieldExpressions).
				Description("One or more Grok expressions to attempt against incoming messages. The first expression to match at least one value will be used to form a result."),
			service.NewStringMapField(gpFieldPatternDefinitions).
				Description("A map of pattern definitions that can be referenced within `patterns`.").
				Default(map[string]any{}),
			service.NewStringListField(gpFieldPatternPaths).
				Description("A list of paths to load Grok patterns from. This field supports wildcards, including super globs (double star).").
				Default([]any{}),
			service.NewBoolField(gpFieldNamedOnly).
				Description("Whether to only capture values from named patterns.").
				Advanced().
				Default(true),
			service.NewBoolField(gpFieldUseDefaults).
				Description("Whether to use a [default set of patterns](#default-patterns).").
				Advanced().
				Default(true),
			service.NewBoolField(gpFieldRemoveEmpty).
				Description("Whether to remove values that are empty from the resulting structure.").
				Advanced().
				Default(true),
		)
}

type grokProcConfig struct {
	Expressions        []string
	RemoveEmpty        bool
	NamedOnly          bool
	UseDefaults        bool
	PatternPaths       []string
	PatternDefinitions map[string]string
}

func init() {
	err := service.RegisterBatchProcessor(
		"grok", grokProcSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			var g grokProcConfig
			var err error

			if g.Expressions, err = conf.FieldStringList(gpFieldExpressions); err != nil {
				return nil, err
			}
			if g.PatternDefinitions, err = conf.FieldStringMap(gpFieldPatternDefinitions); err != nil {
				return nil, err
			}
			if g.PatternPaths, err = conf.FieldStringList(gpFieldPatternPaths); err != nil {
				return nil, err
			}

			if g.RemoveEmpty, err = conf.FieldBool(gpFieldRemoveEmpty); err != nil {
				return nil, err
			}
			if g.NamedOnly, err = conf.FieldBool(gpFieldNamedOnly); err != nil {
				return nil, err
			}
			if g.UseDefaults, err = conf.FieldBool(gpFieldUseDefaults); err != nil {
				return nil, err
			}

			mgr := interop.UnwrapManagement(res)
			p, err := newGrok(g, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(processor.NewAutoObservedProcessor("grok", p, mgr)), nil
		})
	if err != nil {
		panic(err)
	}
}

type grokProc struct {
	gparsers []*grok.CompiledGrok
	log      log.Modular
}

func newGrok(conf grokProcConfig, mgr bundle.NewManagement) (processor.AutoObserved, error) {
	grokConf := grok.Config{
		RemoveEmptyValues:   conf.RemoveEmpty,
		NamedCapturesOnly:   conf.NamedOnly,
		SkipDefaultPatterns: !conf.UseDefaults,
		Patterns:            conf.PatternDefinitions,
	}

	for _, path := range conf.PatternPaths {
		if err := addGrokPatternsFromPath(mgr.FS(), path, grokConf.Patterns); err != nil {
			return nil, fmt.Errorf("failed to parse patterns from path '%v': %v", path, err)
		}
	}

	gcompiler, err := grok.New(grokConf)
	if err != nil {
		return nil, fmt.Errorf("failed to create grok compiler: %v", err)
	}

	var compiled []*grok.CompiledGrok
	for _, pattern := range conf.Expressions {
		var gcompiled *grok.CompiledGrok
		if gcompiled, err = gcompiler.Compile(pattern); err != nil {
			return nil, fmt.Errorf("failed to compile Grok pattern '%v': %v", pattern, err)
		}
		compiled = append(compiled, gcompiled)
	}

	g := &grokProc{
		gparsers: compiled,
		log:      mgr.Logger(),
	}
	return g, nil
}

func addGrokPatternsFromPath(fs ifs.FS, path string, patterns map[string]string) error {
	if s, err := fs.Stat(path); err != nil {
		return err
	} else if s.IsDir() {
		path += "/*"
	}

	files, err := filepath.Globs(fs, []string{path})
	if err != nil {
		return err
	}

	for _, f := range files {
		file, err := fs.Open(f)
		if err != nil {
			return err
		}

		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			l := scanner.Text()
			if l != "" && l[0] != '#' {
				names := strings.SplitN(l, " ", 2)
				patterns[names[0]] = names[1]
			}
		}

		file.Close()
	}

	return nil
}

func (g *grokProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	body := msg.AsBytes()

	var values map[string]any
	for _, compiler := range g.gparsers {
		var err error
		if values, err = compiler.ParseTyped(body); err != nil {
			g.log.Debug("Failed to parse body: %v\n", err)
			continue
		}
		if len(values) > 0 {
			break
		}
	}
	if len(values) == 0 {
		g.log.Debug("No matches found for payload: %s\n", body)
		return nil, errors.New("no pattern matches found")
	}

	gObj := gabs.New()
	for k, v := range values {
		_, _ = gObj.SetP(v, k)
	}

	msg.SetStructuredMut(gObj.Data())
	return []*message.Part{msg}, nil
}

func (g *grokProc) Close(context.Context) error {
	return nil
}
