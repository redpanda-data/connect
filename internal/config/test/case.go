package test

import (
	"github.com/benthosdev/benthos/v4/internal/docs"
)

const (
	fieldCaseName             = "name"
	fieldCaseEnvironment      = "environment"
	fieldCaseTargetProcessors = "target_processors"
	fieldCaseTargetMapping    = "target_mapping"
	fieldCaseMocks            = "mocks"
	fieldCaseInputBatch       = "input_batch"
	fieldCaseInputBatches     = "input_batches"
	fieldCaseOutputBatches    = "output_batches"
)

type Case struct {
	Name             string
	Environment      map[string]string
	TargetProcessors string
	TargetMapping    string
	Mocks            map[string]any
	InputBatches     [][]InputConfig
	OutputBatches    [][]OutputConditionsMap

	line int
}

func (c *Case) Line() int {
	return c.line
}

func caseFields() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString(fieldCaseName, "The name of the test, this should be unique and give a rough indication of what behaviour is being tested."),
		docs.FieldString(fieldCaseEnvironment, "An optional map of environment variables to set for the duration of the test.").Map().Optional(),
		docs.FieldString(fieldCaseTargetProcessors, `
A [JSON Pointer][json-pointer] that identifies the specific processors which should be executed by the test. The target can either be a single processor or an array of processors. Alternatively a resource label can be used to identify a processor.

It is also possible to target processors in a separate file by prefixing the target with a path relative to the test file followed by a # symbol.
`,
			"foo_processor",
			"/pipeline/processors/0",
			"target.yaml#/pipeline/processors",
			"target.yaml#/pipeline/processors",
		).HasDefault("/pipeline/processors"),
		docs.FieldString(fieldCaseTargetMapping,
			"A file path relative to the test definition path of a Bloblang file to execute as an alternative to testing processors with the `target_processors` field. This allows you to define unit tests for Bloblang mappings directly.",
		).HasDefault(""),
		docs.FieldAnything(fieldCaseMocks,
			"An optional map of processors to mock. Keys should contain either a label or a JSON pointer of a processor that should be mocked. Values should contain a processor definition, which will replace the mocked processor. Most of the time you'll want to use a [`mapping` processor][processors.mapping] here, and use it to create a result that emulates the target processor.",
			map[string]any{
				"get_foobar_api": map[string]any{
					"mapping": "root = content().string() + \" this is some mock content\"",
				},
			},
			map[string]any{
				"/pipeline/processors/1": map[string]any{
					"mapping": "root = content().string() + \" this is some mock content\"",
				},
			},
		).Map().Optional(),
		docs.FieldObject(fieldCaseInputBatch, "Define a batch of messages to feed into your test, specify either an `input_batch` or a series of `input_batches`.").
			Array().Optional().WithChildren(inputFields()...),
		docs.FieldObject(fieldCaseInputBatches, "Define a series of batches of messages to feed into your test, specify either an `input_batch` or a series of `input_batches`.").
			ArrayOfArrays().Optional().WithChildren(inputFields()...),
		docs.FieldObject(fieldCaseOutputBatches, "List of output batches.").
			ArrayOfArrays().Optional().WithChildren(outputFields()...),
	}
}

func CaseFromAny(v any) (Case, error) {
	pConf, err := caseFields().ParsedConfigFromAny(v)
	if err != nil {
		return Case{}, err
	}
	return CaseFromParsed(pConf)
}

func CaseFromParsed(pConf *docs.ParsedConfig) (c Case, err error) {
	c.line, _ = pConf.Line()
	if c.Name, err = pConf.FieldString(fieldCaseName); err != nil {
		return
	}
	if pConf.Contains(fieldCaseEnvironment) {
		if c.Environment, err = pConf.FieldStringMap(fieldCaseEnvironment); err != nil {
			return
		}
	}
	if c.TargetProcessors, err = pConf.FieldString(fieldCaseTargetProcessors); err != nil {
		return
	}
	if c.TargetMapping, err = pConf.FieldString(fieldCaseTargetMapping); err != nil {
		return
	}

	if pConf.Contains(fieldCaseMocks) {
		var tmpMocksAny map[string]*docs.ParsedConfig
		if tmpMocksAny, err = pConf.FieldAnyMap(fieldCaseMocks); err != nil {
			return
		}
		c.Mocks = map[string]any{}
		for k, v := range tmpMocksAny {
			if c.Mocks[k], err = v.FieldAny(); err != nil {
				return
			}
		}
	}

	if pConf.Contains(fieldCaseInputBatches) {
		var iBListOfList [][]*docs.ParsedConfig
		if iBListOfList, err = pConf.FieldObjectListOfLists(fieldCaseInputBatches); err != nil {
			return
		}
		for _, ol := range iBListOfList {
			tmpList := make([]InputConfig, len(ol))
			for i, il := range ol {
				if tmpList[i], err = InputFromParsed(il); err != nil {
					return
				}
			}
			c.InputBatches = append(c.InputBatches, tmpList)
		}
	}

	if pConf.Contains(fieldCaseInputBatch) {
		var iBList []*docs.ParsedConfig
		if iBList, err = pConf.FieldObjectList(fieldCaseInputBatch); err != nil {
			return
		}
		if len(iBList) > 0 {
			var tmpList []InputConfig
			for _, icp := range iBList {
				var inputTmp InputConfig
				if inputTmp, err = InputFromParsed(icp); err != nil {
					return
				}
				tmpList = append(tmpList, inputTmp)
			}
			c.InputBatches = append(c.InputBatches, tmpList)
		}
	}

	if pConf.Contains(fieldCaseOutputBatches) {
		var oBListOfList [][]*docs.ParsedConfig
		if oBListOfList, err = pConf.FieldObjectListOfLists(fieldCaseOutputBatches); err != nil {
			return
		}
		for _, ol := range oBListOfList {
			tmpList := make([]OutputConditionsMap, len(ol))
			for i, il := range ol {
				if tmpList[i], err = OutputConditionsFromParsed(il); err != nil {
					return
				}
			}
			c.OutputBatches = append(c.OutputBatches, tmpList)
		}
	}
	return
}
