package test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"sort"

	"github.com/fatih/color"
	"github.com/nsf/jsondiff"

	"github.com/benthosdev/benthos/v4/internal/bloblang"
	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/value"
)

var (
	red  = color.New(color.FgRed).SprintFunc()
	blue = color.New(color.FgBlue).SprintFunc()
)

const (
	fieldOutputBloblang         = "bloblang"
	fieldOutputContentEquals    = "content_equals"
	fieldOutputContentMatches   = "content_matches"
	fieldOutputMetadataEquals   = "metadata_equals"
	fieldOutputFileEquals       = "file_equals"
	fieldOutputFileJSONEquals   = "file_json_equals"
	fieldOutputFileJSONContains = "file_json_contains"
	fieldOutputJSONEquals       = "json_equals"
	fieldOutputJSONContains     = "json_contains"
)

func outputFields() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldBloblang(fieldOutputBloblang, "Executes a Bloblang mapping on the output message, if the result is anything other than a boolean equalling `true` the test fails.",
			"this.age > 10 && @foo.length() > 0",
		).Optional(),
		docs.FieldString(fieldOutputContentEquals, "Checks the full raw contents of a message against a value.").Optional(),
		docs.FieldString(fieldOutputContentMatches, "Checks whether the full raw contents of a message matches a regular expression (re2).", "^foo [a-z]+ bar$").Optional(),
		docs.FieldAnything(fieldOutputMetadataEquals, "Checks a map of metadata keys to values against the metadata stored in the message. If there is a value mismatch between a key of the condition versus the message metadata this condition will fail.",
			map[string]any{
				"example_key": "example metadata value",
			},
		).Map().Optional(),
		docs.FieldString(fieldOutputFileEquals, "Checks that the contents of a message matches the contents of a file. The path of the file should be relative to the path of the test file.",
			"./foo/bar.txt",
		).Optional(),
		docs.FieldString(fieldOutputFileJSONEquals, "Checks that both the message and the file contents are valid JSON documents, and that they are structurally equivalent. Will ignore formatting and ordering differences. The path of the file should be relative to the path of the test file.",
			"./foo/bar.json",
		).Optional(),
		docs.FieldAnything(fieldOutputJSONEquals, "Checks that both the message and the condition are valid JSON documents, and that they are structurally equivalent. Will ignore formatting and ordering differences.",
			map[string]any{"key": "value"},
		).Optional(),
		docs.FieldAnything(fieldOutputJSONContains, "Checks that both the message and the condition are valid JSON documents, and that the message is a superset of the condition.",
			map[string]any{"key": "value"},
		).Optional(),
		docs.FieldString(fieldOutputFileJSONContains, "Checks that both the message and the file contents are valid JSON documents, and that the message is a superset of the condition. Will ignore formatting and ordering differences. The path of the file should be relative to the path of the test file.",
			"./foo/bar.json",
		).Optional(),
	}
}

type OutputCondition interface {
	Check(fs fs.FS, dir string, part *message.Part) error
}

type OutputConditionsMap map[string]OutputCondition

func (c OutputConditionsMap) CheckAll(fs fs.FS, dir string, part *message.Part) (errs []error) {
	condTypes := []string{}
	for k := range c {
		condTypes = append(condTypes, k)
	}
	sort.Strings(condTypes)
	for _, k := range condTypes {
		if err := c[k].Check(fs, dir, part); err != nil {
			errs = append(errs, fmt.Errorf("%v: %v", k, err))
		}
	}
	return
}

func OutputConditionsFromParsed(pConf *docs.ParsedConfig) (m OutputConditionsMap, err error) {
	m = OutputConditionsMap{}
	if pConf.Contains(fieldOutputBloblang) {
		var tmpStr string
		if tmpStr, err = pConf.FieldString(fieldOutputBloblang); err != nil {
			return
		}
		var bloblCond *BloblangCondition
		if bloblCond, err = parseBloblangCondition(tmpStr); err != nil {
			err = fmt.Errorf(fieldOutputBloblang+": %w", err)
			return
		}
		m[fieldOutputBloblang] = bloblCond
	}

	if pConf.Contains(fieldOutputContentEquals) {
		var tmpStr string
		if tmpStr, err = pConf.FieldString(fieldOutputContentEquals); err != nil {
			return
		}
		m[fieldOutputContentEquals] = ContentEqualsCondition(tmpStr)
	}

	if pConf.Contains(fieldOutputContentMatches) {
		var tmpStr string
		if tmpStr, err = pConf.FieldString(fieldOutputContentMatches); err != nil {
			return
		}
		m[fieldOutputContentMatches] = ContentMatchesCondition(tmpStr)
	}

	if pConf.Contains(fieldOutputMetadataEquals) {
		var tmpMap map[string]*docs.ParsedConfig
		if tmpMap, err = pConf.FieldAnyMap(fieldOutputMetadataEquals); err != nil {
			return
		}
		metaMap := MetadataEqualsCondition{}
		for k, v := range tmpMap {
			if metaMap[k], err = v.FieldAny(); err != nil {
				return
			}
		}
		m[fieldOutputMetadataEquals] = metaMap
	}

	if pConf.Contains(fieldOutputFileEquals) {
		var tmpStr string
		if tmpStr, err = pConf.FieldString(fieldOutputFileEquals); err != nil {
			return
		}
		m[fieldOutputFileEquals] = FileEqualsCondition(tmpStr)
	}

	if pConf.Contains(fieldOutputFileJSONEquals) {
		var tmpStr string
		if tmpStr, err = pConf.FieldString(fieldOutputFileJSONEquals); err != nil {
			return
		}
		m[fieldOutputFileJSONEquals] = FileJSONEqualsCondition(tmpStr)
	}

	if pConf.Contains(fieldOutputFileJSONContains) {
		var tmpStr string
		if tmpStr, err = pConf.FieldString(fieldOutputFileJSONContains); err != nil {
			return
		}
		m[fieldOutputFileJSONContains] = FileJSONContainsCondition(tmpStr)
	}

	if pConf.Contains(fieldOutputJSONEquals) {
		var tmpAny any
		if tmpAny, err = pConf.FieldAny(fieldOutputJSONEquals); err != nil {
			return
		}
		var tmpStr string
		if tmpStr, err = anyValueToJSONTestString(tmpAny); err != nil {
			return
		}
		m[fieldOutputJSONEquals] = ContentJSONEqualsCondition(tmpStr)
	}

	if pConf.Contains(fieldOutputJSONContains) {
		var tmpAny any
		if tmpAny, err = pConf.FieldAny(fieldOutputJSONContains); err != nil {
			return
		}
		var tmpStr string
		if tmpStr, err = anyValueToJSONTestString(tmpAny); err != nil {
			return
		}
		m[fieldOutputJSONContains] = ContentJSONContainsCondition(tmpStr)
	}
	return
}

type BloblangCondition struct {
	m *mapping.Executor
}

func parseBloblangCondition(expr string) (*BloblangCondition, error) {
	m, err := bloblang.GlobalEnvironment().NewMapping(expr)
	if err != nil {
		return nil, err
	}
	return &BloblangCondition{m}, nil
}

func (b *BloblangCondition) Check(fs fs.FS, dir string, p *message.Part) error {
	msg := message.Batch{p}
	res, err := b.m.QueryPart(0, msg)
	if err != nil {
		return err
	}
	if !res {
		return errors.New("bloblang expression was false")
	}
	return nil
}

type ContentEqualsCondition string

func (c ContentEqualsCondition) Check(fs fs.FS, dir string, p *message.Part) error {
	if exp, act := string(c), string(p.AsBytes()); exp != act {
		return fmt.Errorf("content mismatch\n  expected: %v\n  received: %v", blue(exp), red(act))
	}
	return nil
}

type ContentMatchesCondition string

func (c ContentMatchesCondition) Check(fs fs.FS, dir string, p *message.Part) error {
	re := regexp.MustCompile(string(c))
	if !re.Match(p.AsBytes()) {
		return fmt.Errorf("pattern mismatch\n   pattern: %v\n  received: %v", blue(string(c)), red(string(p.AsBytes())))
	}
	return nil
}

type ContentJSONEqualsCondition string

func (c ContentJSONEqualsCondition) Check(fs fs.FS, dir string, p *message.Part) error {
	jdopts := jsondiff.DefaultConsoleOptions()
	diff, explanation := jsondiff.Compare(p.AsBytes(), []byte(c), &jdopts)
	if diff != jsondiff.FullMatch {
		return fmt.Errorf("JSON content mismatch\n%v", explanation)
	}
	return nil
}

type ContentJSONContainsCondition string

func (c ContentJSONContainsCondition) Check(fs fs.FS, dir string, p *message.Part) error {
	jdopts := jsondiff.DefaultConsoleOptions()
	diff, explanation := jsondiff.Compare(p.AsBytes(), []byte(c), &jdopts)
	if diff != jsondiff.FullMatch && diff != jsondiff.SupersetMatch {
		return fmt.Errorf("JSON superset mismatch\n%v", explanation)
	}
	return nil
}

type FileEqualsCondition string

func (c FileEqualsCondition) Check(fs fs.FS, dir string, p *message.Part) error {
	relPath := filepath.Join(dir, string(c))

	fileContent, err := ifs.ReadFile(fs, relPath)
	if err != nil {
		return fmt.Errorf("failed to read comparison file: %w", err)
	}

	if exp, act := string(fileContent), string(p.AsBytes()); exp != act {
		return fmt.Errorf("content mismatch\n  expected: %v\n  received: %v", blue(exp), red(act))
	}
	return nil
}

type FileJSONEqualsCondition string

func (c FileJSONEqualsCondition) Check(fs fs.FS, dir string, p *message.Part) error {
	relPath := filepath.Join(dir, string(c))

	fileContent, err := ifs.ReadFile(fs, relPath)
	if err != nil {
		return fmt.Errorf("failed to read comparison JSON file: %w", err)
	}

	comparison := ContentJSONEqualsCondition(fileContent)
	return comparison.Check(fs, dir, p)
}

type FileJSONContainsCondition string

func (c FileJSONContainsCondition) Check(fs fs.FS, dir string, p *message.Part) error {
	relPath := filepath.Join(dir, string(c))

	fileContent, err := ifs.ReadFile(fs, relPath)
	if err != nil {
		return fmt.Errorf("failed to read comparison JSON file: %w", err)
	}

	comparison := ContentJSONContainsCondition(fileContent)
	return comparison.Check(fs, dir, p)
}

type MetadataEqualsCondition map[string]any

func (m MetadataEqualsCondition) Check(fs fs.FS, dir string, p *message.Part) error {
	for k, exp := range m {
		act, exists := p.MetaGetMut(k)
		if !exists {
			return fmt.Errorf("metadata key '%v' expected but not found", k)
		}
		if !value.ICompare(exp, act) {
			return fmt.Errorf("metadata key '%v' mismatch\n  expected: %v\n  received: %v", k, blue(exp), red(act))
		}
	}
	return nil
}

func anyValueToJSONTestString(v any) (string, error) {
	if str, ok := v.(string); ok {
		return str, nil
	}
	bval, err := json.Marshal(v)
	return bytes.NewBuffer(bval).String(), err
}
