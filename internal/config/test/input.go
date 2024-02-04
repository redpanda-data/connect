package test

import (
	"encoding/json"
	"io/fs"
	"path/filepath"

	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/message"
)

const (
	fieldInputContent     = "content"
	fieldInputJSONContent = "json_content"
	fieldInputFileContent = "file_content"
	fieldInputMetadata    = "metadata"
)

func inputFields() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString(fieldInputContent, "The raw content of the input message.").Optional(),
		docs.FieldAnything(fieldInputJSONContent, "Sets the raw content of the message to a JSON document matching the structure of the value.",
			map[string]any{
				"foo": "foo value",
				"bar": []any{"element1", 10},
			},
		).Optional(),
		docs.FieldString(fieldInputFileContent, "Sets the raw content of the message by reading a file. The path of the file should be relative to the path of the test file.", "./foo/bar.txt").Optional(),
		docs.FieldAnything(fieldInputMetadata, "A map of metadata key/values to add to the input message.").Map().Optional(),
	}
}

type InputConfig struct {
	Content  string
	Path     string
	Metadata map[string]any
}

func (i InputConfig) ToMessage(fs fs.FS, dir string) (*message.Part, error) {
	msgContent := []byte(i.Content)
	if i.Path != "" {
		relPath := filepath.Join(dir, i.Path)
		rawBytes, err := ifs.ReadFile(fs, relPath)
		if err != nil {
			return nil, err
		}
		msgContent = rawBytes
	}

	msg := message.NewPart(msgContent)
	for k, v := range i.Metadata {
		msg.MetaSetMut(k, v)
	}
	return msg, nil
}

func InputFromParsed(pConf *docs.ParsedConfig) (conf InputConfig, err error) {
	if pConf.Contains(fieldInputContent) {
		if conf.Content, err = pConf.FieldString(fieldInputContent); err != nil {
			return
		}
	}
	if pConf.Contains(fieldInputJSONContent) {
		var v any
		if v, err = pConf.FieldAny(fieldInputJSONContent); err != nil {
			return
		}
		var jBytes []byte
		if jBytes, err = json.Marshal(v); err != nil {
			return
		}
		conf.Content = string(jBytes)
	}
	if pConf.Contains(fieldInputFileContent) {
		if conf.Path, err = pConf.FieldString(fieldInputFileContent); err != nil {
			return
		}
	}
	if pConf.Contains(fieldInputMetadata) {
		conf.Metadata = map[string]any{}
		var tmpMap map[string]*docs.ParsedConfig
		if tmpMap, err = pConf.FieldAnyMap(fieldInputMetadata); err != nil {
			return
		}
		for k, v := range tmpMap {
			if conf.Metadata[k], err = v.FieldAny(); err != nil {
				return
			}
		}
	}
	return
}
