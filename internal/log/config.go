package log

import "github.com/benthosdev/benthos/v4/internal/docs"

const (
	fieldLogLevel         = "level"
	fieldFormat           = "format"
	fieldAddTimeStamp     = "add_timestamp"
	fieldLevelName        = "level_name"
	fieldMessageName      = "message_name"
	fieldTimestampName    = "timestamp_name"
	fieldStaticFields     = "static_fields"
	fieldFile             = "file"
	fieldFilePath         = "path"
	fieldFileRotate       = "rotate"
	fieldFileRotateMaxAge = "rotate_max_age_days"
)

// Config holds configuration options for a logger object.
type Config struct {
	LogLevel      string            `json:"level" yaml:"level"`
	Format        string            `json:"format" yaml:"format"`
	AddTimeStamp  bool              `json:"add_timestamp" yaml:"add_timestamp"`
	LevelName     string            `json:"level_name" yaml:"level_name"`
	MessageName   string            `json:"message_name" yaml:"message_name"`
	TimestampName string            `json:"timestamp_name" yaml:"timestamp_name"`
	StaticFields  map[string]string `json:"static_fields" yaml:"static_fields"`
	File          File              `json:"file" yaml:"file"`
}

// File contains configuration for file based logging.
type File struct {
	Path         string `json:"path" yaml:"path"`
	Rotate       bool   `json:"rotate" yaml:"rotate"`
	RotateMaxAge int    `json:"rotate_max_age_days" yaml:"rotate_max_age_days"`
}

// NewConfig returns a config struct with the default values for each field.
func NewConfig() Config {
	return Config{
		LogLevel:      "INFO",
		Format:        "logfmt",
		AddTimeStamp:  false,
		LevelName:     "level",
		TimestampName: "time",
		MessageName:   "msg",
		StaticFields: map[string]string{
			"@service": "benthos",
		},
	}
}

// UnmarshalYAML ensures that when parsing configs that are in a slice the
// default values are still applied.
func (conf *Config) UnmarshalYAML(unmarshal func(any) error) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	defaultFields := aliased.StaticFields
	aliased.StaticFields = nil

	if err := unmarshal(&aliased); err != nil {
		return err
	}

	if aliased.StaticFields == nil {
		aliased.StaticFields = defaultFields
	}

	*conf = Config(aliased)
	return nil
}

func FromParsed(pConf *docs.ParsedConfig) (conf Config, err error) {
	if conf.LogLevel, err = pConf.FieldString(fieldLogLevel); err != nil {
		return
	}
	if conf.Format, err = pConf.FieldString(fieldFormat); err != nil {
		return
	}
	if conf.AddTimeStamp, err = pConf.FieldBool(fieldAddTimeStamp); err != nil {
		return
	}
	if conf.LevelName, err = pConf.FieldString(fieldLevelName); err != nil {
		return
	}
	if conf.MessageName, err = pConf.FieldString(fieldMessageName); err != nil {
		return
	}
	if conf.TimestampName, err = pConf.FieldString(fieldTimestampName); err != nil {
		return
	}
	if conf.StaticFields, err = pConf.FieldStringMap(fieldStaticFields); err != nil {
		return
	}

	{
		fConf := pConf.Namespace(fieldFile)
		if conf.File.Path, err = fConf.FieldString(fieldFilePath); err != nil {
			return
		}
		if conf.File.Rotate, err = fConf.FieldBool(fieldFileRotate); err != nil {
			return
		}
		if conf.File.RotateMaxAge, err = fConf.FieldInt(fieldFileRotateMaxAge); err != nil {
			return
		}
	}
	return
}
