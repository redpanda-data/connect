package processor

// KVConfig is a configuration struct containing fields for the KV
// processor, which breaks message into key value pairs
type KVConfig struct {
	FieldSplit string `json:"field_split" yaml:"field_split"`
	ValueSplit string `json:"value_split" yaml:"value_split"`
}

// NewKVConfig returns a KVConfig with default values.
func NewKVConfig() KVConfig {
	return KVConfig{
		FieldSplit: " ",
		ValueSplit: "=",
	}
}
