package tracer

type Collector struct {
	Url string `json:"url" yaml:"url"`
}

// OtlpConfig is config for the Otlp tracer type.
type OtlpConfig struct {
	Grpc []Collector       `json:"grpc" yaml:"grpc"`
	Http []Collector       `json:"http" yaml:"http"`
	Tags map[string]string `json:"tags" yaml:"tags"`
}

// NewOtlpConfig creates an OtlpConfig struct with default values.
func NewOtlpConfig() OtlpConfig {
	return OtlpConfig{
		Grpc: []Collector{},
		Http: []Collector{},
		Tags: map[string]string{},
	}
}
