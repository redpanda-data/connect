package output

import (
	"cloud.google.com/go/bigquery"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
)

// GCPBigQueryCSVConfig contains configuration fields for CSV format used
// in the GCP BigQuery output type.
type GCPBigQueryCSVConfig struct {
	// CSV options
	Header              []string `json:"header" yaml:"header"`
	FieldDelimiter      string   `json:"field_delimiter" yaml:"field_delimiter"`
	AllowJaggedRows     bool     `json:"allow_jagged_rows" yaml:"allow_jagged_rows"`
	AllowQuotedNewlines bool     `json:"allow_quoted_newlines" yaml:"allow_quoted_newlines"`
	Encoding            string   `json:"encoding" yaml:"encoding"`
	SkipLeadingRows     int64    `json:"skip_leading_rows" yaml:"skip_leading_rows"`
}

// GCPBigQueryConfig contains configuration fields for the GCP BigQuery
// output type.
type GCPBigQueryConfig struct {
	ProjectID           string `json:"project" yaml:"project"`
	DatasetID           string `json:"dataset" yaml:"dataset"`
	TableID             string `json:"table" yaml:"table"`
	Format              string `json:"format" yaml:"format"`
	WriteDisposition    string `json:"write_disposition" yaml:"write_disposition"`
	CreateDisposition   string `json:"create_disposition" yaml:"create_disposition"`
	AutoDetect          bool   `json:"auto_detect" yaml:"auto_detect"`
	IgnoreUnknownValues bool   `json:"ignore_unknown_values" yaml:"ignore_unknown_values"`
	MaxBadRecords       int64  `json:"max_bad_records" yaml:"max_bad_records"`

	MaxInFlight int                `json:"max_in_flight" yaml:"max_in_flight"`
	Batching    batch.PolicyConfig `json:"batching" yaml:"batching"`

	// CSV options
	CSVOptions GCPBigQueryCSVConfig `json:"csv" yaml:"csv"`
}

// NewGCPBigQueryCSVConfig creates a new CSV config with default values.
func NewGCPBigQueryCSVConfig() GCPBigQueryCSVConfig {
	return GCPBigQueryCSVConfig{
		Header:              nil,
		FieldDelimiter:      ",",
		AllowJaggedRows:     false,
		AllowQuotedNewlines: false,
		Encoding:            string(bigquery.UTF_8),
		SkipLeadingRows:     1,
	}
}

// NewGCPBigQueryConfig creates a new Config with default values.
func NewGCPBigQueryConfig() GCPBigQueryConfig {
	return GCPBigQueryConfig{
		ProjectID:           "",
		DatasetID:           "",
		TableID:             "",
		AutoDetect:          false,
		IgnoreUnknownValues: false,
		WriteDisposition:    string(bigquery.WriteAppend),
		CreateDisposition:   string(bigquery.CreateIfNeeded),
		MaxBadRecords:       0,
		Format:              string(bigquery.JSON),
		MaxInFlight:         1,
		Batching:            batch.NewPolicyConfig(),
		CSVOptions:          NewGCPBigQueryCSVConfig(),
	}
}
