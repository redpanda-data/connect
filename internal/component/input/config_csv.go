package input

// CSVFileConfig contains configuration values for the CSVFile input type.
type CSVFileConfig struct {
	Paths          []string `json:"paths" yaml:"paths"`
	ParseHeaderRow bool     `json:"parse_header_row" yaml:"parse_header_row"`
	Delim          string   `json:"delimiter" yaml:"delimiter"`
	LazyQuotes     bool     `json:"lazy_quotes" yaml:"lazy_quotes"`
	BatchCount     int      `json:"batch_count" yaml:"batch_count"`
	DeleteOnFinish bool     `json:"delete_on_finish" yaml:"delete_on_finish"`
}

// NewCSVFileConfig creates a new CSVFileConfig with default values.
func NewCSVFileConfig() CSVFileConfig {
	return CSVFileConfig{
		Paths:          []string{},
		ParseHeaderRow: true,
		Delim:          ",",
		LazyQuotes:     false,
		BatchCount:     1,
		DeleteOnFinish: false,
	}
}
