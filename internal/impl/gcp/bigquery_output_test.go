package gcp

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/stretchr/testify/require"
)

func TestNewGCPBigQueryOutputJsonNewLineOk(t *testing.T) {
	output, err := newGCPBigQueryOutput(output.GCPBigQueryConfig{}, nil, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
}

func TestNewGCPBigQueryOutputCsvDefaultConfigIsoOk(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)

	output, err := newGCPBigQueryOutput(config, nil, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, ",", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvDefaultConfigUtfOk(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)

	output, err := newGCPBigQueryOutput(config, nil, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, ",", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvCustomConfigIsoOk(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.FieldDelimiter = "¨"

	output, err := newGCPBigQueryOutput(config, nil, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, "\xa8", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvCustomConfigUtfOk(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)
	config.CSVOptions.FieldDelimiter = "¨"

	output, err := newGCPBigQueryOutput(config, nil, nil)

	require.NoError(t, err)
	require.Equal(t, "\n", string(output.newLineBytes))
	require.Equal(t, "¨", string(output.fieldDelimiterBytes))
}

func TestNewGCPBigQueryOutputCsvHeaderIsoOk(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.Header = []string{"a", "â", "ã", "ä"}

	output, err := newGCPBigQueryOutput(config, nil, nil)

	require.NoError(t, err)
	require.Equal(t, "\"a\",\"\xe2\",\"\xe3\",\"\xe4\"", string(output.csvHeaderBytes))
}

func TestNewGCPBigQueryOutputCsvHeaderUtfOk(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Header = []string{"a", "â", "ã", "ä"}

	output, err := newGCPBigQueryOutput(config, nil, nil)

	require.NoError(t, err)
	require.Equal(t, "\"a\",\"â\",\"ã\",\"ä\"", string(output.csvHeaderBytes))
}

func TestNewGCPBigQueryOutputCsvFieldDelimiterIsoError(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.FieldDelimiter = "\xa8"

	_, err := newGCPBigQueryOutput(config, nil, nil)

	require.Error(t, err)
}

func TestNewGCPBigQueryOutputCsvHeaderIsoError(t *testing.T) {
	config := output.NewGCPBigQueryConfig()
	config.Format = string(bigquery.CSV)
	config.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	config.CSVOptions.Header = []string{"\xa8"}

	_, err := newGCPBigQueryOutput(config, nil, nil)

	require.Error(t, err)
}

func TestConvertToIsoOk(t *testing.T) {
	value := "\"a\"¨\"â\"¨\"ã\"¨\"ä\""

	result, err := convertToIso([]byte(value))

	require.NoError(t, err)
	require.Equal(t, "\"a\"\xa8\"\xe2\"\xa8\"\xe3\"\xa8\"\xe4\"", string(result))
}

func TestConvertToIsoError(t *testing.T) {
	value := "\xa8"

	_, err := convertToIso([]byte(value))
	require.Error(t, err)
}

func TestCreateTableLoaderOk(t *testing.T) {
	// Setting non-default values
	outputConfig := output.NewGCPBigQueryConfig()
	outputConfig.ProjectID = "project"
	outputConfig.DatasetID = "dataset"
	outputConfig.TableID = "table"
	outputConfig.WriteDisposition = string(bigquery.WriteTruncate)
	outputConfig.CreateDisposition = string(bigquery.CreateNever)
	outputConfig.Format = string(bigquery.CSV)
	outputConfig.AutoDetect = true
	outputConfig.IgnoreUnknownValues = true
	outputConfig.MaxBadRecords = 123
	outputConfig.CSVOptions.FieldDelimiter = ";"
	outputConfig.CSVOptions.AllowJaggedRows = true
	outputConfig.CSVOptions.AllowQuotedNewlines = true
	outputConfig.CSVOptions.Encoding = string(bigquery.ISO_8859_1)
	outputConfig.CSVOptions.SkipLeadingRows = 10

	output, err := newGCPBigQueryOutput(outputConfig, nil, nil)

	require.NoError(t, err)

	var data = []byte("1,2,3")
	loader := output.createTableLoader(&data)

	require.Equal(t, "table", loader.Dst.TableID)
	require.Equal(t, "dataset", loader.Dst.DatasetID)
	require.Equal(t, "project", loader.Dst.ProjectID)
	require.Equal(t, bigquery.TableWriteDisposition(outputConfig.WriteDisposition), loader.WriteDisposition)
	require.Equal(t, bigquery.TableCreateDisposition(outputConfig.CreateDisposition), loader.CreateDisposition)

	readerSource, ok := loader.Src.(*bigquery.ReaderSource)
	require.True(t, ok)

	require.Equal(t, bigquery.DataFormat(outputConfig.Format), readerSource.SourceFormat)
	require.Equal(t, outputConfig.AutoDetect, readerSource.AutoDetect)
	require.Equal(t, outputConfig.IgnoreUnknownValues, readerSource.IgnoreUnknownValues)
	require.Equal(t, outputConfig.MaxBadRecords, readerSource.MaxBadRecords)

	expectedCsvOptions := outputConfig.CSVOptions

	require.Equal(t, expectedCsvOptions.FieldDelimiter, readerSource.FieldDelimiter)
	require.Equal(t, expectedCsvOptions.AllowJaggedRows, readerSource.AllowJaggedRows)
	require.Equal(t, expectedCsvOptions.AllowQuotedNewlines, readerSource.AllowQuotedNewlines)
	require.Equal(t, bigquery.Encoding(expectedCsvOptions.Encoding), readerSource.Encoding)
	require.Equal(t, expectedCsvOptions.SkipLeadingRows, readerSource.SkipLeadingRows)
}
