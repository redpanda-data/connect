// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlredo

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

// OracleValueConverter handles conversion of Oracle function calls and special values
// to their proper Go types. This mirrors Debezium's OracleValueConverters.java and TimestampUtils.java
type OracleValueConverter struct {
	timezone *time.Location
}

// NewOracleValueConverter creates a new converter with the specified timezone
func NewOracleValueConverter(timezone *time.Location) *OracleValueConverter {
	return &OracleValueConverter{
		timezone: timezone,
	}
}

// Patterns for Oracle function calls
var (
	// TO_TIMESTAMP('2020-01-15 10:30:00','YYYY-MM-DD HH24:MI:SS')
	// TO_TIMESTAMP('2020-01-15 10:30:00.123456','YYYY-MM-DD HH24:MI:SS.FF6')
	toTimestampPattern = regexp.MustCompile(`(?i)TO_TIMESTAMP\('([^']+)'(?:,\s*'[^']*')?\)`)

	// TO_DATE('2020-01-15','YYYY-MM-DD')
	toDatePattern = regexp.MustCompile(`(?i)TO_DATE\('([^']+)',\s*'([^']+)'\)`)

	// TO_TIMESTAMP_TZ('2020-01-15 10:30:00 +00:00')
	toTimestampTzPattern = regexp.MustCompile(`(?i)TO_TIMESTAMP_TZ\('([^']+)'\)`)

	// HEXTORAW('48656C6C6F') - converts hex string to bytes
	hexToRawPattern = regexp.MustCompile(`(?i)HEXTORAW\('([0-9A-Fa-f]+)'\)`)

	// EMPTY_CLOB() or EMPTY_BLOB()
	emptyLobPattern = regexp.MustCompile(`(?i)EMPTY_(CLOB|BLOB)\(\)`)
)

// ConvertValue converts an Oracle value (potentially a function call) to its proper Go type
// columnType should be the Oracle column type (e.g., "DATE", "TIMESTAMP", "VARCHAR2", etc.)
func (c *OracleValueConverter) ConvertValue(value any, columnType string) any {
	str, ok := value.(string)
	if !ok {
		return value
	}

	switch strings.ToUpper(columnType) {
	case "DATE":
		return c.convertDateValue(str)
	case "TIMESTAMP", "TIMESTAMP(0)", "TIMESTAMP(3)", "TIMESTAMP(6)", "TIMESTAMP(9)":
		return c.convertTimestampValue(str)
	case "TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH LOCAL TIME ZONE":
		return c.convertTimestampWithZone(str)
	case "RAW", "LONG RAW":
		return c.convertRawValue(str)
	case "CLOB", "BLOB", "NCLOB":
		return c.convertLobValue(str)
	case "NUMBER", "FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE":
		return c.convertNumericValue(str)
	}

	if result := c.convertDateValue(str); result != nil {
		return result
	}
	if result := c.convertTimestampValue(str); result != nil {
		return result
	}
	if result := c.convertTimestampWithZone(str); result != nil {
		return result
	}

	return value
}

// convertDateValue converts TO_DATE function calls to time.Time
func (c *OracleValueConverter) convertDateValue(value string) any {
	matches := toDatePattern.FindStringSubmatch(value)
	if matches == nil {
		return nil
	}

	dateStr := matches[1]
	formatStr := matches[2] // Oracle format like 'YYYY-MM-DD'

	// Convert Oracle format to Go format
	goFormat := c.oracleFormatToGo(formatStr)
	if goFormat == "" {
		// first try common date formats
		for _, format := range []string{
			"2006-01-02",
			"2006-01-02 15:04:05",
			"02-Jan-06",
		} {
			if t, err := time.ParseInLocation(format, dateStr, c.timezone); err == nil {
				return t
			}
		}
		return nil
	}

	t, err := time.ParseInLocation(goFormat, dateStr, c.timezone)
	if err != nil {
		return nil
	}
	return t
}

// convertTimestampValue converts TO_TIMESTAMP function calls to time.Time
func (c *OracleValueConverter) convertTimestampValue(value string) any {
	matches := toTimestampPattern.FindStringSubmatch(value)
	if matches == nil {
		return nil
	}

	timestampStr := matches[1]

	// Try common timestamp formats
	formats := []string{
		"2006-01-02 15:04:05.999999999", // With nanoseconds
		"2006-01-02 15:04:05.999999",    // With microseconds
		"2006-01-02 15:04:05.999",       // With milliseconds
		"2006-01-02 15:04:05",           // Without fractional seconds
		"02-Jan-06 03.04.05.999999 PM",  // Oracle NLS format with fractional
		"02-Jan-06 03.04.05 PM",         // Oracle NLS format
	}

	for _, format := range formats {
		if t, err := time.ParseInLocation(format, timestampStr, c.timezone); err == nil {
			return t
		}
	}

	return nil
}

// convertTimestampWithZone converts TO_TIMESTAMP_TZ function calls
func (*OracleValueConverter) convertTimestampWithZone(value string) any {
	matches := toTimestampTzPattern.FindStringSubmatch(value)
	if matches == nil {
		return nil
	}

	timestampStr := matches[1]

	// Try formats with timezone
	formats := []string{
		"2006-01-02 15:04:05.999999999 -07:00",
		"2006-01-02 15:04:05.999999 -07:00",
		"2006-01-02 15:04:05.999 -07:00",
		"2006-01-02 15:04:05 -07:00",
		"2006-01-02 15:04:05.999999999 MST",
		"2006-01-02 15:04:05 MST",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, timestampStr); err == nil {
			return t
		}
	}

	return nil
}

// convertRawValue converts HEXTORAW function calls to byte slices
func (*OracleValueConverter) convertRawValue(value string) any {
	matches := hexToRawPattern.FindStringSubmatch(value)
	if matches == nil {
		return value
	}

	hexStr := matches[1]
	bytes := make([]byte, len(hexStr)/2)

	for i := 0; i < len(hexStr); i += 2 {
		b, err := strconv.ParseUint(hexStr[i:i+2], 16, 8)
		if err != nil {
			return value
		}
		bytes[i/2] = byte(b)
	}

	return bytes
}

// convertLobValue handles EMPTY_CLOB() and EMPTY_BLOB()
func (*OracleValueConverter) convertLobValue(value string) any {
	if emptyLobPattern.MatchString(value) {
		// Return empty byte slice for empty LOBs
		return []byte{}
	}
	return value
}

// convertNumericValue attempts to parse numeric values
func (*OracleValueConverter) convertNumericValue(value string) any {
	if i, err := strconv.ParseInt(value, 10, 64); err == nil {
		return i
	}

	if f, err := strconv.ParseFloat(value, 64); err == nil {
		return f
	}

	return value
}

// oracleFormatToGo converts Oracle date/timestamp format to Go format
// Oracle formats: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Format-Models.html
func (*OracleValueConverter) oracleFormatToGo(oracleFormat string) string {
	// CRITICAL: Must replace in order from longest to shortest pattern to avoid substring conflicts!
	// For example, "YYYY" must be replaced before "YY", otherwise "YY" will match inside "YYYY"
	// and corrupt it to "Y06Y". This caused dates like 9999 to be parsed as 1999.
	replacements := []struct {
		oracle string
		golang string
	}{
		// Fractional seconds - longest first
		{"FF9", ".999999999"},
		{"FF6", ".999999"},
		{"FF3", ".999"},
		{"FF", ".999999"}, // Default to microseconds
		// Years - longest first
		{"YYYY", "2006"},
		{"YY", "06"},
		// Hours - longest first
		{"HH24", "15"},
		{"HH", "03"},
		// Other elements
		{"MON", "Jan"},
		{"MM", "01"},
		{"DD", "02"},
		{"MI", "04"},
		{"SS", "05"},
		{"AM", "PM"},
		{"PM", "PM"},
	}

	result := oracleFormat
	for _, r := range replacements {
		result = strings.ReplaceAll(result, r.oracle, r.golang)
	}

	return result
}
