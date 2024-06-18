package influxdb

import (
	"sort"
	"strings"
	"time"

	"github.com/influxdata/influxdb1-client/pkg/escape"
	"github.com/rcrowley/go-metrics"
)

// not sure if this is necessary yet.
var tagEncodingSeparator = ","

type influxDBGauge struct {
	metrics.Gauge
}

// Set sets a gauge metric.
func (g influxDBGauge) Set(value int64) {
	g.Update(value)
}

func (g influxDBGauge) SetFloat64(value float64) {
	g.Set(int64(value))
}

// Incr increments a metric by an amount.
func (g influxDBGauge) Incr(count int64) {
	g.Update(g.Value() + count)
}

func (g influxDBGauge) IncrFloat64(count float64) {
	g.Incr(int64(count))
}

// Decr decrements a metric by an amount.
func (g influxDBGauge) Decr(count int64) {
	g.Update(g.Value() - count)
}

func (g influxDBGauge) DecrFloat64(count float64) {
	g.Decr(int64(count))
}

type influxDBCounter struct {
	metrics.Counter
}

// Incr increments a metric by an integer amount.
func (i influxDBCounter) Incr(count int64) {
	i.Inc(count)
}

// IncrFloat64 increments a metric by a decimal amount.
func (i influxDBCounter) IncrFloat64(count float64) {
	i.Inc(int64(count))
}

type influxDBTimer struct {
	metrics.Timer
}

// Timing sets a timing metric.
func (i influxDBTimer) Timing(delta int64) {
	i.Update(time.Duration(delta))
}

// encodeInfluxDBName accepts a measurement name and a map of tag values and
// returns influx line protocol-formatted string.
func encodeInfluxDBName(name string, tagNames, tagValues []string) string {
	b := &strings.Builder{}
	b.WriteString(escape.String(name))

	// only add tags+values if they're equal length
	if len(tagNames) > 0 && len(tagNames) == len(tagValues) {
		tags := make(map[string]string, len(tagNames))
		for k, v := range tagNames {
			tags[v] = tagValues[k]
		}

		tagSort := make([]string, len(tagNames))
		copy(tagSort, tagNames)
		sort.Strings(tagSort)

		// name,tag1=value1,tag2=value\ 3
		for _, v := range tagSort {
			b.WriteString(tagEncodingSeparator)
			b.WriteString(escape.String(v))
			b.WriteString("=")
			b.WriteString(escape.String(tags[v]))
		}
	}
	return b.String()
}

// decodeInfluxDBName accepts an ILP-formatted string (measurementName,tag=value) and
// returns the measurement name along with a map of tags and their values.
func decodeInfluxDBName(n string) (outName string, tags map[string]string) {
	nameSplit := splitUnescaped(n, tagEncodingSeparator)
	if len(nameSplit) == 0 {
		return "", nil
	} else if len(nameSplit) == 1 {
		return escape.UnescapeString(nameSplit[0]), nil
	}

	tags = make(map[string]string, len(nameSplit)-1)
	for _, v := range nameSplit[1:] {
		tagSplit := splitUnescaped(v, "=")
		if len(tagSplit) == 2 {
			key := escape.UnescapeString(tagSplit[0])
			value := escape.UnescapeString(tagSplit[1])
			tags[key] = value
		}
	}
	return escape.UnescapeString(nameSplit[0]), tags
}

func splitUnescaped(name, separator string) []string {
	parts := strings.Split(name, separator)
	out := make([]string, len(parts))
	writeIdx := 0
	for i := 0; i < len(parts); i++ {
		part := parts[i]
		// detect escaped
		for strings.HasSuffix(part, `\`) {
			part += separator
			if i+1 < len(parts) {
				part += parts[i+1]
				i++
			}
		}
		out[writeIdx] = part
		writeIdx++
	}
	return out[:writeIdx]
}
