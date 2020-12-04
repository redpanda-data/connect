package metrics

import (
	"sort"
	"strings"
	"time"

	"github.com/influxdata/influxdb1-client/pkg/escape"
	"github.com/rcrowley/go-metrics"
)

// not sure if this is necessary yet
var tagEncodingSeparator = ","

type influxGauge struct {
	metrics.Gauge
}

// Set sets a gauge metric.
func (g influxGauge) Set(value int64) error {
	g.Update(value)
	return nil
}

// Incr increments a metric by an amount.
func (g influxGauge) Incr(count int64) error {
	g.Update(g.Value() + count)
	return nil
}

// Decr decrements a metric by an amount.
func (g influxGauge) Decr(count int64) error {
	g.Update(g.Value() - count)
	return nil
}

type influxCounter struct {
	metrics.Counter
}

// Incr increments a metric by an amount.
func (i influxCounter) Incr(count int64) error {
	i.Inc(count)
	return nil
}

type influxTimer struct {
	metrics.Timer
}

// Timing sets a timing metric.
func (i influxTimer) Timing(delta int64) error {
	i.Update(time.Duration(delta))
	return nil
}

// encodeInfluxName accepts a measurement name and a map of tag values and
// returns influx line protocol-formatted string.
func encodeInfluxName(name string, tagNames []string, tagValues []string) string {
	b := &strings.Builder{}
	b.WriteString(escape.String(name))

	// only add tags+values if they're equal length
	if len(tagNames) == len(tagValues) {
		tags := make(map[string]string, len(tagNames))
		for k, v := range tagNames {
			tags[v] = tagValues[k]
		}

		sort.Strings(tagNames)

		// name,tag1=value1,tag2=value\ 3
		for _, v := range tagNames {
			b.WriteString(tagEncodingSeparator)
			b.WriteString(escape.String(v))
			b.WriteString("=")
			b.WriteString(escape.String(tags[v]))
		}
	}
	return b.String()
}

// decodeInfluxName accepts an ILP-formatted string (measurementName,tag=value) and
// returns the measurement name along with a map of tags and their values.
func decodeInfluxName(n string) (string, map[string]string) {
	nameSplit := splitUnescaped(n, tagEncodingSeparator)
	if len(nameSplit) == 0 {
		return "", nil
	} else if len(nameSplit) == 1 {
		return escape.UnescapeString(nameSplit[0]), nil
	} else {
		var tags = make(map[string]string, len(nameSplit)-1)
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
}

func splitUnescaped(name string, separator string) []string {
	parts := strings.Split(name, separator)
	out := make([]string, len(parts))
	writeIdx := 0
	for i := 0; i < len(parts); i++ {
		part := parts[i]
		// detect escaped
		for strings.HasSuffix(part, `\`) {
			part = part + separator
			if i+1 < len(parts) {
				part = part + parts[i+1]
				i++
			}
		}
		out[writeIdx] = part
		writeIdx++
	}
	return out[:writeIdx]
}
