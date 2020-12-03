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

var _ StatGauge = (*InfluxGauge)(nil)
var _ StatCounter = (*InfluxCounter)(nil)
var _ StatTimer = (*InfluxTimer)(nil)

type InfluxGauge struct {
	metrics.Gauge
}

func NewGauge() InfluxGauge {
	return InfluxGauge{
		metrics.NewGauge(),
	}
}

func (g InfluxGauge) Set(value int64) error {
	g.Update(value)
	return nil
}

func (g InfluxGauge) Incr(count int64) error {
	g.Update(g.Value() + count)
	return nil
}

func (g InfluxGauge) Decr(count int64) error {
	g.Update(g.Value() - count)
	return nil
}

type InfluxCounter struct {
	metrics.Counter
}

func NewCounter() InfluxCounter {
	return InfluxCounter{
		metrics.NewCounter(),
	}
}

func (i InfluxCounter) Incr(count int64) error {
	i.Inc(count)
	return nil
}

type InfluxTimer struct {
	metrics.Timer
}

func NewTimer() InfluxTimer {
	return InfluxTimer{
		metrics.NewTimer(),
	}
}

func (i InfluxTimer) Timing(delta int64) error {
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
