package metrics

import (
	"sort"
	"strings"

	"github.com/influxdata/influxdb1-client/pkg/escape"
	"github.com/rcrowley/go-metrics"
)

// not sure if this is necessary yet
var tagEncodingSeparator = ","

var _ StatGauge = (*Gauge)(nil)
var _ StatGaugeVec = (*Gauge)(nil)

type Gauge struct {
	name  string
	gauge metrics.Gauge
}

func (g Gauge) With(labelValues ...string) StatGauge {
	// encode tags with metric name
	return &Gauge{
		gauge: metrics.NewGauge(),
	}
}

func (g Gauge) Set(value int64) error {
	g.gauge.Update(value)
	return nil
}

func (g Gauge) Incr(count int64) error {
	g.gauge.Update(g.gauge.Value() + 1)
	return nil
}

func (g Gauge) Decr(count int64) error {
	g.gauge.Update(g.gauge.Value() - 1)
	return nil
}

// encodeInfluxName takes a map of tag values and appends as an influx line protocol-formatted string
func encodeInfluxName(name string, tags map[string]string) string {
	b := &strings.Builder{}
	b.WriteString(escape.String(name))
	sorted := make([]string, 0, len(tags))
	for key := range tags {
		sorted = append(sorted, key)
	}
	sort.Strings(sorted)

	// name,tag1=value1,tag2=value\ 3
	for _, v := range sorted {
		b.WriteString(tagEncodingSeparator)
		b.WriteString(escape.String(v))
		b.WriteString("=")
		b.WriteString(escape.String(tags[v]))
	}
	return b.String()
}

func decodeInfluxName(n string) (string, map[string]string) {
	nameSplit := splitUnescaped(n, tagEncodingSeparator)
	if len(nameSplit) == 0 {
		return "", nil
	} else if len(nameSplit) == 1 {
		return escape.UnescapeString(nameSplit[0]), nil
	} else {
		var tags map[string]string
		for _, v := range nameSplit[1:] {
			tagSplit := splitUnescaped(v, "=")
			if len(tags) == 2 {
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
		writeIdx++
		out = append(out, part)
	}
	return out[:writeIdx]
}
