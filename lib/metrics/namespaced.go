package metrics

import "github.com/Jeffail/benthos/v3/lib/log"

//------------------------------------------------------------------------------

// namespacedWrapper wraps an existing Type under a namespace. The full path of
// subsequent metrics under this type will be:
//
// Underlying prefix + this namespace + path of metric
type namespacedWrapper struct {
	ns string
	t  Type
}

// Namespaced embeds an existing metrics aggregator under a new namespace. The
// prefix of the embedded aggregator is still the ultimate prefix of metrics.
func Namespaced(t Type, ns string) Type {
	return namespacedWrapper{
		ns: ns,
		t:  t,
	}
}

//------------------------------------------------------------------------------

func (d namespacedWrapper) GetCounter(path string) StatCounter {
	return d.t.GetCounter(d.ns + "." + path)
}

func (d namespacedWrapper) GetCounterVec(path string, labelNames []string) StatCounterVec {
	return d.t.GetCounterVec(d.ns+"."+path, labelNames)
}

func (d namespacedWrapper) GetTimer(path string) StatTimer {
	return d.t.GetTimer(d.ns + "." + path)
}

func (d namespacedWrapper) GetTimerVec(path string, labelNames []string) StatTimerVec {
	return d.t.GetTimerVec(d.ns+"."+path, labelNames)
}

func (d namespacedWrapper) GetGauge(path string) StatGauge {
	return d.t.GetGauge(d.ns + "." + path)
}

func (d namespacedWrapper) GetGaugeVec(path string, labelNames []string) StatGaugeVec {
	return d.t.GetGaugeVec(d.ns+"."+path, labelNames)
}

func (d namespacedWrapper) SetLogger(log log.Modular) {
	d.t.SetLogger(log.NewModule(d.ns))
}

func (d namespacedWrapper) Close() error {
	return d.t.Close()
}

//------------------------------------------------------------------------------
