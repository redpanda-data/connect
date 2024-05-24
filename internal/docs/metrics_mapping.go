package docs

// MetricsMappingFieldSpec is a field spec that describes a Bloblang mapping for
// renaming metrics.
func MetricsMappingFieldSpec(name string) FieldSpec {
	examples := []any{
		`this.replace("input", "source").replace("output", "sink")`,
		`root = if ![
  "input_received",
  "input_latency",
  "output_sent"
].contains(this) { deleted() }`,
	}
	summary := "An optional xref:guides:bloblang/about.adoc[Bloblang mapping] that allows you to rename or prevent certain metrics paths from being exported. For more information check out the xref:components:metrics/about.adoc#metric-mapping[metrics documentation]. When metric paths are created, renamed and dropped a trace log is written, enabling TRACE level logging is therefore a good way to diagnose path mappings."
	return FieldBloblang(name, summary, examples...).HasDefault("")
}
