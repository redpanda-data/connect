package cuegen

import "cuelang.org/go/cue/ast"

var (
	identConfig = ast.NewIdent("#Config")

	identResources = ast.NewIdent("#Resources")

	identHTTP     = ast.NewIdent("http")
	identHTTPConf = ast.NewIdent("#HTTP")

	identLogger     = ast.NewIdent("logger")
	identLoggerConf = ast.NewIdent("#Logger")

	identInput            = ast.NewIdent("input")
	identInputDisjunction = ast.NewIdent("#Input")
	identInputCollection  = ast.NewIdent("#AllInputs")

	identOutput            = ast.NewIdent("output")
	identOutputDisjunction = ast.NewIdent("#Output")
	identOutputCollection  = ast.NewIdent("#AllOutputs")

	identPipeline             = ast.NewIdent("pipeline")
	identProcessorDisjunction = ast.NewIdent("#Processor")
	identProcessorCollection  = ast.NewIdent("#AllProcessors")

	identRateLimitResources   = ast.NewIdent("rate_limit_resources")
	identRateLimitDisjunction = ast.NewIdent("#RateLimit")
	identRateLimitCollection  = ast.NewIdent("#AllRateLimits")

	identBuffer            = ast.NewIdent("buffer")
	identBufferDisjunction = ast.NewIdent("#Buffer")
	identBufferCollection  = ast.NewIdent("#AllBuffers")

	identCacheResources   = ast.NewIdent("cache_resources")
	identCacheDisjunction = ast.NewIdent("#Cache")
	identCacheCollection  = ast.NewIdent("#AllCaches")

	identMetric            = ast.NewIdent("metric")
	identMetricDisjunction = ast.NewIdent("#Metric")
	identMetricCollection  = ast.NewIdent("#AllMetrics")

	identTracer            = ast.NewIdent("tracer")
	identTracerDisjunction = ast.NewIdent("#Tracer")
	identTracerCollection  = ast.NewIdent("#AllTracers")

	identInputResources = ast.NewIdent("input_resources")

	identProcessorResources = ast.NewIdent("processor_resources")

	identOutputResources = ast.NewIdent("output_resources")

	identShutdownTimeout = ast.NewIdent("shutdown_timeout")

	identTestSuite = ast.NewIdent("#TestSuite")
)
