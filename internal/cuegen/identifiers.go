package cuegen

import "cuelang.org/go/cue/ast"

var (
	identConfig = ast.NewIdent("#Config")

	identInputDisjunction = ast.NewIdent("#Input")
	identInputCollection  = ast.NewIdent("#AllInputs")

	identOutputDisjunction = ast.NewIdent("#Output")
	identOutputCollection  = ast.NewIdent("#AllOutputs")

	identProcessorDisjunction = ast.NewIdent("#Processor")
	identProcessorCollection  = ast.NewIdent("#AllProcessors")

	identRateLimitDisjunction = ast.NewIdent("#RateLimit")
	identRateLimitCollection  = ast.NewIdent("#AllRateLimits")

	identBufferDisjunction = ast.NewIdent("#Buffer")
	identBufferCollection  = ast.NewIdent("#AllBuffers")

	identCacheDisjunction = ast.NewIdent("#Cache")
	identCacheCollection  = ast.NewIdent("#AllCaches")

	identMetricDisjunction = ast.NewIdent("#Metric")
	identMetricCollection  = ast.NewIdent("#AllMetrics")

	identTracerDisjunction = ast.NewIdent("#Tracer")
	identTracerCollection  = ast.NewIdent("#AllTracers")
)
