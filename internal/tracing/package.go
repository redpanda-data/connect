// Package tracing implements utility functions for interacting with a global
// tracing system. Currently this system uses the opentracing APIs. However,
// eventually we will need to migrate to the new opentelemetry APIs, and
// therefore in order to reduce disruption this package abstracts interaction
// with these packages.
package tracing
