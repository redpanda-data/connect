// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package otlp

import (
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	otFieldService = "service"
	otFieldHTTP    = "http"
	otFieldGRPC    = "grpc"
	otFieldTags    = "tags"

	otExporterTimeout = 30 * time.Second
)

type collector struct {
	address string
	secure  bool
}

func collectorListFields() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewObjectListField(otFieldHTTP,
			service.NewStringField("address").
				Description("The endpoint of a collector to send events to.").
				Optional().
				Example("localhost:4318"),
			service.NewStringField("url").
				Description("The URL of a collector to send events to.").
				Deprecated().
				Default("localhost:4318"),
			service.NewBoolField("secure").
				Description("Connect to the collector over HTTPS").
				Default(false),
		).Description("A list of http collectors."),
		service.NewObjectListField(otFieldGRPC,
			service.NewURLField("address").
				Description("The endpoint of a collector to send events to.").
				Optional().
				Example("localhost:4317"),
			service.NewURLField("url").
				Description("The URL of a collector to send events to.").
				Deprecated().
				Default("localhost:4317"),
			service.NewBoolField("secure").
				Description("Connect to the collector with client transport security").
				Default(false),
		).Description("A list of grpc collectors."),
	}
}

func tagsField() *service.ConfigField {
	return service.NewStringMapField(otFieldTags).
		Description("A map of tags to add to all exported spans and metrics.").
		Default(map[string]any{}).
		Advanced()
}

func parseCollectors(conf *service.ParsedConfig, name string) ([]collector, error) {
	list, err := conf.FieldObjectList(name)
	if err != nil {
		return nil, err
	}
	collectors := make([]collector, 0, len(list))
	for _, pc := range list {
		u, _ := pc.FieldString("address")
		if u == "" {
			if u, _ = pc.FieldString("url"); u == "" {
				return nil, errors.New("an address must be specified")
			}
		}

		secure, err := pc.FieldBool("secure")
		if err != nil {
			return nil, err
		}

		collectors = append(collectors, collector{
			address: u,
			secure:  secure,
		})
	}
	return collectors, nil
}

func newResource(tags map[string]string, serviceName, engineVersion string) *resource.Resource {
	var attrs []attribute.KeyValue
	for k, v := range tags {
		attrs = append(attrs, attribute.String(k, v))
	}

	if _, ok := tags[string(semconv.ServiceNameKey)]; !ok {
		attrs = append(attrs, semconv.ServiceNameKey.String(serviceName))

		// Only set the default service version tag if the user doesn't provide
		// a custom service name tag.
		if _, ok := tags[string(semconv.ServiceVersionKey)]; !ok {
			attrs = append(attrs, semconv.ServiceVersionKey.String(engineVersion))
		}
	}

	return resource.NewWithAttributes(semconv.SchemaURL, attrs...)
}
