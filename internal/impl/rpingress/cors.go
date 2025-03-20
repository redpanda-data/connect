// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package rpingress

import (
	"net/http"

	"github.com/gorilla/handlers"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	fieldCORS               = "cors"
	fieldCORSEnabled        = "enabled"
	fieldCORSAllowedOrigins = "allowed_origins"
)

type corsConfig struct {
	Enabled        bool
	AllowedOrigins []string
}

func (conf corsConfig) WrapHandler(handler http.Handler) http.Handler {
	if !conf.Enabled {
		return handler
	}
	return handlers.CORS(
		handlers.AllowedOrigins(conf.AllowedOrigins),
		handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"}),
	)(handler)
}

func corsField() *service.ConfigField {
	return service.NewObjectField(fieldCORS,
		service.NewBoolField(fieldCORSEnabled).Description("Whether to allow CORS requests.").Default(false),
		service.NewStringListField(fieldCORSAllowedOrigins).Description("An explicit list of origins that are allowed for CORS requests.").Default([]any{}),
	).
		Description("Adds Cross-Origin Resource Sharing headers.").
		Advanced()
}

func corsConfigFromParsed(pConf *service.ParsedConfig) (conf corsConfig, err error) {
	pConf = pConf.Namespace(fieldCORS)
	if conf.Enabled, err = pConf.FieldBool(fieldCORSEnabled); err != nil {
		return
	}
	if conf.AllowedOrigins, err = pConf.FieldStringList(fieldCORSAllowedOrigins); err != nil {
		return
	}
	return
}
