// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package migratorrules registers Bloblang V1->V2 translation rules for every
// plugin that ships in the Redpanda Connect tree. Each rule is a mechanical
// pass-through: V1 and V2 names match, arguments thread through unchanged.
//
// The rule pack is consumed by the `connect migrate v5` CLI subcommand. It is
// kept private to the connect repository — downstream plugin authors that need
// to register similar rules can lift the same pattern when the time comes.
package migratorrules

import (
	bloblmig "github.com/redpanda-data/benthos/v4/public/bloblangv2/migrator"
)

// Register installs a translation rule for every Bloblang plugin shipped by
// Redpanda Connect. Every rule rewrites a V1 callsite to its V2 equivalent of
// the same name, threading arguments through unchanged.
//
// Call once per Migrator instance after construction and before Migrate.
func Register(mig *bloblmig.Migrator) {
	for _, name := range methodPlugins {
		mig.RegisterMethodRule(name, passThroughMethodRule(name))
	}
	for _, name := range functionPlugins {
		mig.RegisterFunctionRule(name, passThroughFunctionRule(name))
	}
}

// methodPlugins is the list of every connect-registered V1 Bloblang method
// plugin. The order is irrelevant; the slice is iterated once at Register
// time.
var methodPlugins = []string{
	// internal/impl/msgpack
	"parse_msgpack",
	"format_msgpack",

	// internal/impl/parquet
	"parse_parquet",

	// internal/impl/sql
	"vector",

	// internal/impl/jsonpath
	"json_path",

	// internal/impl/html
	"strip_html",

	// internal/impl/xml
	"parse_xml",
	"format_xml",

	// internal/impl/changelog
	"diff",
	"patch",

	// internal/impl/crypto
	"compare_argon2",
	"compare_bcrypt",

	// internal/impl/lang
	"slug",
	"unicode_segments",

	// internal/impl/crypto (jwt parse + sign)
	"parse_jwt_hs256",
	"parse_jwt_hs384",
	"parse_jwt_hs512",
	"parse_jwt_rs256",
	"parse_jwt_rs384",
	"parse_jwt_rs512",
	"parse_jwt_es256",
	"parse_jwt_es384",
	"parse_jwt_es512",
	"sign_jwt_hs256",
	"sign_jwt_hs384",
	"sign_jwt_hs512",
	"sign_jwt_rs256",
	"sign_jwt_rs384",
	"sign_jwt_rs512",
	"sign_jwt_es256",
	"sign_jwt_es384",
	"sign_jwt_es512",

	// internal/impl/maxmind
	"geoip_city",
	"geoip_country",
	"geoip_asn",
	"geoip_enterprise",
	"geoip_anonymous_ip",
	"geoip_connection_type",
	"geoip_domain",
	"geoip_isp",
}

// functionPlugins is the list of every connect-registered V1 Bloblang function
// plugin.
var functionPlugins = []string{
	// internal/impl/confluent
	"with_schema_registry_header",

	// internal/impl/lang
	"fake",
	"snowflake_id",
	"ulid",
}

// passThroughMethodRule returns a rule that rewrites a V1 method call into a
// V2 method call of the same name with arguments translated recursively. Used
// for every connect plugin whose V1 and V2 signatures match position for
// position.
func passThroughMethodRule(name string) bloblmig.MethodRule {
	return func(ctx *bloblmig.Context, m *bloblmig.V1MethodCall) bloblmig.Result {
		args := make([]bloblmig.V2CallArg, len(m.Args))
		for i, a := range m.Args {
			args[i] = bloblmig.V2CallArg{Name: a.Name, Value: ctx.Translate(a.Value)}
		}
		return ctx.Replace(&bloblmig.V2MethodCallExpr{
			Receiver: ctx.Translate(m.Receiver),
			Method:   name,
			Args:     args,
			Named:    m.Named,
		})
	}
}

// passThroughFunctionRule returns a rule that rewrites a V1 function call into
// a V2 function call of the same name with arguments translated recursively.
func passThroughFunctionRule(name string) bloblmig.FunctionRule {
	return func(ctx *bloblmig.Context, f *bloblmig.V1FunctionCall) bloblmig.Result {
		args := make([]bloblmig.V2CallArg, len(f.Args))
		for i, a := range f.Args {
			args[i] = bloblmig.V2CallArg{Name: a.Name, Value: ctx.Translate(a.Value)}
		}
		return ctx.Replace(&bloblmig.V2CallExpr{
			Name:  name,
			Args:  args,
			Named: f.Named,
		})
	}
}
