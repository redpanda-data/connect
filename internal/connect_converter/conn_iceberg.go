// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"strings"

	"gopkg.in/yaml.v3"
)

func init() {
	// Apache Iceberg Kafka Connect sink (donated from Tabular). The current
	// Apache class and the legacy Tabular class share the same config shape;
	// there is no distinct Confluent or Aiven class (both ship the upstream
	// connector).
	registerConnector("org.apache.iceberg.connect.IcebergSinkConnector", icebergSinkConnector{})
	registerConnector("io.tabular.iceberg.connect.IcebergSinkConnector", icebergSinkConnector{})
}

type icebergSinkConnector struct{}

func (icebergSinkConnector) Map(_ ConnectConfig, ctx *MapCtx) (Component, error) {
	body := mapping()

	// Determine the catalog kind. RPCN's iceberg output speaks ONLY the REST
	// catalog API; Glue is reachable via its REST endpoint, while hive/hadoop/
	// nessie/jdbc catalogs must be migrated to a REST endpoint.
	catType, _ := ctx.Lookup("iceberg.catalog.type")
	catImpl, _ := ctx.Lookup("iceberg.catalog.catalog-impl")
	isGlue := strings.Contains(catImpl, "GlueCatalog")

	kv(body, "catalog", icebergCatalog(ctx, catType, catImpl, isGlue))

	ns, tbl := icebergNamespaceTable(ctx)
	kv(body, "namespace", ns)
	kv(body, "table", tbl)

	kv(body, "storage", icebergStorage(ctx))

	// Schema evolution: evolve-schema-enabled → schema_evolution.enabled. Glue
	// does not assign table locations, so derive one from the warehouse prefix.
	icebergSchemaEvolution(ctx, body, isGlue)

	// The coordinator commit interval is the closest analogue to RPCN's batch
	// flush cadence.
	mapBatching(body, ctx, nil, "", "iceberg.control.commit.interval-ms")

	// Upsert / CDC: RPCN's iceberg output appends rows; it has no upsert/merge
	// path, so flag when the source relied on it.
	if v, ok := ctx.String("iceberg.tables.upsert-mode-enabled"); ok && strings.EqualFold(v, "true") {
		ctx.Warn("iceberg.tables.upsert-mode-enabled", "RPCN iceberg appends rows — KC upsert mode (upsert-mode-enabled/cdc-field/default-id-columns) is not reproduced; dedupe/merge downstream")
	}
	if v, ok := ctx.String("iceberg.tables.cdc-field"); ok && v != "" {
		ctx.Warn("iceberg.tables.cdc-field", "RPCN iceberg appends rows — the CDC op field "+v+" is not applied; handle deletes/updates downstream")
	}

	// Consume the remaining Iceberg plumbing families so they don't surface as
	// unmapped TODO noise. (Specific keys above are already consumed.)
	consumeIgnored(ctx,
		"iceberg.tables",
		"iceberg.catalog",
		"iceberg.hadoop-conf-dir",
		"iceberg.coordinator.transactional.prefix",
	)
	consumePrefix(ctx, "iceberg.catalog.")
	consumePrefix(ctx, "iceberg.tables.")
	consumePrefix(ctx, "iceberg.table.")
	consumePrefix(ctx, "iceberg.control.")
	consumePrefix(ctx, "iceberg.kafka.")
	consumePrefix(ctx, "iceberg.hadoop.")

	// topics / topics.regex are consumed by the engine's sink-input synthesis
	// (sinkInputFromTopics), which builds the redpanda input. value.converter is
	// intentionally left to the converter pass: an Iceberg sink reads encoded
	// records from Kafka, so a schema_registry_decode IS appropriate.
	return Component{Output: component("iceberg", body)}, nil
}

// icebergCatalog builds the REST catalog node. Glue is mapped to its REST
// endpoint with SigV4 auth; non-REST catalogs emit a stub URL and a warning.
func icebergCatalog(ctx *MapCtx, catType, catImpl string, isGlue bool) *yaml.Node {
	catalog := mapping()

	uri, _ := ctx.String("iceberg.catalog.uri")
	urlNode := scalar(uri)
	switch {
	case isGlue:
		if urlNode.Value == "" {
			region, _ := ctx.Lookup("iceberg.catalog.client.region")
			if region == "" {
				region = "<region>"
			}
			urlNode.Value = "https://glue." + region + ".amazonaws.com/iceberg"
		}
		urlNode.LineComment = "TODO: AWS Glue via the Iceberg REST endpoint — verify the URL/region"
	case strings.Contains(catType, "hive") || strings.Contains(catType, "hadoop") ||
		strings.Contains(catImpl, "Nessie") || strings.Contains(catImpl, "Jdbc") || strings.Contains(catImpl, "Dynamo"):
		if urlNode.Value == "" {
			urlNode.LineComment = "TODO: RPCN iceberg supports only the REST catalog — point this at a REST endpoint (Polaris, Glue REST, Unity Catalog)"
		} else {
			urlNode.LineComment = "TODO: this is a non-REST catalog URI; RPCN iceberg needs a REST endpoint — migrate to Polaris/Glue REST/Unity"
		}
		ctx.Warn("iceberg.catalog.type", "RPCN iceberg output supports only the REST catalog; the source uses a non-REST catalog (hive/hadoop/nessie/jdbc) — migrate to a REST catalog endpoint")
	case urlNode.Value == "":
		urlNode.LineComment = "TODO: set the REST catalog URL"
	}
	kv(catalog, "url", urlNode)

	if w, ok := ctx.String("iceberg.catalog.warehouse"); ok && w != "" {
		kv(catalog, "warehouse", scalar(w))
	}

	switch {
	case credPresent(ctx):
		cred, _ := ctx.String("iceberg.catalog.credential")
		oauth := mapping()
		id, secret, _ := strings.Cut(cred, ":")
		kv(oauth, "client_id", scalar(id))
		sec := scalar(secret)
		sec.LineComment = "TODO: client secret is inlined — move to a secret/env-var reference"
		kv(oauth, "client_secret", sec)
		if su, ok := ctx.String("iceberg.catalog.oauth2-server-uri"); ok && su != "" {
			kv(oauth, "server_uri", scalar(su))
		}
		if sc, ok := ctx.String("iceberg.catalog.scope"); ok && sc != "" {
			kv(oauth, "scope", scalar(sc))
		}
		auth := mapping()
		kv(auth, "oauth2", oauth)
		kv(catalog, "auth", auth)
		ctx.consume("iceberg.catalog.token")
	case tokenPresent(ctx):
		tok, _ := ctx.String("iceberg.catalog.token")
		b := scalar(tok)
		b.LineComment = "TODO: bearer token is inlined — move to a secret/env-var reference"
		auth := mapping()
		kv(auth, "bearer", b)
		kv(catalog, "auth", auth)
		ctx.consume("iceberg.catalog.oauth2-server-uri")
		ctx.consume("iceberg.catalog.scope")
	case isGlue:
		sig := mapping()
		if region, ok := ctx.String("iceberg.catalog.client.region"); ok && region != "" {
			kv(sig, "region", scalar(region))
		}
		kv(sig, "service", scalar("glue"))
		auth := mapping()
		kv(auth, "aws_sigv4", sig)
		kv(catalog, "auth", auth)
	}

	return catalog
}

func credPresent(ctx *MapCtx) bool {
	v, ok := ctx.Lookup("iceberg.catalog.credential")
	return ok && strings.TrimSpace(v) != ""
}

func tokenPresent(ctx *MapCtx) bool {
	v, ok := ctx.Lookup("iceberg.catalog.token")
	return ok && strings.TrimSpace(v) != ""
}

// icebergNamespaceTable derives the namespace + table from iceberg.tables (or
// dynamic routing). RPCN writes one table per output, so multiple listed tables
// keep the first with a TODO, and dynamic routing becomes an interpolation.
func icebergNamespaceTable(ctx *MapCtx) (nsNode, tblNode *yaml.Node) {
	dynamic, _ := ctx.String("iceberg.tables.dynamic-enabled")
	routeField, _ := ctx.String("iceberg.tables.route-field")
	if strings.EqualFold(strings.TrimSpace(dynamic), "true") && routeField != "" {
		tblNode = scalar("${! this." + routeField + " }")
		tblNode.LineComment = "TODO: dynamic routing on field " + routeField + " — its value should be the destination <namespace>.<table>; verify"
		nsNode = scalar("")
		nsNode.LineComment = "TODO: set the default Iceberg namespace (dynamic routing supplies the table name)"
		return nsNode, tblNode
	}

	tables, ok := ctx.String("iceberg.tables")
	if !ok || strings.TrimSpace(tables) == "" {
		nsNode = scalar("")
		nsNode.LineComment = "TODO: set the Iceberg namespace"
		tblNode = scalar("")
		tblNode.LineComment = "TODO: set the Iceberg table"
		return nsNode, tblNode
	}

	entries := csvFields(tables)
	ns, table := splitNamespaceTable(entries[0])
	nsNode = scalar(ns)
	if ns == "" {
		nsNode.LineComment = "TODO: set the Iceberg namespace (iceberg.tables entry had no namespace prefix)"
	}
	tblNode = scalar(table)
	if len(entries) > 1 {
		tblNode.LineComment = "TODO: KC iceberg.tables listed multiple tables; RPCN iceberg writes one — use a ${! ... } interpolation or one output per table"
	}
	return nsNode, tblNode
}

// splitNamespaceTable splits "a.b.events" into namespace "a.b" + table "events".
func splitNamespaceTable(s string) (ns, table string) {
	s = strings.TrimSpace(s)
	if i := strings.LastIndex(s, "."); i >= 0 {
		return s[:i], s[i+1:]
	}
	return "", s
}

// icebergStorage emits the data-file storage backend (exactly one is required
// by the iceberg output). It picks the backend from io-impl / the warehouse URI
// scheme and extracts the bucket/container from the warehouse location.
func icebergStorage(ctx *MapCtx) *yaml.Node {
	storage := mapping()
	ioImpl, _ := ctx.Lookup("iceberg.catalog.io-impl")
	warehouse, _ := ctx.Lookup("iceberg.catalog.warehouse")
	scheme, bucket := parseObjectURI(warehouse)

	switch {
	case strings.Contains(ioImpl, "GCSFileIO") || scheme == "gs":
		gcs := mapping()
		kv(gcs, "bucket", bucketScalar(bucket, "GCS"))
		kv(storage, "gcp_cloud_storage", gcs)
	case strings.Contains(ioImpl, "ADLSFileIO") || scheme == "abfs" || scheme == "abfss" || scheme == "wasb" || scheme == "wasbs":
		az := mapping()
		container := scalar(bucket)
		if bucket == "" {
			container.LineComment = "TODO: set the Azure blob container"
		}
		kv(az, "container", container)
		acct := scalar("")
		acct.LineComment = "TODO: set the Azure storage account name"
		kv(az, "storage_account", acct)
		kv(storage, "azure_blob_storage", az)
	default:
		// Default to S3 (the most common Iceberg FileIO, incl. S3FileIO and
		// s3://, s3a:// warehouse schemes).
		s3 := mapping()
		kv(s3, "bucket", bucketScalar(bucket, "S3"))
		if r, ok := ctx.String("iceberg.catalog.client.region"); ok && r != "" {
			kv(s3, "region", scalar(r))
		}
		if e, ok := ctx.String("iceberg.catalog.s3.endpoint"); ok && e != "" {
			kv(s3, "endpoint", scalar(e))
			kv(s3, "force_path_style_urls", boolScalar(true))
		}
		kv(storage, "aws_s3", s3)
	}
	return storage
}

func bucketScalar(bucket, backend string) *yaml.Node {
	n := scalar(bucket)
	if bucket == "" {
		n.LineComment = "TODO: set the " + backend + " bucket backing the Iceberg warehouse"
	}
	return n
}

// parseObjectURI splits "scheme://bucket/path" → (scheme, bucket). For Azure
// abfss://container@account.../ forms it returns the container.
func parseObjectURI(uri string) (scheme, bucket string) {
	uri = strings.TrimSpace(uri)
	before, rest, ok := strings.Cut(uri, "://")
	if !ok {
		return "", ""
	}
	scheme = before
	if j := strings.IndexByte(rest, '/'); j >= 0 {
		rest = rest[:j]
	}
	if k := strings.IndexByte(rest, '@'); k >= 0 {
		rest = rest[:k]
	}
	return scheme, rest
}

// icebergSchemaEvolution emits the schema_evolution block when evolution is
// enabled or a table location must be supplied (Glue).
func icebergSchemaEvolution(ctx *MapCtx, body *yaml.Node, isGlue bool) {
	evolve, _ := ctx.String("iceberg.tables.evolve-schema-enabled")
	enabled := strings.EqualFold(strings.TrimSpace(evolve), "true")

	var tableLoc string
	if isGlue {
		if w, ok := ctx.Lookup("iceberg.catalog.warehouse"); ok && w != "" {
			tableLoc = strings.TrimSuffix(w, "/") + "/"
		}
	}

	if !enabled && tableLoc == "" {
		return
	}
	se := mapping()
	if enabled {
		kv(se, "enabled", boolScalar(true))
	}
	if tableLoc != "" {
		loc := scalar(tableLoc)
		loc.LineComment = "TODO: AWS Glue does not assign table locations — verify this S3 prefix"
		kv(se, "table_location", loc)
	}
	kv(body, "schema_evolution", se)
}
