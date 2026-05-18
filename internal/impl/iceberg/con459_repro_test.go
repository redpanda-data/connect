// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package iceberg

import (
	"bytes"
	"testing"

	icebergpkg "github.com/apache/iceberg-go"
	"github.com/stretchr/testify/require"
)

// TestCON459_V2ManifestListAcceptsV1Manifest pins the upstream fix from
// apache/iceberg-go#1030 against the iceberg-go version Connect currently
// imports. The pinned dependency must allow a v2 ManifestListWriter to
// reference v1 manifest files; otherwise every commit on a table that was
// upgraded from v1 to v2 (via Transaction.UpgradeFormatVersion or
// out-of-band) explodes when existingManifests() surfaces the historical
// v1 manifests. If this test starts failing after a future bump, the bug
// has regressed upstream.
func TestCON459_V2ManifestListAcceptsV1Manifest(t *testing.T) {
	v1Manifest := icebergpkg.NewManifestFile(
		1,
		"/warehouse/test/metadata/0125c686-8aa6-4502-bdcc-b6d17ca41a3b-m0.avro",
		7989,
		0,
		9182715666859759686,
	).
		AddedFiles(3).
		ExistingFiles(0).
		DeletedFiles(0).
		AddedRows(237993).
		ExistingRows(0).
		DeletedRows(0).
		Build()

	require.Equal(t, 1, v1Manifest.Version(), "sanity: built manifest is v1")

	var buf bytes.Buffer
	seqNum := int64(1)
	err := icebergpkg.WriteManifestList(
		2,
		&buf,
		9182715666859759686,
		nil,
		&seqNum,
		0,
		[]icebergpkg.ManifestFile{v1Manifest},
	)
	require.NoError(t, err, "v2 manifest list must accept v1 manifests")

	got, err := icebergpkg.ReadManifestList(&buf)
	require.NoError(t, err)
	require.Len(t, got, 1)

	entry := got[0]
	require.Equal(t, v1Manifest.FilePath(), entry.FilePath())
	require.Equal(t, icebergpkg.ManifestContentData, entry.ManifestContent(),
		"v1 manifests inside a v2 list must inherit content=data")
	require.Equal(t, int64(0), entry.SequenceNum(),
		"v1 manifests inside a v2 list must inherit sequence_number=0")
	require.Equal(t, int64(0), entry.MinSequenceNum(),
		"v1 manifests inside a v2 list must inherit min_sequence_number=0")
}
