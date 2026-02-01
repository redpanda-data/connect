/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package icebergx

import (
	"fmt"

	"github.com/apache/iceberg-go"
)

// ApplyPartitionTransforms applies the partition transforms to the given raw values from a raw.
func ApplyPartitionTransforms(spec iceberg.PartitionSpec, pk PartitionKey) (PartitionKey, error) {
	transformed := make(PartitionKey, len(pk))
	if len(pk) != spec.NumFields() {
		return nil, fmt.Errorf("different number of fields and values: %d vs %d", len(pk), spec.NumFields())
	}
	for i := range len(pk) {
		v := pk[i]
		f := spec.Field(i)
		transformed[i] = f.Transform.Apply(v)
	}
	return transformed, nil
}
