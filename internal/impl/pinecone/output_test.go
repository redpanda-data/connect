// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pinecone

import (
	"context"
	"math/rand"
	"slices"
	"testing"

	"github.com/pinecone-io/go-pinecone/pinecone"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
)

type mockClient struct {
	data            map[string]map[string]map[string]*pinecone.Vector
	openConnections int
}

func (c *mockClient) Index(host string) (indexClient, error) {
	i := c.data[host]
	if i == nil {
		c.data[host] = map[string]map[string]*pinecone.Vector{}
		i = c.data[host]
	}
	c.openConnections++
	return &mockIndexClient{index: i, openConnections: &c.openConnections}, nil
}

func (c *mockClient) Write(host string, ns string, value *pinecone.Vector) {
	idx, _ := c.Index(host)
	idx.SetNamespace(ns)
	_ = idx.UpsertVectors(context.Background(), []*pinecone.Vector{value})
}

func (c *mockClient) Get(host, ns, id string) *pinecone.Vector {
	h, ok := c.data[host]
	if !ok {
		return nil
	}
	n, ok := h[ns]
	if !ok {
		return nil
	}
	return n[id]
}

type mockIndexClient struct {
	namespace       string
	index           map[string]map[string]*pinecone.Vector
	openConnections *int
}

func (c *mockIndexClient) SetNamespace(namespace string) {
	c.namespace = namespace
}

func (c *mockIndexClient) GetNamespace() map[string]*pinecone.Vector {
	idx := c.index[c.namespace]
	if idx == nil {
		c.index[c.namespace] = map[string]*pinecone.Vector{}
		idx = c.index[c.namespace]
	}
	return idx
}

func (c *mockIndexClient) UpdateVector(ctx context.Context, req *pinecone.UpdateVectorRequest) error {
	vectors := c.GetNamespace()
	entry, ok := vectors[req.Id]
	if !ok {
		return nil
	}
	entry.Id = req.Id
	entry.Values = req.Values
	entry.SparseValues = req.SparseValues
	entry.Metadata = req.Metadata
	return nil
}

func (c *mockIndexClient) UpsertVectors(ctx context.Context, batch []*pinecone.Vector) error {
	vectors := c.GetNamespace()
	for _, req := range batch {
		entry, ok := vectors[req.Id]
		if !ok {
			vectors[req.Id] = &pinecone.Vector{}
			entry = vectors[req.Id]
		}
		entry.Id = req.Id
		entry.Values = req.Values
		entry.SparseValues = req.SparseValues
		entry.Metadata = req.Metadata
	}
	return nil
}

func (c *mockIndexClient) DeleteVectorsByID(ctx context.Context, ids []string) error {
	vectors := c.GetNamespace()
	for _, id := range ids {
		delete(vectors, id)
	}
	return nil
}

func (c *mockIndexClient) Close() error {
	*c.openConnections--
	return nil
}

type mockMessage struct {
	namespace string
	id        string
	vector    []float32
}

func (m *mockMessage) AsVector() *pinecone.Vector {
	return &pinecone.Vector{
		Id:     m.id,
		Values: slices.Clone(m.vector),
	}
}

func (m *mockMessage) AsMessage() *service.Message {
	msg := service.NewMessage(nil)
	vec := make([]any, len(m.vector))
	for i, f := range m.vector {
		vec[i] = f
	}
	msg.SetStructuredMut(vec)
	msg.MetaSetMut("ns", m.namespace)
	msg.MetaSetMut("id", m.id)
	return msg
}

func newMessage(ns string, id string) mockMessage {
	vec := make([]float32, 384)
	for i := range vec {
		vec[i] = rand.Float32()
	}
	return mockMessage{ns, id, vec}
}

func setup(op operation) (*outputWriter, *mockClient) {
	c := mockClient{
		data: map[string]map[string]map[string]*pinecone.Vector{},
	}
	nsMapping, err := service.NewInterpolatedString(`${! meta("ns") }`)
	if err != nil {
		panic(err)
	}
	idMapping, err := service.NewInterpolatedString(`${! meta("id") }`)
	if err != nil {
		panic(err)
	}
	vectorMapping, err := bloblang.GlobalEnvironment().Parse("root = this")
	if err != nil {
		panic(err)
	}
	w := outputWriter{
		client:        &c,
		host:          "foobar.arpa",
		op:            op,
		namespace:     nsMapping,
		id:            idMapping,
		vectorMapping: vectorMapping,
	}
	return &w, &c
}

func TestUpdate(t *testing.T) {
	w, c := setup(operationUpdate)
	c.Write(w.host, "foo", &pinecone.Vector{Id: "bar", Values: []float32{1, 2, 3}})
	m1 := newMessage("foo", "bar")
	m2 := newMessage("foo", "qux")
	m3 := newMessage("fuzz", "bar")
	err := w.WriteBatch(context.Background(), service.MessageBatch{m1.AsMessage(), m2.AsMessage(), m3.AsMessage()})
	require.NoError(t, err)
	require.Equal(t, m1.AsVector(), c.Get(w.host, m1.namespace, m1.id))
	require.Nil(t, c.Get(w.host, m3.namespace, m2.id))
	require.Nil(t, c.Get(w.host, m3.namespace, m3.id))
}

func TestUpsert(t *testing.T) {
	w, c := setup(operationUpsert)
	c.Write(w.host, "foo", &pinecone.Vector{Id: "bar", Values: []float32{1, 2, 3}})
	m1 := newMessage("foo", "bar")
	m2 := newMessage("foo", "qux")
	m3 := newMessage("fuzz", "bar")
	err := w.WriteBatch(context.Background(), service.MessageBatch{m1.AsMessage(), m2.AsMessage(), m3.AsMessage()})
	require.NoError(t, err)
	for _, m := range []mockMessage{m1, m2, m3} {
		require.Equal(t, m.AsVector(), c.Get(w.host, m.namespace, m.id))
	}
}

func TestDelete(t *testing.T) {
	w, c := setup(operationDelete)
	c.Write(w.host, "foo", &pinecone.Vector{Id: "bar", Values: []float32{1, 2, 3}})
	c.Write(w.host, "fuzz", &pinecone.Vector{Id: "qux", Values: []float32{1, 2, 3}})
	m1 := newMessage("foo", "bar")
	m2 := newMessage("foo", "qux")
	m3 := newMessage("fuzz", "bar")
	err := w.WriteBatch(context.Background(), service.MessageBatch{m1.AsMessage(), m2.AsMessage(), m3.AsMessage()})
	require.NoError(t, err)
	for _, m := range []mockMessage{m1, m2, m3} {
		require.Nil(t, c.Get(w.host, m.namespace, m.id))
	}
	require.NotNil(t, c.Get(w.host, "fuzz", "qux"))
}

func TestMapping(t *testing.T) {
	w, c := setup(operationUpsert)
	var err error
	w.vectorMapping, err = bloblang.GlobalEnvironment().Parse("this.map_each(v -> v * 2)")
	require.NoError(t, err)
	m := newMessage("foo", "bar")
	err = w.WriteBatch(context.Background(), service.MessageBatch{m.AsMessage()})
	require.NoError(t, err)
	for i, v := range m.vector {
		m.vector[i] = v * 2
	}
	require.Equal(t, m.AsVector(), c.Get(w.host, m.namespace, m.id))
}
