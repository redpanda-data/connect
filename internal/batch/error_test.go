package batch

import (
	"errors"
	"fmt"
	"testing"

	"github.com/benthosdev/benthos/v4/internal/message"

	"github.com/stretchr/testify/assert"
)

func TestErrorBatchAssociation(t *testing.T) {
	batch := message.Batch{
		message.NewPart([]byte("foo")),
		message.NewPart([]byte("bar")),
		message.NewPart([]byte("baz")),
	}

	group, batch := message.NewSortGroup(batch)

	bErr := NewError(batch, errors.New("nope"))
	bErr = bErr.Failed(1, errors.New("yas"))

	partsWalked := map[int]string{}
	bErr.WalkParts(group, batch, func(i int, p *message.Part, err error) bool {
		partsWalked[i] = fmt.Sprintf("%s - %v", p.AsBytes(), err)
		return true
	})

	assert.Equal(t, map[int]string{
		0: "foo - <nil>",
		1: "bar - yas",
		2: "baz - <nil>",
	}, partsWalked)
}

func TestErrorBatchAssociationShallowCopy(t *testing.T) {
	batch := message.Batch{
		message.NewPart([]byte("foo")),
		message.NewPart([]byte("bar")),
		message.NewPart([]byte("baz")),
	}

	group, batch := message.NewSortGroup(batch)

	bErr := NewError(batch.ShallowCopy(), errors.New("nope"))
	bErr = bErr.Failed(1, errors.New("yas"))

	partsWalked := map[int]string{}
	bErr.WalkParts(group, batch, func(i int, p *message.Part, err error) bool {
		partsWalked[i] = fmt.Sprintf("%s - %v", p.AsBytes(), err)
		return true
	})

	assert.Equal(t, map[int]string{
		0: "foo - <nil>",
		1: "bar - yas",
		2: "baz - <nil>",
	}, partsWalked)
}

func TestErrorBatchAssociationDeepCopy(t *testing.T) {
	batch := message.Batch{
		message.NewPart([]byte("foo")),
		message.NewPart([]byte("bar")),
		message.NewPart([]byte("baz")),
	}

	group, batch := message.NewSortGroup(batch)

	bErr := NewError(batch.DeepCopy(), errors.New("nope"))
	bErr = bErr.Failed(1, errors.New("yas"))

	partsWalked := map[int]string{}
	bErr.WalkParts(group, batch, func(i int, p *message.Part, err error) bool {
		partsWalked[i] = fmt.Sprintf("%s - %v", p.AsBytes(), err)
		return true
	})

	assert.Equal(t, map[int]string{
		0: "foo - <nil>",
		1: "bar - yas",
		2: "baz - <nil>",
	}, partsWalked)
}

func TestErrorBatchAssociationNested(t *testing.T) {
	batch := message.Batch{
		message.NewPart([]byte("foo")),
		message.NewPart([]byte("bar")),
		message.NewPart([]byte("baz")),
	}

	group, batch := message.NewSortGroup(batch)

	_, batch2 := message.NewSortGroup(batch.ShallowCopy().DeepCopy())

	bErr := NewError(batch2, errors.New("nope"))
	bErr = bErr.Failed(1, errors.New("yas"))

	partsWalked := map[int]string{}
	bErr.WalkParts(group, batch, func(i int, p *message.Part, err error) bool {
		partsWalked[i] = fmt.Sprintf("%s - %v", p.AsBytes(), err)
		return true
	})

	assert.Equal(t, map[int]string{
		0: "foo - <nil>",
		1: "bar - yas",
		2: "baz - <nil>",
	}, partsWalked)
}

func TestErrorBatchAssociationDoubleNested(t *testing.T) {
	batch := message.Batch{
		message.NewPart([]byte("foo")),
		message.NewPart([]byte("bar")),
		message.NewPart([]byte("baz")),
	}

	group, batch := message.NewSortGroup(batch)

	_, batch2 := message.NewSortGroup(batch.ShallowCopy().DeepCopy())
	_, batch3 := message.NewSortGroup(batch2.ShallowCopy().DeepCopy())

	bErr := NewError(batch3, errors.New("nope"))
	bErr = bErr.Failed(1, errors.New("yas"))

	partsWalked := map[int]string{}
	bErr.WalkParts(group, batch, func(i int, p *message.Part, err error) bool {
		partsWalked[i] = fmt.Sprintf("%s - %v", p.AsBytes(), err)
		return true
	})

	assert.Equal(t, map[int]string{
		0: "foo - <nil>",
		1: "bar - yas",
		2: "baz - <nil>",
	}, partsWalked)
}
