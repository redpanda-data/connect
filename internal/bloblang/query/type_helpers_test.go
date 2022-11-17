package query

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func TestIToBytes(t *testing.T) {
	vb := IToBytes(uint64(12345))
	assert.Equal(t, "12345", string(vb))

	vb = IToBytes(true)
	assert.Equal(t, "true", string(vb))

	vb = IToBytes(float64(1.2345))
	assert.Equal(t, "1.2345", string(vb))

	vb = IToBytes(float64(1.234567891234567891234567))
	assert.Equal(t, "1.234567891234568", string(vb))

	vb = IToBytes(float64(1.23 * 4.567 * 1_000_000_000))
	assert.Equal(t, "5.61741e+09", string(vb))
}

func TestObjectId(t *testing.T) {
	objectIdStr := "637452307599bdc8edbc367a"
	testObjectId, _ := primitive.ObjectIDFromHex(objectIdStr)
	time, _ := time.Parse("2006-01-02T15:04:05Z", "2022-11-16T03:00:00Z")

	assert.Equal(t, objectIdStr, IToString(testObjectId))

	timestamp, err := IGetTimestamp(time)
	assert.NoError(t, err)
	assert.Equal(t, time, timestamp)
}
