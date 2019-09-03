// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package processor

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/OneOfOne/xxhash"
	"github.com/opentracing/opentracing-go"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHash] = TypeSpec{
		constructor: NewHash,
		description: `
Hashes messages according to the selected algorithm. Supported algorithms are:
sha256, sha512, sha1, xxhash64.

This processor is mostly useful when combined with the
` + "[`process_field`](#process_field)" + ` processor as it allows you to hash a
specific field of a document like this:

` + "``` yaml" + `
# Hash the contents of 'foo.bar'
process_field:
  path: foo.bar
  processors:
  - hash:
      algorithm: sha256
` + "```" + ``,
	}
}

//------------------------------------------------------------------------------

// HashConfig contains configuration fields for the Hash processor.
type HashConfig struct {
	Parts     []int  `json:"parts" yaml:"parts"`
	Algorithm string `json:"algorithm" yaml:"algorithm"`
}

// NewHashConfig returns a HashConfig with default values.
func NewHashConfig() HashConfig {
	return HashConfig{
		Parts:     []int{},
		Algorithm: "sha256",
	}
}

//------------------------------------------------------------------------------

type hashFunc func(bytes []byte) ([]byte, error)

func sha1Hash(b []byte) ([]byte, error) {
	hasher := sha1.New()
	hasher.Write(b)
	return hasher.Sum(nil), nil
}

func sha256Hash(b []byte) ([]byte, error) {
	hasher := sha256.New()
	hasher.Write(b)
	return hasher.Sum(nil), nil
}

func sha512Hash(b []byte) ([]byte, error) {
	hasher := sha512.New()
	hasher.Write(b)
	return hasher.Sum(nil), nil
}

func xxhash64Hash(b []byte) ([]byte, error) {
	h := xxhash.New64()
	h.Write(b)
	return []byte(strconv.FormatUint(h.Sum64(), 10)), nil
}

func strToHashr(str string) (hashFunc, error) {
	switch str {
	case "sha1":
		return sha1Hash, nil
	case "sha256":
		return sha256Hash, nil
	case "sha512":
		return sha512Hash, nil
	case "xxhash64":
		return xxhash64Hash, nil
	}
	return nil, fmt.Errorf("hash algorithm not recognised: %v", str)
}

//------------------------------------------------------------------------------

// Hash is a processor that can selectively hash parts of a message following a
// chosen algorithm.
type Hash struct {
	conf HashConfig
	fn   hashFunc

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewHash returns a Hash processor.
func NewHash(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	cor, err := strToHashr(conf.Hash.Algorithm)
	if err != nil {
		return nil, err
	}
	return &Hash{
		conf:  conf.Hash,
		fn:    cor,
		log:   log,
		stats: stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (c *Hash) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)

	newMsg := msg.Copy()

	proc := func(index int, span opentracing.Span, part types.Part) error {
		newPart, err := c.fn(part.Get())
		if err == nil {
			newMsg.Get(index).Set(newPart)
		} else {
			c.log.Debugf("Failed to hash message part: %v\n", err)
			c.mErr.Incr(1)
		}
		return err
	}

	if newMsg.Len() == 0 {
		return nil, response.NewAck()
	}

	IteratePartsWithSpan(TypeHash, c.conf.Parts, newMsg, proc)

	c.mBatchSent.Incr(1)
	c.mSent.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (c *Hash) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (c *Hash) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
