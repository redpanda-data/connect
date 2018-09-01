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
	"bytes"
	"fmt"
	"golang.org/x/crypto/openpgp"
	"golang.org/x/crypto/openpgp/armor"
	"golang.org/x/crypto/openpgp/packet"
	"io/ioutil"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/response"
	"github.com/Jeffail/benthos/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeEncrypt] = TypeSpec{
		constructor: NewEncrypt,
		description: `
Encrypts parts of a message according to the selected scheme. Supported schemes
are: pgp.`,
	}
}

//------------------------------------------------------------------------------

// EncryptConfig contains configuration fields for the Encrypt processor.
type EncryptConfig struct {
	Scheme string `json:"scheme" yaml:"scheme"`
	Key    string `json:"key" yaml:"key"`
	Parts  []int  `json:"parts" yaml:"parts"`
}

// NewEncryptConfig returns a EncryptConfig with default values.
func NewEncryptConfig() EncryptConfig {
	return EncryptConfig{
		Scheme: "pgp",
		Key:    "",
		Parts:  []int{},
	}
}

//------------------------------------------------------------------------------

type encryptFunc func(key []byte, bytes []byte) ([]byte, error)

func pgpEncrypt(key []byte, b []byte) ([]byte, error) {
	// decode armor
	keyBlock, err := armor.Decode(bytes.NewReader(key))
	if err != nil {
		return nil, err
	}

	// add key to key list
	keyReader := packet.NewReader(keyBlock.Body)
	keyEntity, err := openpgp.ReadEntity(keyReader)

	encyptedBuffer := bytes.NewBuffer(nil)
	plainText, err := openpgp.Encrypt(encyptedBuffer, []*openpgp.Entity{keyEntity}, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	_, err = plainText.Write(b)
	if err != nil {
		return nil, err
	}
	plainText.Close()

	return encyptedBuffer.Bytes(), nil
}

func strToEncryptr(str string) (encryptFunc, error) {
	switch str {
	case "pgp":
		return pgpEncrypt, nil
	}
	return nil, fmt.Errorf("encrypt scheme not recognised: %v", str)
}

//------------------------------------------------------------------------------

// Encrypt is a processor that can selectively encrypt parts of a message
// following a chosen scheme.
type Encrypt struct {
	conf EncryptConfig
	fn   encryptFunc
	key  []byte

	log   log.Modular
	stats metrics.Type

	mCount     metrics.StatCounter
	mSucc      metrics.StatCounter
	mErr       metrics.StatCounter
	mSkipped   metrics.StatCounter
	mSent      metrics.StatCounter
	mSentParts metrics.StatCounter
}

// NewEncrypt returns a Encrypt processor.
func NewEncrypt(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	key, err := ioutil.ReadFile(conf.Encrypt.Key)
	if err != nil {
		return nil, err
	}
	cor, err := strToEncryptr(conf.Encrypt.Scheme)
	if err != nil {
		return nil, err
	}
	return &Encrypt{
		conf:  conf.Encrypt,
		fn:    cor,
		key:   key,
		log:   log.NewModule(".processor.encrypt"),
		stats: stats,

		mCount:     stats.GetCounter("processor.encrypt.count"),
		mSucc:      stats.GetCounter("processor.encrypt.success"),
		mErr:       stats.GetCounter("processor.encrypt.error"),
		mSkipped:   stats.GetCounter("processor.encrypt.skipped"),
		mSent:      stats.GetCounter("processor.encrypt.sent"),
		mSentParts: stats.GetCounter("processor.encrypt.parts.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (c *Encrypt) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	c.mCount.Incr(1)

	newMsg := msg.Copy()

	targetParts := c.conf.Parts
	if len(targetParts) == 0 {
		targetParts = make([]int, newMsg.Len())
		for i := range targetParts {
			targetParts[i] = i
		}
	}

	for _, index := range targetParts {
		part := msg.Get(index).Get()
		newPart, err := c.fn(c.key, part)
		if err == nil {
			c.mSucc.Incr(1)
			newMsg.Get(index).Set(newPart)
		} else {
			c.log.Debugf("Failed to encrypt message part: %v\n", err)
			c.mErr.Incr(1)
		}
	}

	if newMsg.Len() == 0 {
		c.mSkipped.Incr(1)
		return nil, response.NewAck()
	}

	c.mSent.Incr(1)
	c.mSentParts.Incr(int64(newMsg.Len()))
	msgs := [1]types.Message{newMsg}
	return msgs[:], nil
}

//------------------------------------------------------------------------------
