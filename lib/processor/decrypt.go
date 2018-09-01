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
	Constructors[TypeDecrypt] = TypeSpec{
		constructor: NewDecrypt,
		description: `
Decrypts parts of a message according to the selected scheme. Supported available
schemes are: pgp.`,
	}
}

//------------------------------------------------------------------------------

// DecryptConfig contains configuration fields for the Decrypt processor.
type DecryptConfig struct {
	Scheme     string `json:"scheme" yaml:"scheme"`
	Key        string `json:"key" yaml:"key"`
	Parts      []int  `json:"parts" yaml:"parts"`
}

// NewDecryptConfig returns a DecryptConfig with default values.
func NewDecryptConfig() DecryptConfig {
	return DecryptConfig{
		Scheme:     "pgp",
		Key:        "",
		Parts:      []int{},
	}
}

//------------------------------------------------------------------------------

type decryptFunc func(key []byte, bytes []byte) ([]byte, error)

func pgpDecrypt(key []byte, b []byte) ([]byte, error) {
	// decode armor
	keyBlock, err := armor.Decode(bytes.NewReader(key))
	if err != nil {
		return nil, err
	}
  // check key type
	if keyBlock.Type != openpgp.PrivateKeyType {
		return nil, fmt.Errorf("invalid key type: %v", keyBlock.Type)
	}

	// add key to key list
	keyReader := packet.NewReader(keyBlock.Body)
	keyEntity, err := openpgp.ReadEntity(keyReader)
	entityList := &openpgp.EntityList{keyEntity}

	m := bytes.NewReader(b)
	messageBlock, err := armor.Decode(m)
	if err != nil {
		return nil, err
	}
	message, err := openpgp.ReadMessage(messageBlock.Body, entityList, nil, nil)
	if err != nil {
		return nil, err
	}

	return ioutil.ReadAll(message.UnverifiedBody)
}

func strToDecryptor(str string) (decryptFunc, error) {
	switch str {
	case "pgp":
		return pgpDecrypt, nil
	}
	return nil, fmt.Errorf("decrypt scheme not recognised: %v", str)
}

//------------------------------------------------------------------------------

// Decrypt is a processor that can selectively decrypt parts of a message
// following a chosen scheme.
type Decrypt struct {
	conf DecryptConfig
	fn   decryptFunc
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

// NewDecrypt returns a Decrypt processor.
func NewDecrypt(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	key, err := ioutil.ReadFile(conf.Decrypt.Key)
	if err != nil {
		return nil, err
	}
	cor, err := strToDecryptor(conf.Decrypt.Scheme)
	if err != nil {
		return nil, err
	}
	return &Decrypt{
		conf:  conf.Decrypt,
		fn:    cor,
		key:   key,
		log:   log.NewModule(".processor.decrypt"),
		stats: stats,

		mCount:     stats.GetCounter("processor.decrypt.count"),
		mSucc:      stats.GetCounter("processor.decrypt.success"),
		mErr:       stats.GetCounter("processor.decrypt.error"),
		mSkipped:   stats.GetCounter("processor.decrypt.skipped"),
		mSent:      stats.GetCounter("processor.decrypt.sent"),
		mSentParts: stats.GetCounter("processor.decrypt.parts.sent"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (c *Decrypt) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
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
			c.log.Errorf("Failed to decrypt message part: %v\n", err)
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
