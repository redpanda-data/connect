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

// +build !wasm

package parallel

import (
	"bytes"
	"encoding/binary"
	"sync"

	"github.com/Jeffail/benthos/v3/lib/message/io"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/boltdb/bolt"
)

//------------------------------------------------------------------------------

// Static keys holding BoltDB bucket names for the Benthos buffer.
var (
	BoltDBMessagesBucket = []byte("benthos_messages")
)

type boltDBItem struct {
	key []byte
	msg types.Message
}

// BoltDBConfig contains configuration params for the BoltDB buffer type.
type BoltDBConfig struct {
	File          string `json:"file" yaml:"file"`
	PrefetchCount int    `json:"prefetch_count" yaml:"prefetch_count"`
}

// NewBoltDBConfig returns a BoltDBConfig with default parameters.
func NewBoltDBConfig() BoltDBConfig {
	return BoltDBConfig{
		File:          "",
		PrefetchCount: 50,
	}
}

// BoltDB is a parallel buffer implementation that allows multiple parallel
// consumers to read and purge messages from a BoltDB database asynchronously.
type BoltDB struct {
	running bool

	db            *bolt.DB
	prefetchCount int

	cursor []byte

	backlog     []*boltDBItem
	backlogLock sync.Mutex

	cond *sync.Cond
}

// NewBoltDB creates a memory based parallel buffer.
func NewBoltDB(conf BoltDBConfig) (*BoltDB, error) {
	// Open the database.
	db, err := bolt.Open(conf.File, 0666, nil)
	if err != nil {
		return nil, err
	}

	if err = db.Update(func(tx *bolt.Tx) error {
		_, terr := tx.CreateBucketIfNotExists(BoltDBMessagesBucket)
		return terr
	}); err != nil {
		return nil, err
	}

	return &BoltDB{
		running:       true,
		db:            db,
		prefetchCount: conf.PrefetchCount,
		cond:          sync.NewCond(&sync.Mutex{}),
	}, nil
}

//------------------------------------------------------------------------------

func (m *BoltDB) prefetch() ([]*boltDBItem, error) {
	items := []*boltDBItem{}

	if err := m.db.View(func(tx *bolt.Tx) error {
		var tmpKey, tmpMsgBytes []byte

		bucket := tx.Bucket(BoltDBMessagesBucket)
		cursor := bucket.Cursor()
		if m.cursor == nil {
			tmpKey, tmpMsgBytes = cursor.First()
		} else {
			tmpKey, tmpMsgBytes = cursor.Seek(m.cursor)
			if bytes.Equal(tmpKey, m.cursor) {
				tmpKey, tmpMsgBytes = cursor.Next()
			}
		}

		for i := 0; i < m.prefetchCount; i++ {
			if tmpKey == nil {
				break
			}

			newItem := &boltDBItem{}
			newItem.key = make([]byte, len(tmpKey))
			copy(newItem.key, tmpKey)
			tmpMsg, txerr := io.MessageFromJSON(tmpMsgBytes)
			if txerr != nil {
				return txerr
			}
			newItem.msg = tmpMsg.DeepCopy()
			items = append(items, newItem)
			m.cursor = newItem.key

			tmpKey, tmpMsgBytes = cursor.Next()
		}
		return nil
	}); err != nil {
		if err == bolt.ErrDatabaseNotOpen {
			err = types.ErrTypeClosed
		}
		return nil, err
	}

	return items, nil
}

// NextMessage reads the next oldest message, the message is preserved until the
// returned AckFunc is called.
func (m *BoltDB) NextMessage() (types.Message, AckFunc, error) {
	var key []byte
	var msg types.Message

	for key == nil {
		m.backlogLock.Lock()
		if !m.running {
			m.backlogLock.Unlock()
			return nil, nil, types.ErrTypeClosed
		}
		if len(m.backlog) > 0 {
			item := m.backlog[0]
			m.backlog = m.backlog[1:]
			key = item.key
			msg = item.msg
		}
		m.backlogLock.Unlock()
		if key != nil {
			break
		}

		// Try IO without lock.
		items, err := m.prefetch()
		if err != nil {
			return nil, nil, err
		}
		if len(items) == 0 {
			// Try IO with lock and wait for write broadcast afterwards if we're
			// still empty.
			m.cond.L.Lock()
			if !m.running {
				m.cond.L.Unlock()
				return nil, nil, types.ErrTypeClosed
			}
			items, err = m.prefetch()
			if err != nil {
				return nil, nil, err
			}
			if len(items) == 0 {
				m.cond.Wait()
			}
			m.cond.L.Unlock()
		}
		if lItems := len(items); lItems > 0 {
			key = items[0].key
			msg = items[0].msg
			if lItems > 1 {
				m.backlogLock.Lock()
				m.backlog = append(m.backlog, items[1:]...)
				m.backlogLock.Unlock()
			}
		}
	}

	delFn := func() (backlog int, err error) {
		err = m.db.Batch(func(tx *bolt.Tx) error {
			bucket := tx.Bucket(BoltDBMessagesBucket)
			if derr := bucket.Delete(key); derr != nil {
				return derr
			}
			backlog = bucket.Stats().LeafAlloc
			return nil
		})
		if err == bolt.ErrDatabaseNotOpen {
			err = types.ErrTypeClosed
		}
		return
	}
	backlogFn := func() {
		m.backlogLock.Lock()
		m.backlog = append([]*boltDBItem{
			{
				key: key,
				msg: msg,
			},
		}, m.backlog...)
		m.backlogLock.Unlock()
	}

	return msg, func(ack bool) (int, error) {
		if ack {
			return delFn()
		}
		backlogFn()
		return 0, nil
	}, nil
}

// PushMessage adds a message to the stack. Returns the backlog in bytes.
func (m *BoltDB) PushMessage(msg types.Message) (int, error) {
	return m.PushMessages([]types.Message{msg})
}

// PushMessages adds a slice of new messages to the stack. Returns the backlog
// in bytes.
func (m *BoltDB) PushMessages(msgs []types.Message) (int, error) {
	var backlog int

	if err := m.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(BoltDBMessagesBucket)
		b.FillPercent = 1.0
		for _, msg := range msgs {
			msgBytes, terr := io.MessageToJSON(msg)
			if terr != nil {
				return terr
			}

			seq, terr := b.NextSequence()
			if terr != nil {
				return terr
			}

			seqBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(seqBytes, uint64(seq))

			if terr = b.Put(seqBytes, msgBytes); terr != nil {
				return terr
			}
		}
		backlog = b.Stats().LeafAlloc
		return nil
	}); err != nil {
		if err == bolt.ErrDatabaseNotOpen {
			err = types.ErrTypeClosed
		}
		return 0, err
	}

	m.cond.L.Lock()
	m.cond.Broadcast()
	m.cond.L.Unlock()
	return backlog, nil
}

// CloseOnceEmpty closes the Buffer once the buffer has been emptied, this is a
// way for a writer to signal to a reader that it is finished writing messages,
// and therefore the reader can close once it is caught up. This call blocks
// until the close is completed.
func (m *BoltDB) CloseOnceEmpty() {
	m.cond.L.Lock()
	m.backlogLock.Lock()
	m.running = false
	m.cond.Broadcast()
	m.backlogLock.Unlock()
	m.cond.L.Unlock()
}

// Close closes the Buffer so that blocked readers or writers become
// unblocked.
func (m *BoltDB) Close() {
	m.cond.L.Lock()
	m.db.Close()
	m.cond.Broadcast()
	m.cond.L.Unlock()
}

//------------------------------------------------------------------------------
