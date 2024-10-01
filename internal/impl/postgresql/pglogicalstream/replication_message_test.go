// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"encoding/binary"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var bigEndian = binary.BigEndian

type messageSuite struct {
	suite.Suite
}

func (s *messageSuite) R() *require.Assertions {
	return s.Require()
}

func (s *messageSuite) Equal(e, a interface{}, args ...interface{}) {
	s.R().Equal(e, a, args...)
}

func (s *messageSuite) NoError(err error) {
	s.R().NoError(err)
}

func (s *messageSuite) True(value bool) {
	s.R().True(value)
}

func (s *messageSuite) newLSN() LSN {
	return LSN(rand.Int63())
}

func (s *messageSuite) newXid() uint32 {
	return uint32(rand.Int31())
}

func (s *messageSuite) newTime() (time.Time, uint64) {
	// Postgres time format only support millisecond accuracy.
	now := time.Now().Truncate(time.Millisecond)
	return now, uint64(timeToPgTime(now))
}

func (s *messageSuite) newRelationID() uint32 {
	return uint32(rand.Int31())
}

func (s *messageSuite) putString(dst []byte, value string) int {
	copy(dst, value)
	dst[len(value)] = byte(0)
	return len(value) + 1
}

func (s *messageSuite) tupleColumnLength(dataType uint8, data []byte) int {
	switch dataType {
	case uint8('n'), uint8('u'):
		return 1
	case uint8('t'):
		return 1 + 4 + len(data)
	default:
		s.FailNow("invalid data type of a tuple: %c", dataType)
		return 0
	}
}

func (s *messageSuite) putTupleColumn(dst []byte, dataType uint8, data []byte) int {
	dst[0] = dataType

	switch dataType {
	case uint8('n'), uint8('u'):
		return 1
	case uint8('t'):
		bigEndian.PutUint32(dst[1:], uint32(len(data)))
		copy(dst[5:], data)
		return 5 + len(data)
	default:
		s.FailNow("invalid data type of a tuple: %c", dataType)
		return 0
	}
}

func (s *messageSuite) putMessageTestData(msg []byte) *LogicalDecodingMessage {
	// transaction flag
	msg[0] = 1
	off := 1

	lsn := s.newLSN()
	bigEndian.PutUint64(msg[off:], uint64(lsn))
	off += 8

	off += s.putString(msg[off:], "test")

	content := "hello"

	bigEndian.PutUint32(msg[off:], uint32(len(content)))
	off += 4

	for i := 0; i < len(content); i++ {
		msg[off] = content[i]
		off++
	}
	return &LogicalDecodingMessage{
		Transactional: true,
		LSN:           lsn,
		Prefix:        "test",
		Content:       []byte("hello"),
	}
}

func (s *messageSuite) assertV1NotSupported(msg []byte) {
	_, err := Parse(msg)
	s.Error(err)
	s.True(errors.Is(err, errMsgNotSupported))
}

func (s *messageSuite) createRelationTestData() ([]byte, *RelationMessage) {
	relationID := uint32(rand.Int31())
	namespace := "public"
	relationName := "table1"
	noAtttypmod := int32(-1)
	col1 := "id"         // int8
	col2 := "name"       // text
	col3 := "created_at" // timestamptz

	col1Length := 1 + len(col1) + 1 + 4 + 4
	col2Length := 1 + len(col2) + 1 + 4 + 4
	col3Length := 1 + len(col3) + 1 + 4 + 4

	msg := make([]byte, 1+4+len(namespace)+1+len(relationName)+1+1+
		2+col1Length+col2Length+col3Length)
	msg[0] = 'R'
	off := 1
	bigEndian.PutUint32(msg[off:], relationID)
	off += 4
	off += s.putString(msg[off:], namespace)
	off += s.putString(msg[off:], relationName)
	msg[off] = 1
	off++
	bigEndian.PutUint16(msg[off:], 3)
	off += 2

	msg[off] = 1 // column id is key
	off++
	off += s.putString(msg[off:], col1)
	bigEndian.PutUint32(msg[off:], 20) // int8
	off += 4
	bigEndian.PutUint32(msg[off:], uint32(noAtttypmod))
	off += 4

	msg[off] = 0
	off++
	off += s.putString(msg[off:], col2)
	bigEndian.PutUint32(msg[off:], 25) // text
	off += 4
	bigEndian.PutUint32(msg[off:], uint32(noAtttypmod))
	off += 4

	msg[off] = 0
	off++
	off += s.putString(msg[off:], col3)
	bigEndian.PutUint32(msg[off:], 1184) // timestamptz
	off += 4
	bigEndian.PutUint32(msg[off:], uint32(noAtttypmod))
	off += 4

	expected := &RelationMessage{
		RelationID:      relationID,
		Namespace:       namespace,
		RelationName:    relationName,
		ReplicaIdentity: 1,
		ColumnNum:       3,
		Columns: []*RelationMessageColumn{
			{
				Flags:        1,
				Name:         col1,
				DataType:     20,
				TypeModifier: -1,
			},
			{
				Flags:        0,
				Name:         col2,
				DataType:     25,
				TypeModifier: -1,
			},
			{
				Flags:        0,
				Name:         col3,
				DataType:     1184,
				TypeModifier: -1,
			},
		},
	}
	expected.msgType = 'R'

	return msg, expected
}

func (s *messageSuite) createTypeTestData() ([]byte, *TypeMessage) {
	dataType := uint32(1184) // timestamptz
	namespace := "public"
	name := "created_at"

	msg := make([]byte, 1+4+len(namespace)+1+len(name)+1)
	msg[0] = 'Y'
	off := 1
	bigEndian.PutUint32(msg[off:], dataType)
	off += 4
	off += s.putString(msg[off:], namespace)
	s.putString(msg[off:], name)

	expected := &TypeMessage{
		DataType:  dataType,
		Namespace: namespace,
		Name:      name,
	}
	expected.msgType = 'Y'

	return msg, expected
}

func (s *messageSuite) createInsertTestData() ([]byte, *InsertMessage) {
	relationID := s.newRelationID()

	col1Data := []byte("1")
	col2Data := []byte("myname")
	col3Data := []byte("123456789")
	col1Length := s.tupleColumnLength('t', col1Data)
	col2Length := s.tupleColumnLength('t', col2Data)
	col3Length := s.tupleColumnLength('t', col3Data)
	col4Length := s.tupleColumnLength('n', nil)
	col5Length := s.tupleColumnLength('u', nil)

	msg := make([]byte, 1+4+1+2+col1Length+col2Length+col3Length+col4Length+col5Length)
	msg[0] = 'I'
	off := 1
	bigEndian.PutUint32(msg[off:], relationID)
	off += 4
	msg[off] = 'N'
	off++
	bigEndian.PutUint16(msg[off:], 5)
	off += 2
	off += s.putTupleColumn(msg[off:], 't', col1Data)
	off += s.putTupleColumn(msg[off:], 't', col2Data)
	off += s.putTupleColumn(msg[off:], 't', col3Data)
	off += s.putTupleColumn(msg[off:], 'n', nil)
	s.putTupleColumn(msg[off:], 'u', nil)

	expected := &InsertMessage{
		RelationID: relationID,
		Tuple: &TupleData{
			ColumnNum: 5,
			Columns: []*TupleDataColumn{
				{
					DataType: TupleDataTypeText,
					Length:   uint32(len(col1Data)),
					Data:     col1Data,
				},
				{
					DataType: TupleDataTypeText,
					Length:   uint32(len(col2Data)),
					Data:     col2Data,
				},
				{
					DataType: TupleDataTypeText,
					Length:   uint32(len(col3Data)),
					Data:     col3Data,
				},
				{
					DataType: TupleDataTypeNull,
				},
				{
					DataType: TupleDataTypeToast,
				},
			},
		},
	}
	expected.msgType = 'I'

	return msg, expected
}

func (s *messageSuite) createUpdateTestDataTypeK() ([]byte, *UpdateMessage) {
	relationID := s.newRelationID()

	oldCol1Data := []byte("123") // like an id
	oldCol1Length := s.tupleColumnLength('t', oldCol1Data)

	newCol1Data := []byte("1124")
	newCol2Data := []byte("myname")
	newCol1Length := s.tupleColumnLength('t', newCol1Data)
	newCol2Length := s.tupleColumnLength('t', newCol2Data)

	msg := make([]byte, 1+4+
		1+2+oldCol1Length+
		1+2+newCol1Length+newCol2Length)
	msg[0] = 'U'
	off := 1
	bigEndian.PutUint32(msg[off:], relationID)
	off += 4
	msg[off] = 'K'
	off += 1
	bigEndian.PutUint16(msg[off:], 1)
	off += 2
	off += s.putTupleColumn(msg[off:], 't', oldCol1Data)
	msg[off] = 'N'
	off++
	bigEndian.PutUint16(msg[off:], 2)
	off += 2
	off += s.putTupleColumn(msg[off:], 't', newCol1Data)
	s.putTupleColumn(msg[off:], 't', newCol2Data)
	expected := &UpdateMessage{
		RelationID:   relationID,
		OldTupleType: UpdateMessageTupleTypeKey,
		OldTuple: &TupleData{
			ColumnNum: 1,
			Columns: []*TupleDataColumn{
				{
					DataType: TupleDataTypeText,
					Length:   uint32(len(oldCol1Data)),
					Data:     oldCol1Data,
				},
			},
		},
		NewTuple: &TupleData{
			ColumnNum: 2,
			Columns: []*TupleDataColumn{
				{
					DataType: TupleDataTypeText,
					Length:   uint32(len(newCol1Data)),
					Data:     newCol1Data,
				},
				{
					DataType: TupleDataTypeText,
					Length:   uint32(len(newCol2Data)),
					Data:     newCol2Data,
				},
			},
		},
	}
	expected.msgType = 'U'

	return msg, expected
}

func (s *messageSuite) createUpdateTestDataTypeO() ([]byte, *UpdateMessage) {
	relationID := s.newRelationID()

	oldCol1Data := []byte("123") // like an id
	oldCol1Length := s.tupleColumnLength('t', oldCol1Data)
	oldCol2Data := []byte("myoldname")
	oldCol2Length := s.tupleColumnLength('t', oldCol2Data)

	newCol1Data := []byte("1124")
	newCol2Data := []byte("myname")
	newCol1Length := s.tupleColumnLength('t', newCol1Data)
	newCol2Length := s.tupleColumnLength('t', newCol2Data)

	msg := make([]byte, 1+4+
		1+2+oldCol1Length+oldCol2Length+
		1+2+newCol1Length+newCol2Length)
	msg[0] = 'U'
	off := 1
	bigEndian.PutUint32(msg[off:], relationID)
	off += 4
	msg[off] = 'O'
	off += 1
	bigEndian.PutUint16(msg[off:], 2)
	off += 2
	off += s.putTupleColumn(msg[off:], 't', oldCol1Data)
	off += s.putTupleColumn(msg[off:], 't', oldCol2Data)
	msg[off] = 'N'
	off++
	bigEndian.PutUint16(msg[off:], 2)
	off += 2
	off += s.putTupleColumn(msg[off:], 't', newCol1Data)
	s.putTupleColumn(msg[off:], 't', newCol2Data)
	expected := &UpdateMessage{
		RelationID:   relationID,
		OldTupleType: UpdateMessageTupleTypeOld,
		OldTuple: &TupleData{
			ColumnNum: 2,
			Columns: []*TupleDataColumn{
				{
					DataType: TupleDataTypeText,
					Length:   uint32(len(oldCol1Data)),
					Data:     oldCol1Data,
				},
				{
					DataType: TupleDataTypeText,
					Length:   uint32(len(oldCol2Data)),
					Data:     oldCol2Data,
				},
			},
		},
		NewTuple: &TupleData{
			ColumnNum: 2,
			Columns: []*TupleDataColumn{
				{
					DataType: TupleDataTypeText,
					Length:   uint32(len(newCol1Data)),
					Data:     newCol1Data,
				},
				{
					DataType: TupleDataTypeText,
					Length:   uint32(len(newCol2Data)),
					Data:     newCol2Data,
				},
			},
		},
	}
	expected.msgType = 'U'

	return msg, expected
}

func (s *messageSuite) createUpdateTestDataWithoutOldTuple() ([]byte, *UpdateMessage) {
	relationID := s.newRelationID()

	newCol1Data := []byte("1124")
	newCol2Data := []byte("myname")
	newCol1Length := s.tupleColumnLength('t', newCol1Data)
	newCol2Length := s.tupleColumnLength('t', newCol2Data)

	msg := make([]byte, 1+4+
		1+2+newCol1Length+newCol2Length)
	msg[0] = 'U'
	off := 1
	bigEndian.PutUint32(msg[off:], relationID)
	off += 4
	msg[off] = 'N'
	off++
	bigEndian.PutUint16(msg[off:], 2)
	off += 2
	off += s.putTupleColumn(msg[off:], 't', newCol1Data)
	s.putTupleColumn(msg[off:], 't', newCol2Data)
	expected := &UpdateMessage{
		RelationID:   relationID,
		OldTupleType: UpdateMessageTupleTypeNone,
		NewTuple: &TupleData{
			ColumnNum: 2,
			Columns: []*TupleDataColumn{
				{
					DataType: TupleDataTypeText,
					Length:   uint32(len(newCol1Data)),
					Data:     newCol1Data,
				},
				{
					DataType: TupleDataTypeText,
					Length:   uint32(len(newCol2Data)),
					Data:     newCol2Data,
				},
			},
		},
	}
	expected.msgType = 'U'

	return msg, expected
}

func (s *messageSuite) createDeleteTestDataTypeK() ([]byte, *DeleteMessage) {
	relationID := s.newRelationID()

	oldCol1Data := []byte("123") // like an id
	oldCol1Length := s.tupleColumnLength('t', oldCol1Data)

	msg := make([]byte, 1+4+
		1+2+oldCol1Length)
	msg[0] = 'D'
	off := 1
	bigEndian.PutUint32(msg[off:], relationID)
	off += 4
	msg[off] = 'K'
	off++
	bigEndian.PutUint16(msg[off:], 1)
	off += 2
	off += s.putTupleColumn(msg[off:], 't', oldCol1Data)
	expected := &DeleteMessage{
		RelationID:   relationID,
		OldTupleType: DeleteMessageTupleTypeKey,
		OldTuple: &TupleData{
			ColumnNum: 1,
			Columns: []*TupleDataColumn{
				{
					DataType: TupleDataTypeText,
					Length:   uint32(len(oldCol1Data)),
					Data:     oldCol1Data,
				},
			},
		},
	}
	expected.msgType = 'D'
	return msg, expected
}

func (s *messageSuite) createDeleteTestDataTypeO() ([]byte, *DeleteMessage) {
	relationID := s.newRelationID()

	oldCol1Data := []byte("123") // like an id
	oldCol1Length := s.tupleColumnLength('t', oldCol1Data)
	oldCol2Data := []byte("myoldname")
	oldCol2Length := s.tupleColumnLength('t', oldCol2Data)

	msg := make([]byte, 1+4+
		1+2+oldCol1Length+oldCol2Length)
	msg[0] = 'D'
	off := 1
	bigEndian.PutUint32(msg[off:], relationID)
	off += 4
	msg[off] = 'O'
	off += 1
	bigEndian.PutUint16(msg[off:], 2)
	off += 2
	off += s.putTupleColumn(msg[off:], 't', oldCol1Data)
	off += s.putTupleColumn(msg[off:], 't', oldCol2Data)
	expected := &DeleteMessage{
		RelationID:   relationID,
		OldTupleType: DeleteMessageTupleTypeOld,
		OldTuple: &TupleData{
			ColumnNum: 2,
			Columns: []*TupleDataColumn{
				{
					DataType: TupleDataTypeText,
					Length:   uint32(len(oldCol1Data)),
					Data:     oldCol1Data,
				},
				{
					DataType: TupleDataTypeText,
					Length:   uint32(len(oldCol2Data)),
					Data:     oldCol2Data,
				},
			},
		},
	}
	expected.msgType = 'D'
	return msg, expected
}

func (s *messageSuite) createTruncateTestData() ([]byte, *TruncateMessage) {
	relationID1 := s.newRelationID()
	relationID2 := s.newRelationID()
	option := uint8(0x01 | 0x02)

	msg := make([]byte, 1+4+1+4*2)
	msg[0] = 'T'
	off := 1
	bigEndian.PutUint32(msg[off:], 2)
	off += 4
	msg[off] = option
	off++
	bigEndian.PutUint32(msg[off:], relationID1)
	off += 4
	bigEndian.PutUint32(msg[off:], relationID2)
	expected := &TruncateMessage{
		RelationNum: 2,
		Option:      TruncateOptionCascade | TruncateOptionRestartIdentity,
		RelationIDs: []uint32{
			relationID1,
			relationID2,
		},
	}
	expected.msgType = 'T'
	return msg, expected
}

func (s *messageSuite) insertXid(msg []byte) ([]byte, uint32) {
	msgV2 := make([]byte, 4+len(msg))
	msgV2[0] = msg[0]
	xid := s.newXid()
	bigEndian.PutUint32(msgV2[1:], xid)
	copy(msgV2[5:], msg[1:])

	return msgV2, xid
}

func TestBeginMessageSuite(t *testing.T) {
	suite.Run(t, new(beginMessageSuite))
}

type beginMessageSuite struct {
	messageSuite
}

func (s *beginMessageSuite) Test() {
	finalLSN := s.newLSN()
	commitTime, pgCommitTime := s.newTime()
	xid := s.newXid()

	msg := make([]byte, 1+8+8+4)
	msg[0] = 'B'
	bigEndian.PutUint64(msg[1:], uint64(finalLSN))
	bigEndian.PutUint64(msg[9:], pgCommitTime)
	bigEndian.PutUint32(msg[17:], xid)

	m, err := Parse(msg)
	s.NoError(err)
	beginMsg, ok := m.(*BeginMessage)
	s.True(ok)

	expected := &BeginMessage{
		FinalLSN:   finalLSN,
		CommitTime: commitTime,
		Xid:        xid,
	}
	expected.msgType = 'B'
	s.Equal(expected, beginMsg)
}

func TestCommitMessage(t *testing.T) {
	suite.Run(t, new(commitMessageSuite))
}

type commitMessageSuite struct {
	messageSuite
}

func (s *commitMessageSuite) Test() {
	flags := uint8(0)
	commitLSN := s.newLSN()
	transactionEndLSN := s.newLSN()
	commitTime, pgCommitTime := s.newTime()

	msg := make([]byte, 1+1+8+8+8)
	msg[0] = 'C'
	msg[1] = flags
	bigEndian.PutUint64(msg[2:], uint64(commitLSN))
	bigEndian.PutUint64(msg[10:], uint64(transactionEndLSN))
	bigEndian.PutUint64(msg[18:], pgCommitTime)

	m, err := Parse(msg)
	s.NoError(err)
	commitMsg, ok := m.(*CommitMessage)
	s.True(ok)

	expected := &CommitMessage{
		Flags:             0,
		CommitLSN:         commitLSN,
		TransactionEndLSN: transactionEndLSN,
		CommitTime:        commitTime,
	}
	expected.msgType = 'C'
	s.Equal(expected, commitMsg)
}

func TestOriginMessage(t *testing.T) {
	suite.Run(t, new(originMessageSuite))
}

type originMessageSuite struct {
	messageSuite
}

func (s *originMessageSuite) Test() {
	commitLSN := s.newLSN()
	name := "someorigin"

	msg := make([]byte, 1+8+len(name)+1) // 1 byte for \0
	msg[0] = 'O'
	bigEndian.PutUint64(msg[1:], uint64(commitLSN))
	s.putString(msg[9:], name)

	m, err := Parse(msg)
	s.NoError(err)
	originMsg, ok := m.(*OriginMessage)
	s.True(ok)

	expected := &OriginMessage{
		CommitLSN: commitLSN,
		Name:      name,
	}
	expected.msgType = 'O'
	s.Equal(expected, originMsg)
}

func TestRelationMessageSuite(t *testing.T) {
	suite.Run(t, new(relationMessageSuite))
}

type relationMessageSuite struct {
	messageSuite
}

func (s *relationMessageSuite) Test() {

	msg, expected := s.createRelationTestData()

	m, err := Parse(msg)
	s.NoError(err)
	relationMsg, ok := m.(*RelationMessage)
	s.True(ok)

	s.Equal(expected, relationMsg)
}

func TestTypeMessageSuite(t *testing.T) {
	suite.Run(t, new(typeMessageSuite))
}

type typeMessageSuite struct {
	messageSuite
}

func (s *typeMessageSuite) Test() {
	msg, expected := s.createTypeTestData()

	m, err := Parse(msg)
	s.NoError(err)
	typeMsg, ok := m.(*TypeMessage)
	s.True(ok)

	s.Equal(expected, typeMsg)
}

func TestInsertMessageSuite(t *testing.T) {
	suite.Run(t, new(insertMessageSuite))
}

type insertMessageSuite struct {
	messageSuite
}

func (s *insertMessageSuite) Test() {

	msg, expected := s.createInsertTestData()

	m, err := Parse(msg)
	s.NoError(err)
	insertMsg, ok := m.(*InsertMessage)
	s.True(ok)

	s.Equal(expected, insertMsg)
}

func TestUpdateMessageSuite(t *testing.T) {
	suite.Run(t, new(updateMessageSuite))
}

type updateMessageSuite struct {
	messageSuite
}

func (s *updateMessageSuite) TestWithOldTupleTypeK() {
	msg, expected := s.createUpdateTestDataTypeK()
	m, err := Parse(msg)
	s.NoError(err)
	updateMsg, ok := m.(*UpdateMessage)
	s.True(ok)

	s.Equal(expected, updateMsg)
}

func (s *updateMessageSuite) TestWithOldTupleTypeO() {
	msg, expected := s.createUpdateTestDataTypeO()
	m, err := Parse(msg)
	s.NoError(err)
	updateMsg, ok := m.(*UpdateMessage)
	s.True(ok)

	s.Equal(expected, updateMsg)
}

func (s *updateMessageSuite) TestWithoutOldTuple() {
	msg, expected := s.createUpdateTestDataWithoutOldTuple()
	m, err := Parse(msg)
	s.NoError(err)
	updateMsg, ok := m.(*UpdateMessage)
	s.True(ok)

	s.Equal(expected, updateMsg)
}

func TestDeleteMessageSuite(t *testing.T) {
	suite.Run(t, new(deleteMessageSuite))
}

type deleteMessageSuite struct {
	messageSuite
}

func (s *deleteMessageSuite) TestWithOldTupleTypeK() {
	msg, expected := s.createDeleteTestDataTypeK()

	m, err := Parse(msg)
	s.NoError(err)
	deleteMsg, ok := m.(*DeleteMessage)
	s.True(ok)

	s.Equal(expected, deleteMsg)
}

func (s *deleteMessageSuite) TestWithOldTupleTypeO() {
	msg, expected := s.createDeleteTestDataTypeO()

	m, err := Parse(msg)
	s.NoError(err)
	deleteMsg, ok := m.(*DeleteMessage)
	s.True(ok)

	s.Equal(expected, deleteMsg)
}

func TestTruncateMessageSuite(t *testing.T) {
	suite.Run(t, new(truncateMessageSuite))
}

type truncateMessageSuite struct {
	messageSuite
}

func (s *truncateMessageSuite) Test() {
	msg, expected := s.createTruncateTestData()

	m, err := Parse(msg)
	s.NoError(err)
	truncateMsg, ok := m.(*TruncateMessage)
	s.True(ok)

	s.Equal(expected, truncateMsg)
}

func TestLogicalDecodingMessageSuite(t *testing.T) {
	suite.Run(t, new(logicalDecodingMessageSuite))
}

type logicalDecodingMessageSuite struct {
	messageSuite
}

func (s *logicalDecodingMessageSuite) Test() {
	msg := make([]byte, 1+1+8+5+4+5)
	msg[0] = 'M'

	expected := s.putMessageTestData(msg[1:])

	expected.msgType = MessageTypeMessage

	m, err := Parse(msg)
	s.NoError(err)
	logicalDecodingMsg, ok := m.(*LogicalDecodingMessage)
	s.True(ok)

	s.Equal(expected, logicalDecodingMsg)
}
