// Copyright 2024 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/v4/blob/main/licenses/rcl.md

package pglogicalstream

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"
)

var errMsgNotSupported = errors.New("replication message not supported")

// MessageType indicates the type of logical replication message.
type MessageType uint8

func (t MessageType) String() string {
	switch t {
	case MessageTypeBegin:
		return "Begin"
	case MessageTypeCommit:
		return "Commit"
	case MessageTypeOrigin:
		return "Origin"
	case MessageTypeRelation:
		return "Relation"
	case MessageTypeType:
		return "Type"
	case MessageTypeInsert:
		return "Insert"
	case MessageTypeUpdate:
		return "Update"
	case MessageTypeDelete:
		return "Delete"
	case MessageTypeTruncate:
		return "Truncate"
	case MessageTypeMessage:
		return "Message"
	case MessageTypeStreamStart:
		return "StreamStart"
	case MessageTypeStreamStop:
		return "StreamStop"
	case MessageTypeStreamCommit:
		return "StreamCommit"
	case MessageTypeStreamAbort:
		return "StreamAbort"
	default:
		return "Unknown"
	}
}

// List of types of logical replication messages.
const (
	MessageTypeBegin        MessageType = 'B'
	MessageTypeMessage      MessageType = 'M'
	MessageTypeCommit       MessageType = 'C'
	MessageTypeOrigin       MessageType = 'O'
	MessageTypeRelation     MessageType = 'R'
	MessageTypeType         MessageType = 'Y'
	MessageTypeInsert       MessageType = 'I'
	MessageTypeUpdate       MessageType = 'U'
	MessageTypeDelete       MessageType = 'D'
	MessageTypeTruncate     MessageType = 'T'
	MessageTypeStreamStart  MessageType = 'S'
	MessageTypeStreamStop   MessageType = 'E'
	MessageTypeStreamCommit MessageType = 'c'
	MessageTypeStreamAbort  MessageType = 'A'
)

// Message is a message received from server.
type Message interface {
	Type() MessageType
}

// MessageDecoder decodes message into struct.
type MessageDecoder interface {
	Decode([]byte) error
}

type baseMessage struct {
	msgType MessageType
}

// Type returns message type.
func (m *baseMessage) Type() MessageType {
	return m.msgType
}

// SetType sets message type.
// This method is added to help writing test code in application.
// The message type is still defined by message data.
func (m *baseMessage) SetType(t MessageType) {
	m.msgType = t
}

// Decode parse src into message struct. The src must contain the complete message starts after
// the first message type byte.
func (m *baseMessage) Decode(_ []byte) error {
	return errors.New("message decode not implemented")
}

func (m *baseMessage) lengthError(name string, expectedLen, actualLen int) error {
	return fmt.Errorf("%s must have %d bytes, got %d bytes", name, expectedLen, actualLen)
}

func (m *baseMessage) decodeStringError(name, field string) error {
	return fmt.Errorf("%s.%s decode string error", name, field)
}

func (m *baseMessage) decodeTupleDataError(name, field string, e error) error {
	return fmt.Errorf("%s.%s decode tuple error: %s", name, field, e.Error())
}

func (m *baseMessage) invalidTupleTypeError(name, field, e string, a byte) error {
	return fmt.Errorf("%s.%s invalid tuple type value, expect %s, actual %c", name, field, e, a)
}

// decodeString decode a string from src and returns the length of bytes being parsed.
//
// String type definition: https://www.postgresql.org/docs/current/protocol-message-types.html
// String(s)
//
//	A null-terminated string (C-style string). There is no specific length limitation on strings.
//	If s is specified it is the exact value that will appear, otherwise the value is variable.
//	Eg. String, String("user").
//
// If there is no null byte in src, return -1.
func (m *baseMessage) decodeString(src []byte) (string, int) {
	end := bytes.IndexByte(src, byte(0))
	if end == -1 {
		return "", -1
	}
	// Trim the last null byte before converting it to a Golang string, then we can
	// compare the result string with a Golang string literal.
	return string(src[:end]), end + 1
}

func (m *baseMessage) decodeLSN(src []byte) (LSN, int) {
	return LSN(binary.BigEndian.Uint64(src)), 8
}

func (m *baseMessage) decodeTime(src []byte) (time.Time, int) {
	return pgTimeToTime(int64(binary.BigEndian.Uint64(src))), 8
}

func (m *baseMessage) decodeUint16(src []byte) (uint16, int) {
	return binary.BigEndian.Uint16(src), 2
}

func (m *baseMessage) decodeUint32(src []byte) (uint32, int) {
	return binary.BigEndian.Uint32(src), 4
}

func (m *baseMessage) decodeInt32(src []byte) (int32, int) {
	asUint32, size := m.decodeUint32(src)
	return int32(asUint32), size
}

// BeginMessage is a begin message.
type BeginMessage struct {
	baseMessage
	// FinalLSN is the final LSN of the transaction.
	FinalLSN LSN
	// CommitTime is the commit timestamp of the transaction.
	CommitTime time.Time
	// Xid of the transaction.
	Xid uint32
}

// Decode decodes the message from src.
func (m *BeginMessage) Decode(src []byte) error {
	if len(src) < 20 {
		return m.lengthError("BeginMessage", 20, len(src))
	}
	var low, used int
	m.FinalLSN, used = m.decodeLSN(src)
	low += used
	m.CommitTime, used = m.decodeTime(src[low:])
	low += used
	m.Xid = binary.BigEndian.Uint32(src[low:])

	m.SetType(MessageTypeBegin)

	return nil
}

// CommitMessage is a commit message.
type CommitMessage struct {
	baseMessage
	// Flags currently unused (must be 0).
	Flags uint8
	// CommitLSN is the LSN of the commit.
	CommitLSN LSN
	// TransactionEndLSN is the end LSN of the transaction.
	TransactionEndLSN LSN
	// CommitTime is the commit timestamp of the transaction
	CommitTime time.Time
}

// Decode decodes the message from src.
func (m *CommitMessage) Decode(src []byte) error {
	if len(src) < 25 {
		return m.lengthError("CommitMessage", 25, len(src))
	}
	var low, used int
	m.Flags = src[0]
	low += 1
	m.CommitLSN, used = m.decodeLSN(src[low:])
	low += used
	m.TransactionEndLSN, used = m.decodeLSN(src[low:])
	low += used
	m.CommitTime, _ = m.decodeTime(src[low:])

	m.SetType(MessageTypeCommit)

	return nil
}

// OriginMessage is an origin message.
type OriginMessage struct {
	baseMessage
	// CommitLSN is the LSN of the commit on the origin server.
	CommitLSN LSN
	Name      string
}

// Decode decodes to message from src.
func (m *OriginMessage) Decode(src []byte) error {
	if len(src) < 8 {
		return m.lengthError("OriginMessage", 9, len(src))
	}

	var low, used int
	m.CommitLSN, used = m.decodeLSN(src)
	low += used
	m.Name, used = m.decodeString(src[low:])
	if used < 0 {
		return m.decodeStringError("OriginMessage", "Name")
	}

	m.SetType(MessageTypeOrigin)

	return nil
}

// RelationMessageColumn is one column in a RelationMessage.
type RelationMessageColumn struct {
	// Flags for the column. Currently, it can be either 0 for no flags or 1 which marks the column as part of the key.
	Flags uint8

	Name string

	// DataType is the ID of the column's data type.
	DataType uint32

	// TypeModifier is type modifier of the column (atttypmod).
	TypeModifier int32
}

// RelationMessage is a relation message.
type RelationMessage struct {
	baseMessage
	RelationID      uint32
	Namespace       string
	RelationName    string
	ReplicaIdentity uint8
	ColumnNum       uint16
	Columns         []*RelationMessageColumn
}

// Decode decodes to message from src.
func (m *RelationMessage) Decode(src []byte) error {
	if len(src) < 7 {
		return m.lengthError("RelationMessage", 7, len(src))
	}

	var low, used int
	m.RelationID, used = m.decodeUint32(src)
	low += used

	m.Namespace, used = m.decodeString(src[low:])
	if used < 0 {
		return m.decodeStringError("RelationMessage", "Namespace")
	}
	low += used

	m.RelationName, used = m.decodeString(src[low:])
	if used < 0 {
		return m.decodeStringError("RelationMessage", "RelationName")
	}
	low += used

	m.ReplicaIdentity = src[low]
	low++

	m.ColumnNum, used = m.decodeUint16(src[low:])
	low += used

	for i := 0; i < int(m.ColumnNum); i++ {
		column := new(RelationMessageColumn)
		column.Flags = src[low]
		low++
		column.Name, used = m.decodeString(src[low:])
		if used < 0 {
			return m.decodeStringError("RelationMessage", fmt.Sprintf("Column[%d].Name", i))
		}
		low += used

		column.DataType, used = m.decodeUint32(src[low:])
		low += used

		column.TypeModifier, used = m.decodeInt32(src[low:])
		low += used

		m.Columns = append(m.Columns, column)
	}

	m.SetType(MessageTypeRelation)

	return nil
}

// TypeMessage is a type message.
type TypeMessage struct {
	baseMessage
	DataType  uint32
	Namespace string
	Name      string
}

// Decode decodes to message from src.
func (m *TypeMessage) Decode(src []byte) error {
	if len(src) < 6 {
		return m.lengthError("TypeMessage", 6, len(src))
	}

	var low, used int
	m.DataType, used = m.decodeUint32(src)
	low += used

	m.Namespace, used = m.decodeString(src[low:])
	if used < 0 {
		return m.decodeStringError("TypeMessage", "Namespace")
	}
	low += used

	m.Name, used = m.decodeString(src[low:])
	if used < 0 {
		return m.decodeStringError("TypeMessage", "Name")
	}

	m.SetType(MessageTypeType)

	return nil
}

// List of types of data in a tuple.
const (
	TupleDataTypeNull   = uint8('n')
	TupleDataTypeToast  = uint8('u')
	TupleDataTypeText   = uint8('t')
	TupleDataTypeBinary = uint8('b')
)

// TupleDataColumn is a column in a TupleData.
type TupleDataColumn struct {
	// DataType indicates how the data is stored.
	//	 Byte1('n') Identifies the data as NULL value.
	//	 Or
	//	 Byte1('u') Identifies unchanged TOASTed value (the actual value is not sent).
	//	 Or
	//	 Byte1('t') Identifies the data as text formatted value.
	//	 Or
	//	 Byte1('b') Identifies the data as binary value.
	DataType uint8
	Length   uint32
	// Data is th value of the column, in text format. (A future release might support additional formats.) n is the above length.
	Data []byte
}

// Int64 parse column data as an int64 integer.
func (c *TupleDataColumn) Int64() (int64, error) {
	if c.DataType != TupleDataTypeText {
		return 0, fmt.Errorf("invalid column's data type, expect %c, actual %c",
			TupleDataTypeText, c.DataType)
	}

	return strconv.ParseInt(string(c.Data), 10, 64)
}

// TupleData contains row change information.
type TupleData struct {
	baseMessage
	ColumnNum uint16
	Columns   []*TupleDataColumn
}

// Decode decodes to message from src.
func (m *TupleData) Decode(src []byte) (int, error) {
	var low, used int

	m.ColumnNum, used = m.decodeUint16(src)
	low += used

	for i := 0; i < int(m.ColumnNum); i++ {
		column := new(TupleDataColumn)
		column.DataType = src[low]
		low += 1

		switch column.DataType {
		case TupleDataTypeText, TupleDataTypeBinary:
			column.Length, used = m.decodeUint32(src[low:])
			low += used

			column.Data = make([]byte, int(column.Length))
			for j := 0; j < int(column.Length); j++ {
				column.Data[j] = src[low+j]
			}
			low += int(column.Length)
		case TupleDataTypeNull, TupleDataTypeToast:
		}

		m.Columns = append(m.Columns, column)
	}

	return low, nil
}

// InsertMessage is a insert message
type InsertMessage struct {
	baseMessage
	// RelationID is the ID of the relation corresponding to the ID in the relation message.
	RelationID uint32
	Tuple      *TupleData
}

// Decode decodes to message from src.
func (m *InsertMessage) Decode(src []byte) error {
	if len(src) < 8 {
		return m.lengthError("InsertMessage", 8, len(src))
	}

	var low, used int

	m.RelationID, used = m.decodeUint32(src)
	low += used

	tupleType := src[low]
	low += 1
	if tupleType != 'N' {
		return m.invalidTupleTypeError("InsertMessage", "TupleType", "N", tupleType)
	}

	m.Tuple = new(TupleData)
	_, err := m.Tuple.Decode(src[low:])
	if err != nil {
		return m.decodeTupleDataError("InsertMessage", "TupleData", err)
	}

	m.SetType(MessageTypeInsert)

	return nil
}

// List of types of UpdateMessage tuples.
const (
	UpdateMessageTupleTypeNone = uint8(0)
	UpdateMessageTupleTypeKey  = uint8('K')
	UpdateMessageTupleTypeOld  = uint8('O')
	UpdateMessageTupleTypeNew  = uint8('N')
)

// UpdateMessage is a update message.
type UpdateMessage struct {
	baseMessage
	RelationID uint32

	// OldTupleType
	//   Byte1('K'):
	//     Identifies the following TupleData submessage as a key.
	//     This field is optional and is only present if the update changed data
	//     in any of the column(s) that are part of the REPLICA IDENTITY index.
	//
	//   Byte1('O'):
	//     Identifies the following TupleData submessage as an old tuple.
	//     This field is optional and is only present if table in which the update happened
	//     has REPLICA IDENTITY set to FULL.
	//
	//   The Update message may contain either a 'K' message part or an 'O' message part
	//   or neither of them, but never both of them.
	OldTupleType uint8
	OldTuple     *TupleData

	// NewTuple is the contents of a new tuple.
	//   Byte1('N'): Identifies the following TupleData message as a new tuple.
	NewTuple *TupleData
}

// Decode decodes to message from src.
func (m *UpdateMessage) Decode(src []byte) (err error) {
	if len(src) < 6 {
		return m.lengthError("UpdateMessage", 6, len(src))
	}

	var low, used int

	m.RelationID, used = m.decodeUint32(src)
	low += used

	tupleType := src[low]
	low++

	switch tupleType {
	case UpdateMessageTupleTypeKey, UpdateMessageTupleTypeOld:
		m.OldTupleType = tupleType
		m.OldTuple = new(TupleData)
		used, err = m.OldTuple.Decode(src[low:])
		if err != nil {
			return m.decodeTupleDataError("UpdateMessage", "OldTuple", err)
		}
		low += used
		low++
		fallthrough
	case UpdateMessageTupleTypeNew:
		m.NewTuple = new(TupleData)
		_, err = m.NewTuple.Decode(src[low:])
		if err != nil {
			return m.decodeTupleDataError("UpdateMessage", "NewTuple", err)
		}
	default:
		return m.invalidTupleTypeError("UpdateMessage", "Tuple", "K/O/N", tupleType)
	}

	m.SetType(MessageTypeUpdate)

	return nil
}

// List of types of DeleteMessage tuples.
const (
	DeleteMessageTupleTypeKey = uint8('K')
	DeleteMessageTupleTypeOld = uint8('O')
)

// DeleteMessage is a delete message.
type DeleteMessage struct {
	baseMessage
	RelationID uint32
	// OldTupleType
	//   Byte1('K'):
	//     Identifies the following TupleData submessage as a key.
	//     This field is present if the table in which the delete has happened uses an index
	//     as REPLICA IDENTITY.
	//
	//   Byte1('O')
	//     Identifies the following TupleData message as an old tuple.
	//     This field is present if the table in which the delete has happened has
	//     REPLICA IDENTITY set to FULL.
	//
	// The Delete message may contain either a 'K' message part or an 'O' message part,
	// but never both of them.
	OldTupleType uint8
	OldTuple     *TupleData
}

// Decode decodes a message from src.
func (m *DeleteMessage) Decode(src []byte) (err error) {
	if len(src) < 4 {
		return m.lengthError("DeleteMessage", 4, len(src))
	}

	var low, used int

	m.RelationID, used = m.decodeUint32(src)
	low += used

	m.OldTupleType = src[low]
	low++

	switch m.OldTupleType {
	case DeleteMessageTupleTypeKey, DeleteMessageTupleTypeOld:
		m.OldTuple = new(TupleData)
		_, err = m.OldTuple.Decode(src[low:])
		if err != nil {
			return m.decodeTupleDataError("DeleteMessage", "OldTuple", err)
		}
	default:
		return m.invalidTupleTypeError("DeleteMessage", "OldTupleType", "K/O", m.OldTupleType)
	}

	m.SetType(MessageTypeDelete)

	return nil
}

// List of truncate options.
const (
	TruncateOptionCascade = uint8(1) << iota
	TruncateOptionRestartIdentity
)

// TruncateMessage is a truncate message.
type TruncateMessage struct {
	baseMessage
	RelationNum uint32
	Option      uint8
	RelationIDs []uint32
}

// Decode decodes to message from src.
func (m *TruncateMessage) Decode(src []byte) (err error) {
	if len(src) < 9 {
		return m.lengthError("TruncateMessage", 9, len(src))
	}

	var low, used int
	m.RelationNum, used = m.decodeUint32(src)
	low += used

	m.Option = src[low]
	low++

	m.RelationIDs = make([]uint32, m.RelationNum)
	for i := 0; i < int(m.RelationNum); i++ {
		m.RelationIDs[i], used = m.decodeUint32(src[low:])
		low += used
	}

	m.SetType(MessageTypeTruncate)

	return nil
}

// LogicalDecodingMessage is a logical decoding message.
type LogicalDecodingMessage struct {
	baseMessage

	LSN           LSN
	Transactional bool
	Prefix        string
	Content       []byte
}

// Decode decodes a message from src.
func (m *LogicalDecodingMessage) Decode(src []byte) (err error) {
	if len(src) < 14 {
		return m.lengthError("LogicalDecodingMessage", 14, len(src))
	}

	var low, used int

	flags := src[low]
	m.Transactional = flags == 1
	low++

	m.LSN, used = m.decodeLSN(src[low:])
	low += used

	m.Prefix, used = m.decodeString(src[low:])
	low += used

	contentLength, used := m.decodeUint32(src[low:])
	low += used

	m.Content = src[low : low+int(contentLength)]

	m.SetType(MessageTypeMessage)

	return nil
}

// Parse parse a logical replication message.
func Parse(data []byte) (m Message, err error) {
	var decoder MessageDecoder
	msgType := MessageType(data[0])
	switch msgType {
	case MessageTypeRelation:
		decoder = new(RelationMessage)
	case MessageTypeType:
		decoder = new(TypeMessage)
	case MessageTypeInsert:
		decoder = new(InsertMessage)
	case MessageTypeUpdate:
		decoder = new(UpdateMessage)
	case MessageTypeDelete:
		decoder = new(DeleteMessage)
	case MessageTypeTruncate:
		decoder = new(TruncateMessage)
	case MessageTypeMessage:
		decoder = new(LogicalDecodingMessage)
	default:
		decoder = getCommonDecoder(msgType)
	}

	if decoder == nil {
		return nil, errMsgNotSupported
	}

	if err = decoder.Decode(data[1:]); err != nil {
		return nil, err
	}

	return decoder.(Message), nil
}

func getCommonDecoder(msgType MessageType) MessageDecoder {
	var decoder MessageDecoder
	switch msgType {
	case MessageTypeBegin:
		decoder = new(BeginMessage)
	case MessageTypeCommit:
		decoder = new(CommitMessage)
	case MessageTypeOrigin:
		decoder = new(OriginMessage)
	}

	return decoder
}
