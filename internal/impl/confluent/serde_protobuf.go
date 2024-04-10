package confluent

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/benthosdev/benthos/v4/internal/impl/protobuf"
	"github.com/benthosdev/benthos/v4/public/service"
)

func (s *schemaRegistryDecoder) getProtobufDecoder(ctx context.Context, info SchemaInfo) (schemaDecoder, error) {
	regMap := map[string]string{
		".": info.Schema,
	}
	if err := s.client.WalkReferences(ctx, info.References, func(ctx context.Context, name string, si SchemaInfo) error {
		regMap[name] = si.Schema
		return nil
	}); err != nil {
		return nil, err
	}

	files, types, err := protobuf.RegistriesFromMap(regMap)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto schema: %v", err)
	}

	targetFile, err := files.FindFileByPath(".")
	if err != nil {
		return nil, err
	}

	msgTypes := targetFile.Messages()
	return func(m *service.Message) error {
		b, err := m.AsBytes()
		if err != nil {
			return err
		}

		bytesRead, msgIndexes, err := readMessageIndexes(b)
		if err != nil {
			return err
		}

		var msgDesc protoreflect.MessageDescriptor
		for i, j := range msgIndexes {
			var targetDescriptors protoreflect.MessageDescriptors
			if i == 0 {
				targetDescriptors = msgTypes
			} else {
				targetDescriptors = msgDesc.Messages()
			}
			if l := targetDescriptors.Len(); l <= j {
				return fmt.Errorf("message index (%v) is greater than available message definitions (%v)", j, l)
			}
			msgDesc = targetDescriptors.Get(j)
		}

		dynMsg := dynamicpb.NewMessage(msgDesc)
		remaining := b[bytesRead:]

		if err := proto.Unmarshal(remaining, dynMsg); err != nil {
			return fmt.Errorf("failed to unmarshal protobuf message: %w", err)
		}

		data, err := protojson.MarshalOptions{Resolver: types}.Marshal(dynMsg)
		if err != nil {
			return fmt.Errorf("failed to marshal JSON protobuf message: %w", err)
		}

		m.SetBytes(data)
		return nil
	}, nil
}

func (s *schemaRegistryEncoder) getProtobufEncoder(ctx context.Context, info SchemaInfo) (schemaEncoder, error) {
	regMap := map[string]string{
		".": info.Schema,
	}
	if err := s.client.WalkReferences(ctx, info.References, func(ctx context.Context, name string, si SchemaInfo) error {
		regMap[name] = si.Schema
		return nil
	}); err != nil {
		return nil, err
	}

	files, types, err := protobuf.RegistriesFromMap(regMap)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto schema: %v", err)
	}

	targetFile, err := files.FindFileByPath(".")
	if err != nil {
		return nil, err
	}
	msgTypesCache := newCachedMessageTypes(targetFile.Messages(), types)

	return func(m *service.Message) error {
		b, err := m.AsBytes()
		if err != nil {
			return err
		}

		dynMsg, indexBytes, err := msgTypesCache.TryParseMsg(b)
		if err != nil {
			return err
		}

		data, err := proto.Marshal(dynMsg)
		if err != nil {
			return fmt.Errorf("failed to marshal protobuf message: %w", err)
		}

		m.SetBytes(append(indexBytes, data...)) // TODO: Only allocate once by passing id through
		return nil
	}, nil
}

//------------------------------------------------------------------------------

// This is some whacky and wild code. The problem we have is that a single given
// schema identifier is capable of providing any number of message types within
// the protobuf schema, any of which could be the candidate for the appropriate
// type of the data we're encoding.
//
// When decoding against this schema we're provided with a set of indexes which
// points to the specific message type to parse. However, when encoding we have
// nothing to go by and are instead expected to work this out and provide the
// indexes once we're done.
//
// Most systems likely skip this problem by already having the data in a
// protobuf type, in which case you can use reflect to gather this data.
// However, Benthos is agnostic here and we're dealing with dynamic data in raw
// bytes form (usually JSON). We therefore have three options:
//
//  1. Consider any schema that contains more than one message definition
//     invalid, and we simply won't support it
//  2. Request that users provide the explicit full name (or indexes) of the
//     message they intend to encode against in their config.
//  3. Exhaustively attempt to encode against each message type until we run out
//     of candidates or find a success, with caching as an optimisation for when
//     all messages of a subject are consistent.
//
// I've decided that option 1 is inadequate and would be a frustrating
// limitation. Between 2 and 3 I've chosen to proceed with 3 for now since we
// can add 2 as an optional enhancement later on, and to rely on it solely would
// be very annoying as in cases where the subject is dynamic the user would need
// to do the tedious task of making sure the two always line up, which negates a
// lot of the goodies that come with using a schema registry service in the
// first place.
type cachedMessageTypes struct {
	singleMsgType protoreflect.MessageDescriptor
	msgTypeMap    map[string]protoreflect.MessageDescriptor
	allTypes      *protoregistry.Types

	lastSuccessful string
	cacheMut       sync.Mutex
}

func messageDescriptorsToMap(msgs protoreflect.MessageDescriptors, m map[string]protoreflect.MessageDescriptor) {
	for i := 0; i < msgs.Len(); i++ {
		msg := msgs.Get(i)
		indexBytes := toMessageIndexBytes(msg)
		m[string(indexBytes)] = msg
		// TODO: Currently we ignore nested message types and only test those
		// at the top level of the file.
		// messageDescriptorsToMap(msg.Messages(), m)
	}
}

func newCachedMessageTypes(rootMsgs protoreflect.MessageDescriptors, allTypes *protoregistry.Types) *cachedMessageTypes {
	c := &cachedMessageTypes{
		allTypes: allTypes,
	}
	if rootMsgs.Len() == 1 {
		c.singleMsgType = rootMsgs.Get(0)
	} else {
		c.msgTypeMap = map[string]protoreflect.MessageDescriptor{}
		messageDescriptorsToMap(rootMsgs, c.msgTypeMap)
	}
	return c
}

func (c *cachedMessageTypes) TryParseMsg(data []byte) (*dynamicpb.Message, []byte, error) {
	if c.singleMsgType != nil {
		d, err := c.tryDesc(data, c.singleMsgType)
		if err != nil {
			return nil, nil, err
		}
		return d, []byte{0}, nil
	}

	c.cacheMut.Lock()
	lastSuccessful := c.lastSuccessful
	c.cacheMut.Unlock()

	if lastSuccessful != "" {
		if msgDesc, ok := c.msgTypeMap[lastSuccessful]; ok {
			if dynMsg, err := c.tryDesc(data, msgDesc); err == nil {
				// Happy path: We had a cached message index that worked with a
				// previous encode attempt and it worked again, so no need to
				// perform any random checks.
				return dynMsg, []byte(lastSuccessful), nil
			}
		}
	}

	var errs error
	for k, msgDesc := range c.msgTypeMap {
		dynMsg, err := c.tryDesc(data, msgDesc)
		if err == nil {
			c.cacheMut.Lock()
			c.lastSuccessful = k
			c.cacheMut.Unlock()
			return dynMsg, []byte(k), nil
		}
		if errs != nil {
			errs = fmt.Errorf("%v, %v", errs, err)
		} else {
			errs = err
		}
	}
	return nil, nil, errs
}

func (c *cachedMessageTypes) tryDesc(data []byte, desc protoreflect.MessageDescriptor) (*dynamicpb.Message, error) {
	dynMsg := dynamicpb.NewMessage(desc)
	opts := protojson.UnmarshalOptions{
		Resolver: c.allTypes,
	}
	if err := opts.Unmarshal(data, dynMsg); err != nil {
		return nil, fmt.Errorf("unmarshal '%v': %w", desc.Name(), err)
	}
	return dynMsg, nil
}

//------------------------------------------------------------------------------

// The following is largely adapted from:
// https://github.com/confluentinc/confluent-kafka-go/blob/master/schemaregistry/serde/protobuf
//
// NOTE: The purpose of these indexes is to direct the parser to the exact
// message definition by index rather than absolute name (likely for space
// efficiency), and so the list of indexes points to a message index within the
// file descriptor, followed by an optional index of a message within that
// message definition, and so on.
func readMessageIndexes(payload []byte) (int, []int, error) {
	arrayLen, bytesRead := binary.Varint(payload)
	if bytesRead <= 0 {
		return bytesRead, nil, errors.New("unable to read message indexes")
	}
	if arrayLen == 0 {
		// Handle the optimization for the first message in the schema
		return bytesRead, []int{0}, nil
	}
	msgIndexes := make([]int, arrayLen)
	for i := 0; i < int(arrayLen); i++ {
		idx, read := binary.Varint(payload[bytesRead:])
		if read <= 0 {
			return bytesRead, nil, errors.New("unable to read message indexes")
		}
		bytesRead += read
		msgIndexes[i] = int(idx)
	}
	return bytesRead, msgIndexes, nil
}

func toMessageIndexBytes(descriptor protoreflect.Descriptor) []byte {
	if descriptor.Index() == 0 {
		switch descriptor.Parent().(type) {
		case protoreflect.FileDescriptor:
			// This is an optimization for the first message in the schema
			return []byte{0}
		}
	}
	msgIndexes := toMessageIndexes(descriptor, 0)
	buf := make([]byte, (1+len(msgIndexes))*binary.MaxVarintLen64)
	length := binary.PutVarint(buf, int64(len(msgIndexes)))

	for _, element := range msgIndexes {
		length += binary.PutVarint(buf[length:], int64(element))
	}
	return buf[0:length]
}

// Taken from: https://github.com/confluentinc/confluent-kafka-go/blob/master/schemaregistry/serde/protobuf
// Which itself was adapted from ideasculptor, see https://github.com/riferrei/srclient/issues/17
func toMessageIndexes(descriptor protoreflect.Descriptor, count int) []int {
	index := descriptor.Index()
	switch v := descriptor.Parent().(type) {
	case protoreflect.FileDescriptor:
		// parent is FileDescriptor, we reached the top of the stack, so we are
		// done. Allocate an array large enough to hold count+1 entries and
		// populate first value with index
		msgIndexes := make([]int, count+1)
		msgIndexes[0] = index
		return msgIndexes[0:1]
	default:
		// parent is another MessageDescriptor.  We were nested so get that
		// descriptor's indexes and append the index of this one
		msgIndexes := toMessageIndexes(v, count+1)
		return append(msgIndexes, index)
	}
}
