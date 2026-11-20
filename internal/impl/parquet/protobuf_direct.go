// Copyright 2026 Redpanda Data, Inc.
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

package parquet

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/parquet-go/parquet-go"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// This file holds a second way of producing the exact file parquet_encode
// produces for protobuf data: instead of decoding each message into a Go map
// and handing the maps to the parquet writer, the protobuf wire format is
// parsed straight into parquet column values.
//
// The direct path is deliberately narrow. It only accepts schemas and
// messages for which its output is known to be identical to decoding with
// schema_registry_decode (serialize_to_json: false) and then encoding with
// parquet_encode. Everything else is refused, either when the plan is built
// or while encoding, and the caller is expected to run the regular path on
// the untouched batch instead. Refusing is always safe, accepting is not, so
// when in doubt this code refuses.

// EncodeProcessorFields returns the config fields of the parquet_encode
// processor so that processors embedding it expose the same options.
func EncodeProcessorFields() []*service.ConfigField {
	return parquetEncodeFields()
}

// EncodeProcessor is the parquet_encode processor, exported for processors
// that embed it.
type EncodeProcessor struct {
	p *parquetEncodeProcessor
}

// NewEncodeProcessorFromConfig creates a parquet_encode processor from a
// config containing the fields returned by EncodeProcessorFields.
func NewEncodeProcessorFromConfig(conf *service.ParsedConfig, logger *service.Logger) (*EncodeProcessor, error) {
	p, err := newParquetEncodeProcessorFromConfig(conf, logger)
	if err != nil {
		return nil, err
	}
	return &EncodeProcessor{p: p}, nil
}

// ProcessBatch encodes a batch of structured messages exactly as the
// parquet_encode processor does.
func (e *EncodeProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	return e.p.ProcessBatch(ctx, batch)
}

// Close the processor.
func (e *EncodeProcessor) Close(ctx context.Context) error {
	return e.p.Close(ctx)
}

// ProtobufDirectOptions are the schema_registry_decode protobuf options that
// change what the decoded message looks like, and therefore what the direct
// path has to reproduce.
type ProtobufDirectOptions struct {
	UseProtoNames     bool
	UseEnumNumbers    bool
	EmitUnpopulated   bool
	EmitDefaultValues bool
}

// maxDirectFieldNumber bounds the field number lookup table of a plan.
const maxDirectFieldNumber = 1 << 16

// directChunkRows is how many rows are buffered before column values are
// handed to the parquet column writers.
const directChunkRows = 1024

// canonicalFloat32NaN is the bit pattern of float32(math.NaN()).
const canonicalFloat32NaN = 0x7fc00000

// errDirectRefused is wrapped by every error returned while encoding.
var errDirectRefused = errors.New("message not eligible for direct parquet encoding")

func refuse(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errDirectRefused, fmt.Sprintf(format, args...))
}

// directField says how one protobuf field is validated and, when it maps to a
// parquet column, written.
type directField struct {
	known bool
	// col is the parquet leaf column index, or -1 when the field has no
	// column and is only validated.
	col      int
	src      protoreflect.Kind
	dst      parquet.Kind
	wire     protowire.Type
	repeated bool
	// implicit is true for fields without presence tracking. A canonical
	// encoder never writes their zero value, and decoders treat a written
	// zero as unset, so a zero on the wire is refused.
	implicit bool
	maxDef   int
	// emptyNull is set for bytes fields written to optional columns, where
	// the regular path writes an empty value as null.
	emptyNull bool
	// enumNames is set when enum values are written as their names.
	enumNames   map[int32][]byte
	enumDefault []byte
}

// ProtobufPlan maps one protobuf message type onto the columns of the
// processor's parquet schema.
type ProtobufPlan struct {
	fields []directField
	// absent holds, per parquet column, the value written for a row that has
	// no value for the column.
	absent []parquet.Value
}

// directColumn describes a parquet leaf column usable by the direct path.
type directColumn struct {
	index    int
	kind     parquet.Kind
	repeated bool
	optional bool
	// text is true for byte array columns annotated as strings or enums.
	text bool
}

func directColumns(schema *parquet.Schema) (map[string]directColumn, error) {
	cols := map[string]directColumn{}
	for _, f := range schema.Fields() {
		name := f.Name()
		if !f.Leaf() {
			return nil, fmt.Errorf("parquet column %q is a group", name)
		}
		leaf, ok := schema.Lookup(name)
		if !ok {
			return nil, fmt.Errorf("parquet column %q not found in schema", name)
		}
		c := directColumn{
			index:    leaf.ColumnIndex,
			kind:     f.Type().Kind(),
			repeated: f.Repeated(),
			optional: f.Optional(),
		}
		switch c.kind {
		case parquet.Boolean, parquet.Int32, parquet.Int64, parquet.Float, parquet.Double, parquet.ByteArray:
		default:
			return nil, fmt.Errorf("parquet column %q has unsupported physical type %v", name, c.kind)
		}
		if lt := f.Type().LogicalType(); lt != nil {
			switch {
			case lt.UTF8 != nil && c.kind == parquet.ByteArray:
				c.text = true
			case lt.Enum != nil && c.kind == parquet.ByteArray:
				c.text = true
			case lt.Integer != nil && lt.Integer.IsSigned && (c.kind == parquet.Int32 || c.kind == parquet.Int64):
			default:
				return nil, fmt.Errorf("parquet column %q has an unsupported logical type", name)
			}
		}
		wantDef, wantRep := 0, 0
		if c.repeated {
			wantDef, wantRep = 1, 1
		} else if c.optional {
			wantDef = 1
		}
		if leaf.MaxDefinitionLevel != wantDef || leaf.MaxRepetitionLevel != wantRep {
			return nil, fmt.Errorf("parquet column %q has unexpected nesting levels", name)
		}
		cols[name] = c
	}
	return cols, nil
}

func zeroValue(kind parquet.Kind) parquet.Value {
	switch kind {
	case parquet.Boolean:
		return parquet.BooleanValue(false)
	case parquet.Int32:
		return parquet.Int32Value(0)
	case parquet.Int64:
		return parquet.Int64Value(0)
	case parquet.Float:
		return parquet.FloatValue(0)
	case parquet.Double:
		return parquet.DoubleValue(0)
	default:
		return parquet.ByteArrayValue([]byte{})
	}
}

// directKindAllowed reports whether values of a protobuf kind may be written
// to a parquet column of the given physical type. Only pairs for which the
// regular path performs no lossy or surprising conversion are allowed.
func directKindAllowed(src protoreflect.Kind, enumNumbers bool, c directColumn) bool {
	switch src {
	case protoreflect.BoolKind:
		return c.kind == parquet.Boolean
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return c.kind == parquet.Int32 || c.kind == parquet.Int64
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return c.kind == parquet.Int64
	case protoreflect.FloatKind:
		return c.kind == parquet.Float || c.kind == parquet.Double
	case protoreflect.DoubleKind:
		return c.kind == parquet.Double
	case protoreflect.StringKind:
		return c.kind == parquet.ByteArray
	case protoreflect.BytesKind:
		return c.kind == parquet.ByteArray && !c.text
	case protoreflect.EnumKind:
		if enumNumbers {
			return c.kind == parquet.Int32 || c.kind == parquet.Int64
		}
		return c.kind == parquet.ByteArray
	default:
		return false
	}
}

func directWireType(k protoreflect.Kind) (protowire.Type, bool) {
	switch k {
	case protoreflect.BoolKind, protoreflect.EnumKind,
		protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Uint32Kind,
		protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Uint64Kind:
		return protowire.VarintType, true
	case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind, protoreflect.FloatKind:
		return protowire.Fixed32Type, true
	case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind, protoreflect.DoubleKind:
		return protowire.Fixed64Type, true
	case protoreflect.StringKind, protoreflect.BytesKind:
		return protowire.BytesType, true
	default:
		return 0, false
	}
}

// NewProtobufPlan returns a plan for writing messages of the given type
// straight to parquet, or an error saying why the regular path has to be used
// for them.
func (e *EncodeProcessor) NewProtobufPlan(md protoreflect.MessageDescriptor, opts ProtobufDirectOptions) (*ProtobufPlan, error) {
	if e.p.schema == nil || e.p.schemaMeta != "" {
		return nil, errors.New("the parquet schema is taken from metadata rather than the config")
	}
	cols, err := directColumns(e.p.schema)
	if err != nil {
		return nil, err
	}
	if md.FullName().Parent() == "google.protobuf" {
		return nil, fmt.Errorf("message %v is a well known type", md.FullName())
	}
	if md.ExtensionRanges().Len() > 0 {
		return nil, fmt.Errorf("message %v declares extension ranges", md.FullName())
	}

	numCols := len(e.p.schema.Columns())
	plan := &ProtobufPlan{absent: make([]parquet.Value, numCols)}
	for _, c := range cols {
		if c.repeated || c.optional {
			plan.absent[c.index] = parquet.Value{}.Level(0, 0, c.index)
		} else {
			plan.absent[c.index] = zeroValue(c.kind).Level(0, 0, c.index)
		}
	}

	emitDefaults := opts.EmitUnpopulated || opts.EmitDefaultValues
	usedCols := map[int]protoreflect.Name{}
	fds := md.Fields()
	for i := range fds.Len() {
		fd := fds.Get(i)
		name := fd.Name()
		num := int(fd.Number())
		if num >= maxDirectFieldNumber {
			return nil, fmt.Errorf("field %v has number %d, above the supported maximum", name, num)
		}
		if fd.IsMap() {
			return nil, fmt.Errorf("field %v is a map", name)
		}
		if fd.Cardinality() == protoreflect.Required {
			return nil, fmt.Errorf("field %v is required", name)
		}
		if oo := fd.ContainingOneof(); oo != nil && !oo.IsSynthetic() {
			return nil, fmt.Errorf("field %v is part of a oneof", name)
		}
		wire, ok := directWireType(fd.Kind())
		if !ok {
			return nil, fmt.Errorf("field %v is a nested message", name)
		}
		f := directField{
			known:    true,
			col:      -1,
			src:      fd.Kind(),
			wire:     wire,
			repeated: fd.IsList(),
			implicit: !fd.IsList() && !fd.HasPresence(),
		}
		if fd.Kind() == protoreflect.EnumKind {
			ed := fd.Enum()
			if ed.IsClosed() {
				return nil, fmt.Errorf("field %v is a closed enum", name)
			}
			if ed.FullName() == "google.protobuf.NullValue" {
				return nil, fmt.Errorf("field %v is a google.protobuf.NullValue", name)
			}
			if ed.Values().Len() == 0 {
				return nil, fmt.Errorf("field %v is an enum without values", name)
			}
			if !opts.UseEnumNumbers {
				// Mirrors the name lookup of the regular decoder: the
				// declared name, else the default value, else the first.
				f.enumNames = map[int32][]byte{}
				evs := ed.Values()
				for j := range evs.Len() {
					ev := evs.Get(j)
					if byNum := evs.ByNumber(ev.Number()); byNum != nil {
						f.enumNames[int32(ev.Number())] = []byte(byNum.Name())
					}
				}
				def := fd.DefaultEnumValue()
				if def == nil {
					def = evs.Get(0)
				}
				f.enumDefault = []byte(def.Name())
			}
		}

		key := fd.JSONName()
		if opts.UseProtoNames {
			key = fd.TextName()
		}
		if c, ok := cols[key]; ok {
			if prev, dup := usedCols[c.index]; dup {
				return nil, fmt.Errorf("fields %v and %v both map to parquet column %q", prev, name, key)
			}
			usedCols[c.index] = name
			if c.repeated != fd.IsList() {
				return nil, fmt.Errorf("field %v and parquet column %q disagree on being repeated", name, key)
			}
			if !directKindAllowed(fd.Kind(), opts.UseEnumNumbers, c) {
				return nil, fmt.Errorf("field %v of kind %v cannot be written directly to parquet column %q of type %v", name, fd.Kind(), key, c.kind)
			}
			f.col = c.index
			f.dst = c.kind
			if c.repeated || c.optional {
				f.maxDef = 1
			}
			f.emptyNull = fd.Kind() == protoreflect.BytesKind && c.optional
			if f.implicit && emitDefaults && !f.emptyNull {
				// The regular decoder emits the zero value of unset fields
				// without presence, which lands as a value rather than null.
				zero := zeroValue(c.kind)
				if f.enumNames != nil {
					name, ok := f.enumNames[0]
					if !ok {
						name = f.enumDefault
					}
					zero = parquet.ByteArrayValue(name)
				}
				plan.absent[c.index] = zero.Level(0, f.maxDef, c.index)
			}
		}

		for num >= len(plan.fields) {
			plan.fields = append(plan.fields, directField{col: -1})
		}
		plan.fields[num] = f
	}
	return plan, nil
}

// directEncoder holds the state of one direct encode call.
type directEncoder struct {
	cols [][]parquet.Value
	// colRow[col] is the row (plus one) that last wrote to the column.
	colRow []int
	// fieldRow[num] is the row (plus one) that last carried the field.
	fieldRow []int
}

// EncodeProtobuf writes one parquet file from protobuf encoded messages, where
// plans[i] is the plan for data[i]. Any error means nothing usable was
// produced and the batch must go through the regular path instead.
func (e *EncodeProcessor) EncodeProtobuf(plans []*ProtobufPlan, data [][]byte) (file []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			file, err = nil, refuse("encoding panic: %v", r)
		}
	}()
	if len(plans) != len(data) {
		return nil, refuse("plans and data differ in length")
	}

	buf := bytes.NewBuffer(nil)
	w := parquet.NewGenericWriter[any](buf, e.p.schema, parquet.Compression(e.p.compressionType))
	writers := w.ColumnWriters()

	enc := directEncoder{
		cols:   make([][]parquet.Value, len(writers)),
		colRow: make([]int, len(writers)),
	}
	for i := range enc.cols {
		enc.cols[i] = make([]parquet.Value, 0, directChunkRows+64)
	}
	for _, p := range plans {
		if len(p.absent) != len(writers) {
			return nil, refuse("plan does not belong to this schema")
		}
		if len(p.fields) > len(enc.fieldRow) {
			enc.fieldRow = make([]int, len(p.fields))
		}
	}

	flush := func() error {
		for i, vals := range enc.cols {
			if len(vals) == 0 {
				continue
			}
			if _, err := writers[i].WriteRowValues(vals); err != nil {
				return refuse("writing column %d: %v", i, err)
			}
			enc.cols[i] = vals[:0]
		}
		return nil
	}

	for r, b := range data {
		row := r + 1
		p := plans[r]
		if err := enc.message(p, row, b); err != nil {
			return nil, err
		}
		for col, last := range enc.colRow {
			if last != row {
				enc.cols[col] = append(enc.cols[col], p.absent[col])
			}
		}
		if row%directChunkRows == 0 {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, refuse("closing writer: %v", err)
	}
	return buf.Bytes(), nil
}

// message parses one protobuf message and appends its values to the column
// buffers. It refuses anything a strict decoder could treat differently from
// the plain reading done here.
func (enc *directEncoder) message(p *ProtobufPlan, row int, b []byte) error {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return refuse("malformed field tag")
		}
		b = b[n:]
		if int(num) >= len(p.fields) || !p.fields[num].known {
			return refuse("unknown field number %d", num)
		}
		f := &p.fields[num]
		if !f.repeated {
			if enc.fieldRow[num] == row {
				return refuse("field number %d appears more than once", num)
			}
		}
		enc.fieldRow[num] = row

		switch {
		case typ == f.wire && typ != protowire.BytesType:
			u, n := consumeScalar(typ, b)
			if n < 0 {
				return refuse("malformed value for field number %d", num)
			}
			b = b[n:]
			if err := enc.scalar(f, row, num, u); err != nil {
				return err
			}
		case typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return refuse("malformed value for field number %d", num)
			}
			b = b[n:]
			if f.wire == protowire.BytesType {
				if f.implicit && len(v) == 0 {
					return refuse("explicit zero value for field number %d", num)
				}
				if f.src == protoreflect.StringKind && !directStringAccepted(v) {
					return refuse("string of field number %d is not accepted as UTF-8 by every decoder", num)
				}
				switch {
				case f.col < 0:
				case f.emptyNull && len(v) == 0:
					enc.colRow[f.col] = row
					enc.cols[f.col] = append(enc.cols[f.col], parquet.Value{}.Level(0, 0, f.col))
				default:
					// Aliases the input message, which outlives the write.
					enc.emit(f, row, parquet.ByteArrayValue(v))
				}
				break
			}
			if !f.repeated {
				return refuse("unexpected wire type for field number %d", num)
			}
			// Packed repeated scalars.
			for len(v) > 0 {
				u, n := consumeScalar(f.wire, v)
				if n < 0 {
					return refuse("malformed packed value for field number %d", num)
				}
				v = v[n:]
				if err := enc.scalar(f, row, num, u); err != nil {
					return err
				}
			}
		default:
			return refuse("unexpected wire type for field number %d", num)
		}
	}
	return nil
}

// directStringAccepted reports whether a string is valid UTF-8 that the
// regular decoder is known to accept. The regular decoder rejects some valid
// UTF-8: code points from U+80000 up, which are unassigned or private use.
// Code points from U+40000 up are refused here to stay well clear of that.
func directStringAccepted(v []byte) bool {
	for i, c := range v {
		if c < utf8.RuneSelf {
			continue
		}
		rest := v[i:]
		if !utf8.Valid(rest) {
			return false
		}
		for _, c := range rest {
			// In valid UTF-8, 0xF1 to 0xF4 only occur as the lead byte of
			// code points from U+40000 up.
			if c >= 0xF1 {
				return false
			}
		}
		return true
	}
	return true
}

func consumeScalar(typ protowire.Type, b []byte) (uint64, int) {
	switch typ {
	case protowire.VarintType:
		return protowire.ConsumeVarint(b)
	case protowire.Fixed32Type:
		v, n := protowire.ConsumeFixed32(b)
		return uint64(v), n
	default:
		return protowire.ConsumeFixed64(b)
	}
}

// emit appends a value to the field's column with the levels of its position
// in the row.
func (enc *directEncoder) emit(f *directField, row int, v parquet.Value) {
	rep := 0
	if enc.colRow[f.col] == row {
		rep = 1
	}
	enc.colRow[f.col] = row
	enc.cols[f.col] = append(enc.cols[f.col], v.Level(rep, f.maxDef, f.col))
}

// scalar validates one varint or fixed width value and writes it when the
// field has a column. u holds the raw varint, or the raw fixed width bits.
func (enc *directEncoder) scalar(f *directField, row int, num protowire.Number, u uint64) error {
	var x int64
	switch f.src {
	case protoreflect.BoolKind:
		if u > 1 {
			return refuse("non canonical bool for field number %d", num)
		}
		if f.implicit && u == 0 {
			return refuse("explicit zero value for field number %d", num)
		}
		if f.col >= 0 {
			enc.emit(f, row, parquet.BooleanValue(u == 1))
		}
		return nil
	case protoreflect.FloatKind:
		v := math.Float32frombits(uint32(u))
		if f.implicit && v == 0 {
			return refuse("explicit zero value for field number %d", num)
		}
		if v != v && uint32(u) != canonicalFloat32NaN {
			// The regular path widens to float64 and narrows again, which
			// does not preserve the bits of every NaN.
			return refuse("non canonical NaN for field number %d", num)
		}
		if f.col >= 0 {
			if f.dst == parquet.Float {
				enc.emit(f, row, parquet.FloatValue(v))
			} else {
				enc.emit(f, row, parquet.DoubleValue(float64(v)))
			}
		}
		return nil
	case protoreflect.DoubleKind:
		v := math.Float64frombits(u)
		if f.implicit && v == 0 {
			return refuse("explicit zero value for field number %d", num)
		}
		if f.col >= 0 {
			enc.emit(f, row, parquet.DoubleValue(v))
		}
		return nil
	case protoreflect.Int32Kind, protoreflect.EnumKind:
		x = int64(u)
		if x != int64(int32(x)) {
			return refuse("out of range 32 bit value for field number %d", num)
		}
	case protoreflect.Sint32Kind:
		if u > math.MaxUint32 {
			return refuse("out of range 32 bit value for field number %d", num)
		}
		x = protowire.DecodeZigZag(u)
	case protoreflect.Uint32Kind:
		if u > math.MaxUint32 {
			return refuse("out of range 32 bit value for field number %d", num)
		}
		x = int64(u)
	case protoreflect.Sint64Kind:
		x = protowire.DecodeZigZag(u)
	case protoreflect.Fixed32Kind:
		x = int64(uint32(u))
	case protoreflect.Sfixed32Kind:
		x = int64(int32(uint32(u)))
	default: // Int64Kind, Uint64Kind, Fixed64Kind, Sfixed64Kind
		x = int64(u)
	}
	if f.implicit && x == 0 {
		return refuse("explicit zero value for field number %d", num)
	}
	if f.col < 0 {
		return nil
	}
	switch {
	case f.enumNames != nil:
		name, ok := f.enumNames[int32(x)]
		if !ok {
			name = f.enumDefault
		}
		enc.emit(f, row, parquet.ByteArrayValue(name))
	case f.dst == parquet.Int32:
		enc.emit(f, row, parquet.Int32Value(int32(x)))
	default:
		enc.emit(f, row, parquet.Int64Value(x))
	}
	return nil
}
