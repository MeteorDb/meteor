package common

import (
	"encoding/binary"
	"math"
)

type BinaryBuffer struct {
	buf     *[]byte
	offset  uint64
	managed bool // true if buffer is internally owned and resizable
}

func NewBinaryBuffer(initialSize int) *BinaryBuffer {
	buf := make([]byte, 0, initialSize)
	return &BinaryBuffer{
		buf:     &buf,
		offset:  0,
		managed: true,
	}
}

func NewBinaryBufferFrom(buf *[]byte, offset uint64) *BinaryBuffer {
	return &BinaryBuffer{
		buf:     buf,
		offset:  offset,
		managed: false,
	}
}

func (b *BinaryBuffer) ensureCapacity(n uint64) {
	if !b.managed {
		// Do nothing for externally provided buffer
		return
	}
	required := b.offset + n
	if uint64(cap(*b.buf)) < required {
		newCap := max(uint64(cap(*b.buf))*2, required)
		newBuf := make([]byte, len(*b.buf), newCap)
		copy(newBuf, *b.buf)
		*b.buf = newBuf
	}
	if uint64(len(*b.buf)) < required {
		*b.buf = (*b.buf)[:required]
	}
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func (b *BinaryBuffer) GetOffset() uint64 {
	return b.offset
}

func (b *BinaryBuffer) GetBuffer() []byte {
	return (*b.buf)[:b.offset]
}

func (b *BinaryBuffer) ResetOffset() *BinaryBuffer {
	b.offset = 0
	return b
}

func (b *BinaryBuffer) WriteUint8(value uint8) *BinaryBuffer {
	b.ensureCapacity(1)
	(*b.buf)[b.offset] = value
	b.offset += 1
	return b
}

func (b *BinaryBuffer) WriteUint16(value uint16) *BinaryBuffer {
	b.ensureCapacity(2)
	binary.BigEndian.PutUint16((*b.buf)[b.offset:], value)
	b.offset += 2
	return b
}

func (b *BinaryBuffer) WriteUint32(value uint32) *BinaryBuffer {
	b.ensureCapacity(4)
	binary.BigEndian.PutUint32((*b.buf)[b.offset:], value)
	b.offset += 4
	return b
}

func (b *BinaryBuffer) WriteUint64(value uint64) *BinaryBuffer {
	b.ensureCapacity(8)
	binary.BigEndian.PutUint64((*b.buf)[b.offset:], value)
	b.offset += 8
	return b
}

func (b *BinaryBuffer) WriteInt8(value int8) *BinaryBuffer {
	b.ensureCapacity(1)
	(*b.buf)[b.offset] = byte(value)
	b.offset += 1
	return b
}

func (b *BinaryBuffer) WriteInt16(value int16) *BinaryBuffer {
	b.ensureCapacity(2)
	binary.BigEndian.PutUint16((*b.buf)[b.offset:], uint16(value))
	b.offset += 2
	return b
}

func (b *BinaryBuffer) WriteInt32(value int32) *BinaryBuffer {
	b.ensureCapacity(4)
	binary.BigEndian.PutUint32((*b.buf)[b.offset:], uint32(value))
	b.offset += 4
	return b
}

func (b *BinaryBuffer) WriteInt64(value int64) *BinaryBuffer {
	b.ensureCapacity(8)
	binary.BigEndian.PutUint64((*b.buf)[b.offset:], uint64(value))
	b.offset += 8
	return b
}

func (b *BinaryBuffer) WriteBool(value bool) *BinaryBuffer {
	b.ensureCapacity(1)
	if value {
		(*b.buf)[b.offset] = 1
	} else {
		(*b.buf)[b.offset] = 0
	}
	b.offset += 1
	return b
}

func (b *BinaryBuffer) WriteFloat32(value float32) *BinaryBuffer {
	bits := math.Float32bits(value)
	b.ensureCapacity(4)
	binary.BigEndian.PutUint32((*b.buf)[b.offset:], bits)
	b.offset += 4
	return b
}

func (b *BinaryBuffer) WriteFloat64(value float64) *BinaryBuffer {
	bits := math.Float64bits(value)
	b.ensureCapacity(8)
	binary.BigEndian.PutUint64((*b.buf)[b.offset:], bits)
	b.offset += 8
	return b
}

func (b *BinaryBuffer) WriteBytes(value []byte) *BinaryBuffer {
	b.WriteUint32(uint32(len(value)))
	b.ensureCapacity(uint64(len(value)))
	copy((*b.buf)[b.offset:], value)
	b.offset += uint64(len(value))
	return b
}

func (b *BinaryBuffer) WriteString(value string) *BinaryBuffer {
	return b.WriteBytes([]byte(value))
}

func (b *BinaryBuffer) ReadUint8(out *uint8) *BinaryBuffer {
	*out = (*b.buf)[b.offset]
	b.offset += 1
	return b
}

func (b *BinaryBuffer) ReadUint16(out *uint16) *BinaryBuffer {
	*out = binary.BigEndian.Uint16((*b.buf)[b.offset:])
	b.offset += 2
	return b
}

func (b *BinaryBuffer) ReadUint32(out *uint32) *BinaryBuffer {
	*out = binary.BigEndian.Uint32((*b.buf)[b.offset:])
	b.offset += 4
	return b
}

func (b *BinaryBuffer) ReadUint64(out *uint64) *BinaryBuffer {
	*out = binary.BigEndian.Uint64((*b.buf)[b.offset:])
	b.offset += 8
	return b
}

func (b *BinaryBuffer) ReadInt8(out *int8) *BinaryBuffer {
	*out = int8((*b.buf)[b.offset])
	b.offset += 1
	return b
}

func (b *BinaryBuffer) ReadInt16(out *int16) *BinaryBuffer {
	*out = int16(binary.BigEndian.Uint16((*b.buf)[b.offset:]))
	b.offset += 2
	return b
}

func (b *BinaryBuffer) ReadInt32(out *int32) *BinaryBuffer {
	*out = int32(binary.BigEndian.Uint32((*b.buf)[b.offset:]))
	b.offset += 4
	return b
}

func (b *BinaryBuffer) ReadInt64(out *int64) *BinaryBuffer {
	*out = int64(binary.BigEndian.Uint64((*b.buf)[b.offset:]))
	b.offset += 8
	return b
}

func (b *BinaryBuffer) ReadBool(out *bool) *BinaryBuffer {
	*out = (*b.buf)[b.offset] != 0
	b.offset += 1
	return b
}

func (b *BinaryBuffer) ReadFloat32(out *float32) *BinaryBuffer {
	bits := binary.BigEndian.Uint32((*b.buf)[b.offset:])
	*out = math.Float32frombits(bits)
	b.offset += 4
	return b
}

func (b *BinaryBuffer) ReadFloat64(out *float64) *BinaryBuffer {
	bits := binary.BigEndian.Uint64((*b.buf)[b.offset:])
	*out = math.Float64frombits(bits)
	b.offset += 8
	return b
}

func (b *BinaryBuffer) ReadBytes(out *[]byte) *BinaryBuffer {
	var length uint32
	b.ReadUint32(&length)
	start := b.offset
	end := start + uint64(length)
	*out = (*b.buf)[start:end]
	b.offset = end
	return b
}

func (b *BinaryBuffer) ReadString(out *string) *BinaryBuffer {
	var bytes []byte
	b.ReadBytes(&bytes)
	*out = string(bytes)
	return b
}
