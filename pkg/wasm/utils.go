package wasm

import (
	"bytes"
)

// writeLeb128 writes an unsigned LEB128 encoded value
func writeLeb128(buf *bytes.Buffer, value uint64) {
	for {
		byteVal := uint8(value & 0x7f)
		value >>= 7
		if value != 0 {
			byteVal |= 0x80
		}
		buf.WriteByte(byteVal)
		if value == 0 {
			break
		}
	}
}

// writeLeb128Signed writes a signed LEB128 encoded value
func writeLeb128Signed(buf *bytes.Buffer, value int64) {
	more := true
	for more {
		byteVal := uint8(value & 0x7f)
		value >>= 7
		if (value == 0 && (byteVal&0x40) == 0) || (value == -1 && (byteVal&0x40) != 0) {
			more = false
		} else {
			byteVal |= 0x80
		}
		buf.WriteByte(byteVal)
	}
}

// readLeb128 reads an unsigned LEB128 encoded value
func readLeb128(data []byte, offset int) (uint64, int) {
	var result uint64
	var shift uint
	pos := offset

	for {
		byteVal := data[pos]
		pos++
		result |= uint64(byteVal&0x7f) << shift
		if (byteVal & 0x80) == 0 {
			break
		}
		shift += 7
	}

	return result, pos - offset
}

// readLeb128Signed reads a signed LEB128 encoded value
func readLeb128Signed(data []byte, offset int) (int64, int) {
	var result int64
	var shift uint
	pos := offset

	for {
		byteVal := data[pos]
		pos++
		result |= int64(byteVal&0x7f) << shift
		shift += 7
		if (byteVal & 0x80) == 0 {
			if shift < 64 && (byteVal&0x40) != 0 {
				result |= -1 << shift
			}
			break
		}
	}

	return result, pos - offset
}
