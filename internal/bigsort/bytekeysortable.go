package bigsort

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type ByteKeySortable struct {
	Key   []byte
	Value []byte
}

func (b *ByteKeySortable) Less(other BigSortable) bool {
	return bytes.Compare(b.Key, other.(*ByteKeySortable).Key) < 0
}

func (b *ByteKeySortable) Deserialize(data []byte) error {
	// Read the length of the key (first 2 bytes)
	if len(data) < 4 {
		return fmt.Errorf("data too short to deserialize ByteKeySortable")
	}
	keyLen := int(binary.BigEndian.Uint16(data[:2]))
	valLen := int(binary.BigEndian.Uint16(data[2:4]))

	if len(data) < 4+keyLen+valLen {
		return fmt.Errorf("data too short to deserialize ByteKeySortable")
	}

	buf := make([]byte, keyLen+valLen)
	copy(buf, data[4:4+keyLen+valLen])
	b.Key = buf[:keyLen]
	b.Value = buf[keyLen : keyLen+valLen]
	return nil
}

func (b *ByteKeySortable) Serialize(buf []byte) []byte {
	if len(buf) < 4+len(b.Key)+len(b.Value) {
		buf = make([]byte, 4+len(b.Key)+len(b.Value))
	}
	keyLen := len(b.Key)
	valLen := len(b.Value)
	binary.BigEndian.PutUint16(buf[:2], uint16(keyLen))
	binary.BigEndian.PutUint16(buf[2:4], uint16(valLen))
	copy(buf[4:4+keyLen], b.Key)
	copy(buf[4+keyLen:], b.Value)
	return buf[:4+keyLen+valLen]
}

func (b *ByteKeySortable) Size() int64 {
	return int64(4 + len(b.Key) + len(b.Value))
}
