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
	keyLen := int(binary.LittleEndian.Uint16(data[:2]))
	valLen := int(binary.LittleEndian.Uint16(data[2:4]))

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
	binary.LittleEndian.PutUint16(buf[:2], uint16(keyLen))
	binary.LittleEndian.PutUint16(buf[2:4], uint16(valLen))
	copy(buf[4:4+keyLen], b.Key)
	copy(buf[4+keyLen:], b.Value)
	return buf[:4+keyLen+valLen]
}

func (b *ByteKeySortable) Size() int64 {
	return int64(4 + len(b.Key) + len(b.Value))
}

type Uint64KeySortable struct {
	Key   uint64
	Value []byte
}

func (u *Uint64KeySortable) Less(other BigSortable) bool {
	return u.Key < other.(*Uint64KeySortable).Key
}

func (u *Uint64KeySortable) Deserialize(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("data too short to deserialize Uint64KeySortable")
	}
	u.Key = binary.LittleEndian.Uint64(data[:8])
	u.Value = make([]byte, len(data[8:]))
	copy(u.Value, data[8:])
	return nil
}
func (u *Uint64KeySortable) Serialize(buf []byte) []byte {
	if len(buf) < 8+len(u.Value) {
		buf = make([]byte, 8+len(u.Value))
	}
	binary.LittleEndian.PutUint64(buf[:8], u.Key)
	copy(buf[8:], u.Value)
	return buf[:8+len(u.Value)]
}

func (u *Uint64KeySortable) Size() int64 {
	return int64(8 + len(u.Value))
}
