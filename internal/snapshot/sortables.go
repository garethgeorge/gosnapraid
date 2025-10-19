package snapshot

import (
	"encoding/binary"
	"fmt"

	"github.com/garethgeorge/gosnapraid/internal/bigsort"
)

type HashOrdered struct {
	hashhi uint64
	hashlo uint64
	value  []byte
}

var _ bigsort.BigSortable = (*HashOrdered)(nil)

func (f *HashOrdered) Less(other bigsort.BigSortable) bool {
	otherFile := other.(*HashOrdered)
	return f.hashhi < otherFile.hashhi || (f.hashhi == otherFile.hashhi && f.hashlo < otherFile.hashlo)
}

func (f *HashOrdered) Deserialize(data []byte) error {
	if len(data) < 16 {
		return fmt.Errorf("data too short to deserialize FileAndHash")
	}
	f.hashhi = binary.LittleEndian.Uint64(data[0:8])
	f.hashlo = binary.LittleEndian.Uint64(data[8:16])
	f.value = make([]byte, len(data[16:]))
	copy(f.value, data[16:])
	return nil
}

func (f *HashOrdered) Serialize(buf []byte) []byte {
	totalLen := 16 + len(f.value)
	if len(buf) < totalLen {
		buf = make([]byte, totalLen)
	}
	binary.LittleEndian.PutUint64(buf[0:8], f.hashhi)
	binary.LittleEndian.PutUint64(buf[8:16], f.hashlo)
	copy(buf[16:], f.value)
	return buf[:totalLen]
}

func (f *HashOrdered) Size() int64 {
	return 16 + int64(len(f.value))
}

type RangeToFile struct {
	sidx int64
	eidx int64
	path []byte
}

var _ bigsort.BigSortable = (*RangeToFile)(nil)

func (f *RangeToFile) Less(other bigsort.BigSortable) bool {
	otherFile := other.(*RangeToFile)
	return f.sidx < otherFile.sidx
}

func (f *RangeToFile) Deserialize(data []byte) error {
	if len(data) < 16 {
		return fmt.Errorf("data too short to deserialize FileAndRange")
	}
	f.sidx = int64(binary.LittleEndian.Uint64(data[0:8]))
	f.eidx = int64(binary.LittleEndian.Uint64(data[8:16]))
	f.path = make([]byte, len(data[16:]))
	copy(f.path, data[16:])
	return nil
}

func (f *RangeToFile) Serialize(buf []byte) []byte {
	totalLen := 16 + len(f.path)
	if len(buf) < totalLen {
		buf = make([]byte, totalLen)
	}
	binary.LittleEndian.PutUint64(buf[0:8], uint64(f.sidx))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(f.eidx))
	copy(buf[16:], f.path)
	return buf[:totalLen]
}

func (f *RangeToFile) Size() int64 {
	return 16 + int64(len(f.path))
}

type hashToRange struct {
	hashlo      uint64
	hashhi      uint64
	sliceStarts []uint64
	sliceEnds   []uint64
}
