package hashing

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/zeebo/xxh3"
)

type ContentHashAlgorithm uint8

const (
	CONTENT_HASH_ALGORITHM_UNKNOWN ContentHashAlgorithm = iota
	CONTENT_HASH_ALGORITHM_XXH3
)

type ContentHash struct {
	Lo        uint64
	Hi        uint64
	Algorithm ContentHashAlgorithm
}

func (h ContentHash) Compare(other ContentHash) int {
	if h.Lo != other.Lo {
		return int(h.Lo - other.Lo)
	}
	return int(h.Hi - other.Hi)
}

func DeserializeContentHash(reader io.Reader) (ContentHash, error) {
	var buf [1 + 8*2]byte
	if _, err := reader.Read(buf[:]); err != nil {
		return ContentHash{}, err
	}
	return ContentHash{
		Algorithm: ContentHashAlgorithm(buf[0]),
		Lo:        binary.LittleEndian.Uint64(buf[1:9]),
		Hi:        binary.LittleEndian.Uint64(buf[9:17]),
	}, nil
}

func (h ContentHash) Serialize(writer io.Writer) error {
	var buf [1 + 8*2]byte
	buf[0] = byte(h.Algorithm)
	binary.LittleEndian.PutUint64(buf[1:9], h.Lo)
	binary.LittleEndian.PutUint64(buf[9:17], h.Hi)
	_, err := writer.Write(buf[:])
	return err
}

func ContentHashFromBytes(algorithm ContentHashAlgorithm, data []byte) ContentHash {
	switch algorithm {
	case CONTENT_HASH_ALGORITHM_XXH3:
		h := xxh3.Hash128(data)
		return ContentHash{
			Lo:        h.Lo,
			Hi:        h.Hi,
			Algorithm: CONTENT_HASH_ALGORITHM_XXH3,
		}
	default:
		panic("unknown hash algorithm")
	}
}

func ContentHashFromReader(algorithm ContentHashAlgorithm, reader io.Reader) (ContentHash, error) {
	switch algorithm {
	case CONTENT_HASH_ALGORITHM_XXH3:
		h := xxh3.New()
		buffer := make([]byte, h.BlockSize())
		if _, err := io.CopyBuffer(h, reader, buffer); err != nil {
			return ContentHash{}, err
		}
		sum := h.Sum128()
		return ContentHash{
			Lo:        sum.Lo,
			Hi:        sum.Hi,
			Algorithm: CONTENT_HASH_ALGORITHM_XXH3,
		}, nil
	default:
		return ContentHash{}, fmt.Errorf("unknown hash algorithm: %d", algorithm)
	}
}
