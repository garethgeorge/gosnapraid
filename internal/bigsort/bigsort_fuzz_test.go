package bigsort

import (
	"bytes"
	"math/rand"
	"slices"
	"testing"

	"github.com/garethgeorge/gosnapraid/internal/bigsort/buffers"
)

func FuzzBigSorterConfigs(f *testing.F) {
	// Seed with some interesting configurations
	f.Add(true, uint32(1024), uint32(8))        // Compressed, 1KB buffer, 8 iter chunk
	f.Add(true, uint32(1024*128), uint32(256))  // Compressed, 128KB buffer, 256 iter chunk
	f.Add(false, uint32(1024*512), uint32(512)) // Uncompressed, 512KB buffer, 512 iter chunk
	f.Add(true, uint32(128), uint32(1))         // Compressed, tiny buffer, 1 iter chunk

	f.Fuzz(func(t *testing.T, compressed bool, maxBufferSizeBytes uint32, iterChunkSize uint32) {
		// Constrain the fuzzed values to a reasonable range.
		const minMaxBufferSizeBytes = 128
		const maxMaxBufferSizeBytes = 1 * 1024 * 1024
		if maxBufferSizeBytes < minMaxBufferSizeBytes {
			maxBufferSizeBytes = minMaxBufferSizeBytes
		}
		if maxBufferSizeBytes > maxMaxBufferSizeBytes {
			maxBufferSizeBytes = maxMaxBufferSizeBytes
		}
		if iterChunkSize < 1 {
			iterChunkSize = 1
		}
		if iterChunkSize > 512 {
			iterChunkSize = 512
		}

		var factory buffers.BufferFactory
		if compressed {
			factory = buffers.NewCompressedBufferFactory(buffers.NewInMemoryBufferFactory())
		} else {
			factory = buffers.NewInMemoryBufferFactory()
		}
		defer factory.Release()

		opts := []func(*options){
			WithMaxBufferSizeBytes(int64(maxBufferSizeBytes)),
			WithIterChunkSize(int(iterChunkSize)),
		}

		sorter := NewBigSorter[ByteKeySortable](factory, opts...)
		defer sorter.Close()

		// Generate 1MB of data with random key sizes.
		const totalDataSize = 1 * 1024 * 1024
		var generatedItems []ByteKeySortable
		var currentSize int64
		for currentSize < totalDataSize {
			keySize := rand.Intn(256) + 1 // 1 to 256 bytes key
			key := make([]byte, keySize)
			_, err := rand.Read(key)
			if err != nil {
				t.Fatalf("Failed to generate random key: %v", err)
			}
			item := ByteKeySortable{Key: key, Value: []byte{}}
			generatedItems = append(generatedItems, item)
			currentSize += item.Size()
		}

		for _, item := range generatedItems {
			if err := sorter.Add(item); err != nil {
				t.Fatalf("failed to add item: %v", err)
			}
		}

		if err := sorter.Flush(); err != nil {
			t.Fatalf("failed to flush: %v", err)
		}

		// Always verify for this fuzzer.
		iterator := sorter.SortIter()

		var retrievedItems []ByteKeySortable
		for item := range iterator.Iter() {
			keyCopy := slices.Clone(item.Key)
			valueCopy := slices.Clone(item.Value)
			retrievedItems = append(retrievedItems, ByteKeySortable{Key: keyCopy, Value: valueCopy})
		}

		if err := iterator.Err(); err != nil {
			t.Fatalf("iteration failed: %v", err)
		}

		if len(retrievedItems) != len(generatedItems) {
			t.Fatalf("expected %d items, but got %d", len(generatedItems), len(retrievedItems))
		}

		// Sort the original items to compare against the sorted output.
		slices.SortFunc(generatedItems, func(a, b ByteKeySortable) int {
			return bytes.Compare(a.Key, b.Key)
		})

		// Compare item by item
		for i := 0; i < len(generatedItems); i++ {
			if !bytes.Equal(generatedItems[i].Key, retrievedItems[i].Key) {
				t.Fatalf("Item %d: key mismatch. expected %x, got %x", i, generatedItems[i].Key, retrievedItems[i].Key)
			}
			if !bytes.Equal(generatedItems[i].Value, retrievedItems[i].Value) {
				t.Fatalf("Item %d: value mismatch. expected %x, got %x", i, generatedItems[i].Value, retrievedItems[i].Value)
			}
		}
	})
}
