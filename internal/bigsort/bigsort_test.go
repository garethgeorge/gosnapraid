package bigsort

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/garethgeorge/gosnapraid/internal/bigsort/buffers"
)

func generateItems(t *testing.T, count int, keySize int) ([]ByteKeySortable, int64) {
	t.Helper()
	items := make([]ByteKeySortable, count)
	var totalSize int64
	for i := 0; i < count; i++ {
		key := make([]byte, keySize)
		if _, err := rand.Read(key); err != nil {
			t.Fatalf("failed to generate random key: %v", err)
		}
		// Use an empty value as per user instruction.
		items[i] = ByteKeySortable{Key: key, Value: []byte{}}
		totalSize += items[i].Size()
	}
	return items, totalSize
}

func TestBigSorter(t *testing.T) {
	// Disable logging for tests to keep output clean.
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	// Define test cases for different buffer factories
	testCases := []struct {
		name          string
		bufferFactory func(t *testing.T) buffers.BufferFactory
	}{
		{
			name: "InMemory",
			bufferFactory: func(t *testing.T) buffers.BufferFactory {
				return buffers.NewInMemoryBufferFactory()
			},
		},
		{
			name: "Dir",
			bufferFactory: func(t *testing.T) buffers.BufferFactory {
				dir := t.TempDir()
				factory, err := buffers.NewDirBufferFactory(filepath.Join(dir, "buffers"))
				if err != nil {
					t.Fatalf("failed to create dir buffer factory: %v", err)
				}
				return factory
			},
		},
		{
			name: "CompressedInMemory",
			bufferFactory: func(t *testing.T) buffers.BufferFactory {
				return buffers.NewCompressedBufferFactory(buffers.NewInMemoryBufferFactory())
			},
		},
		{
			name: "CompressedDir",
			bufferFactory: func(t *testing.T) buffers.BufferFactory {
				dir := t.TempDir()
				baseFactory, err := buffers.NewDirBufferFactory(filepath.Join(dir, "buffers"))
				if err != nil {
					t.Fatalf("failed to create dir buffer factory: %v", err)
				}
				return buffers.NewCompressedBufferFactory(baseFactory)
			},
		},
	}

	// Define buffer sizes to test with
	bufferSizes := []struct {
		name string
		size int64
	}{
		{"16KB", 16 * 1024},
		{"1MB", 1 * 1024 * 1024},
	}

	const itemCount = 5000
	const keySize = 32
	generatedItems, totalSize := generateItems(t, itemCount, keySize)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for _, bs := range bufferSizes {
				// Make buffer size small enough to ensure multiple buffers
				maxBlockSizeBytes := totalSize / 4
				if bs.size < maxBlockSizeBytes {
					maxBlockSizeBytes = bs.size
				}

				t.Run(bs.name, func(t *testing.T) {
					factory := tc.bufferFactory(t)
					defer factory.Release()

					sorter := NewBigSorter[ByteKeySortable](factory, WithMaxBufferSizeBytes(maxBlockSizeBytes))

					for _, item := range generatedItems {
						if err := sorter.Add(item); err != nil {
							t.Fatalf("failed to add item: %v", err)
						}
					}

					if err := sorter.Flush(); err != nil {
						t.Fatalf("failed to flush: %v", err)
					}

					// Assert that merging will happen
					if len(sorter.buffers) < 2 {
						t.Fatalf("expected at least 2 buffers to be created to test merging, but got %d", len(sorter.buffers))
					}

					iterator := sorter.SortIter()

					var sortedItems []ByteKeySortable
					for item := range iterator.Iter() {
						// We must copy the item as it may be backed by a reusable buffer
						keyCopy := slices.Clone(item.Key)
						valueCopy := slices.Clone(item.Value)
						sortedItems = append(sortedItems, ByteKeySortable{Key: keyCopy, Value: valueCopy})
					}

					// Assert no error occurred during iteration
					if err := iterator.Err(); err != nil {
						t.Fatalf("iteration failed: %v", err)
					}

					// Assert all items were retrieved
					if len(sortedItems) != len(generatedItems) {
						t.Fatalf("expected %d items, but got %d", len(generatedItems), len(sortedItems))
					}

					// Assert the output is sorted
					if !slices.IsSortedFunc(sortedItems, func(a, b ByteKeySortable) int {
						return bytes.Compare(a.Key, b.Key)
					}) {
						t.Fatal("output is not sorted")
					}

					// Assert the output set matches the input set
					expectedItems := slices.Clone(generatedItems)
					slices.SortFunc(expectedItems, func(a, b ByteKeySortable) int {
						return bytes.Compare(a.Key, b.Key)
					})

					if !reflect.DeepEqual(sortedItems, expectedItems) {
						t.Fatal("sorted output does not match expected output")
					}
				})
			}
		})
	}
}

func TestBigSorter_Cancellation(t *testing.T) {
	// Disable logging for tests to keep output clean.
	log.SetOutput(io.Discard)
	defer log.SetOutput(os.Stderr)

	const itemCount = 2000
	const valueSize = 100
	const maxBlockSizeBytes = (itemCount * valueSize) / 4 // Ensure multiple buffers
	items, _ := generateItems(t, itemCount, valueSize)

	factory := buffers.NewInMemoryBufferFactory()
	defer factory.Release()

	sorter := NewBigSorter[ByteKeySortable](factory, WithMaxBufferSizeBytes(maxBlockSizeBytes))

	for _, item := range items {
		if err := sorter.Add(item); err != nil {
			t.Fatalf("failed to add item: %v", err)
		}
	}
	if err := sorter.Flush(); err != nil {
		t.Fatalf("failed to flush: %v", err)
	}

	if len(sorter.buffers) < 2 {
		t.Fatalf("expected at least 2 buffers, but got %d", len(sorter.buffers))
	}

	iterator := sorter.SortIter()
	iteratedCount := 0
	const breakAfter = 5

	for range iterator.Iter() {
		iteratedCount++
		if iteratedCount >= breakAfter {
			break
		}
	}

	// Assert that breaking early does not result in an error
	if err := iterator.Err(); err != nil {
		t.Fatalf("expected no error on cancellation, but got: %v", err)
	}

	if iteratedCount != breakAfter {
		t.Fatalf("expected to iterate %d times, but iterated %d times", breakAfter, iteratedCount)
	}
}

// This test is to ensure that the heap comparison logic is correct.
// It was found during development that not taking a pointer to the slice element
// for the Less method call resulted in incorrect sorting.
func TestBigSorter_Correctness(t *testing.T) {
	bs := NewBigSorter[ByteKeySortable](buffers.NewInMemoryBufferFactory(), WithMaxBufferSizeBytes(1024))

	// Insert items in reverse order to test sorting explicitly
	for i := 100; i > 0; i-- {
		err := bs.Add(ByteKeySortable{
			Key:   []byte(fmt.Sprintf("%03d", i)),
			Value: []byte("v"),
		})
		if err != nil {
			t.Fatalf("add: %v", err)
		}
	}

	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	iter := bs.SortIter()
	count := 1
	for elem := range iter.Iter() {
		keyStr := string(elem.Key)
		expectedKeyStr := fmt.Sprintf("%03d", count)
		if keyStr != expectedKeyStr {
			t.Fatalf("expected key %s but got %s", expectedKeyStr, keyStr)
		}
		count++
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iteration failed: %v", err)
	}
}

// TestBigSorter_PanicOnUnflushed demonstrates that calling SortIter with unflushed data panics.
func TestBigSorter_PanicOnUnflushed(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	bs := NewBigSorter[ByteKeySortable](buffers.NewInMemoryBufferFactory(), WithMaxBufferSizeBytes(1024))
	_ = bs.Add(ByteKeySortable{Key: []byte("key"), Value: []byte("value")})

	// This should panic
	_ = bs.SortIter()
}

// TestBigSorter_Empty demonstrates that the sorter works correctly with no items.
func TestBigSorter_Empty(t *testing.T) {
	bs := NewBigSorter[ByteKeySortable](buffers.NewInMemoryBufferFactory(), WithMaxBufferSizeBytes(1024))

	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	iter := bs.SortIter()
	count := 0
	for range iter.Iter() {
		count++
	}

	if err := iter.Err(); err != nil {
		t.Fatalf("iteration failed: %v", err)
	}

	if count != 0 {
		t.Fatalf("expected 0 items, but got %d", count)
	}
}

// TestBigSorter_SingleItem demonstrates that the sorter works correctly with one item.
func TestBigSorter_SingleItem(t *testing.T) {
	bs := NewBigSorter[ByteKeySortable](buffers.NewInMemoryBufferFactory(), WithMaxBufferSizeBytes(1024))

	item := ByteKeySortable{Key: []byte("thekey"), Value: []byte("thevalue")}
	if err := bs.Add(item); err != nil {
		t.Fatalf("add: %v", err)
	}

	if err := bs.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	iter := bs.SortIter()
	var items []ByteKeySortable
	for elem := range iter.Iter() {
		items = append(items, elem)
	}

	if err := iter.Err(); err != nil {
		t.Fatalf("iteration failed: %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("expected 1 item, but got %d", len(items))
	}
	if !reflect.DeepEqual(items[0], item) {
		t.Fatalf("retrieved item does not match original item")
	}
}

// Mock factory and handle to inject errors for testing
type errorBufferFactory struct {
	sync.Mutex
	newCount      int
	newErrOnCount int // 0 means no error
}

func (f *errorBufferFactory) New() (buffers.BufferHandle, error) {
	f.Lock()
	defer f.Unlock()
	f.newCount++
	if f.newErrOnCount > 0 && f.newCount == f.newErrOnCount {
		return nil, fmt.Errorf("injected error on New")
	}
	return &errorBufferHandle{}, nil
}
func (f *errorBufferFactory) Release() error { return nil }

type errorBufferHandle struct {
	writeErr bool
	readErr  bool
}

func (h *errorBufferHandle) GetWriter() (io.WriteCloser, error) {
	return &errorReadWriteCloser{writeErr: h.writeErr}, nil
}
func (h *errorBufferHandle) GetReader() (io.ReadCloser, error) {
	return &errorReadWriteCloser{readErr: h.readErr}, nil
}

type errorReadWriteCloser struct {
	writeErr bool
	readErr  bool
}

func (w *errorReadWriteCloser) Write(p []byte) (n int, err error) {
	if w.writeErr {
		return 0, fmt.Errorf("injected write error")
	}
	return len(p), nil
}
func (w *errorReadWriteCloser) Read(p []byte) (n int, err error) {
	if w.readErr {
		return 0, fmt.Errorf("injected read error")
	}
	return 0, io.EOF
}
func (w *errorReadWriteCloser) Close() error { return nil }

func TestBigSorter_IteratorError(t *testing.T) {
	factory := &errorBufferFactory{}
	sorter := NewBigSorter[ByteKeySortable](factory, WithMaxBufferSizeBytes(10))
	_ = sorter.Add(ByteKeySortable{Key: []byte("a"), Value: []byte("b")})
	_ = sorter.Flush()

	// Inject an error on reading from the buffer
	handle := sorter.buffers[0].(*errorBufferHandle)
	handle.readErr = true

	iterator := sorter.SortIter()
	for range iterator.Iter() {
		// We expect the loop to terminate early due to the error
	}

	err := iterator.Err()
	if err == nil {
		t.Fatal("expected an error from iterator, but got nil")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("injected read error")) {
		t.Fatalf("expected error to contain 'injected read error', but got: %v", err)
	}
}

func BenchmarkBigSorter_Add(b *testing.B) {
	b.ReportAllocs()

	// Use a fixed key and value size for consistent item size
	const keySize = 32
	const valueSize = 100
	// From ByteKeySortable's Serialize method: 2 bytes for key length, 2 for value length
	const itemSize = int64(4 + keySize + valueSize)
	const totalDataSize = 5 * 1024 * 1024 * 1024 // 5 GB
	const itemCount = totalDataSize / itemSize

	// Use a small max block size to ensure many blocks are created and merged.
	const maxBlockSizeBytes = 64 * 1024 * 1024 // 64MB

	// Pre-generate items to avoid measuring allocation time
	items := make([]ByteKeySortable, itemCount)
	value := make([]byte, valueSize)
	for i := int64(0); i < itemCount; i++ {
		key := []byte(fmt.Sprintf("key-%9d"+fmt.Sprintf("%d", keySize-4)+"d", i))
		items[i] = ByteKeySortable{Key: key, Value: value}
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		factory := buffers.NewCompressedBufferFactory(buffers.NewInMemoryBufferFactory())
		sorter := NewBigSorter[ByteKeySortable](factory, WithMaxBufferSizeBytes(maxBlockSizeBytes))

		// Add items
		for i := int64(0); i < itemCount; i++ {
			if err := sorter.Add(items[i]); err != nil {
				b.Fatalf("failed to add item: %v", err)
			}
		}

		if err := sorter.Flush(); err != nil {
			b.Fatalf("failed to flush: %v", err)
		}

		factory.Release()
	}
}

func BenchmarkBigSorter_Sort(b *testing.B) {
	b.ReportAllocs()

	// Use a fixed key and value size for consistent item size
	const keySize = 32
	const valueSize = 100
	// From ByteKeySortable's Serialize method: 2 bytes for key length, 2 for value length
	const itemSize = int64(4 + keySize + valueSize)
	const totalDataSize = 5 * 1024 * 1024 * 1024 // 5 GB
	const itemCount = totalDataSize / itemSize

	// Use a small max block size to ensure many blocks are created and merged.
	const maxBlockSizeBytes = 64 * 1024 * 1024 // 64MB

	// Pre-populate the sorter (this is not timed)
	factory := buffers.NewCompressedBufferFactory(buffers.NewInMemoryBufferFactory())
	defer factory.Release()
	sorter := NewBigSorter[ByteKeySortable](factory, WithMaxBufferSizeBytes(maxBlockSizeBytes))

	for i := int64(0); i < itemCount; i++ {
		key := []byte(fmt.Sprintf("key-%9d"+fmt.Sprintf("%d", keySize-4)+"d", i))
		value := make([]byte, valueSize)
		item := ByteKeySortable{Key: key, Value: value}
		if err := sorter.Add(item); err != nil {
			b.Fatalf("failed to add item: %v", err)
		}
	}

	if err := sorter.Flush(); err != nil {
		b.Fatalf("failed to flush: %v", err)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		// Time only the sorting/iteration phase
		iterator := sorter.SortIter()
		iteratedCount := int64(0)
		for range iterator.Iter() {
			iteratedCount++
		}

		if err := iterator.Err(); err != nil {
			b.Fatalf("iteration failed: %v", err)
		}

		if iteratedCount != itemCount {
			b.Fatalf("expected %d items, but got %d", itemCount, iteratedCount)
		}
	}
}

func BenchmarkBigSorter_Disk50GB(b *testing.B) {
	b.ReportAllocs()

	// Use a fixed key and value size for consistent item size
	const keySize = 32
	const valueSize = 100
	// From ByteKeySortable's Serialize method: 2 bytes for key length, 2 for value length
	const itemSize = int64(4 + keySize + valueSize)
	const totalDataSize = 20 * 1024 * 1024 * 1024 // 20 GB
	const itemCount = totalDataSize / itemSize

	fmt.Printf("Benchmark will sort approximately %d items (~%.2f GB)", itemCount, float64(itemCount*itemSize)/(1024*1024*1024))

	const maxBlockSizeBytes = 256 * 1024 * 1024 // 256MB

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		// Create a temporary directory for this iteration
		tempDir := b.TempDir()
		fmt.Printf("Using temporary directory: %s", tempDir)
		baseFactory, err := buffers.NewDirBufferFactory(filepath.Join(tempDir, "buffers"))
		if err != nil {
			b.Fatalf("failed to create dir buffer factory: %v", err)
		}
		factory := buffers.NewCompressedBufferFactory(baseFactory)
		defer factory.Release()

		sorter := NewBigSorter[ByteKeySortable](factory, WithMaxBufferSizeBytes(maxBlockSizeBytes))

		// Add items - generate on-the-fly to avoid memory pressure
		b.Log("Adding items...")
		var keyBuf [8]byte
		for i := int64(0); i < itemCount; i++ {
			binary.BigEndian.PutUint64(keyBuf[:], uint64(i))
			value := make([]byte, valueSize)
			item := ByteKeySortable{Key: keyBuf[:], Value: value}
			if err := sorter.Add(item); err != nil {
				b.Fatalf("failed to add item: %v", err)
			}
			// Log progress every 10 million items
			if (i+1)%10_000_000 == 0 {
				b.Logf("Added %d/%d items (%.1f%%)", i+1, itemCount, float64(i+1)/float64(itemCount)*100)
			}
		}

		if err := sorter.Flush(); err != nil {
			b.Fatalf("failed to flush: %v", err)
		}

		// Sort and verify
		b.Log("Sorting...")
		iterator := sorter.SortIter()
		var prevKey []byte
		iteratedCount := int64(0)
		for item := range iterator.Iter() {
			if prevKey != nil {
				if bytes.Compare(prevKey, item.Key) > 0 {
					b.Errorf("output is not sorted")
					break
				}
			}
			prevKey = slices.Clone(item.Key)
			iteratedCount++

			// Log progress every 10 million items
			if iteratedCount%10_000_000 == 0 {
				b.Logf("Sorted %d/%d items (%.1f%%)", iteratedCount, itemCount, float64(iteratedCount)/float64(itemCount)*100)
			}
		}

		if err := iterator.Err(); err != nil {
			b.Fatalf("iteration failed: %v", err)
		}

		if iteratedCount != itemCount {
			b.Fatalf("expected %d items, but got %d", itemCount, iteratedCount)
		}

		b.Logf("Completed sorting %d items", iteratedCount)
	}
}
