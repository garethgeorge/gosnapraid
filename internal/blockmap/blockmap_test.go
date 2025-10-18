package blockmap

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBlockMap_AllocateFree(t *testing.T) {
	t.Parallel()
	bm := NewBlockMap(0, 100)

	// Allocate for file1
	allocs1 := bm.Allocate("file1", 20)
	assert.NotNil(t, allocs1)
	assert.Len(t, allocs1, 1)
	assert.Equal(t, int64(0), allocs1[0].Range.Start)
	assert.Equal(t, int64(19), allocs1[0].Range.End)
	assert.Equal(t, int64(20), bm.allocator.AllocatedSpace)

	// Allocate for file2
	allocs2 := bm.Allocate("file2", 30)
	assert.NotNil(t, allocs2)
	assert.Len(t, allocs2, 1)
	assert.Equal(t, int64(20), allocs2[0].Range.Start)
	assert.Equal(t, int64(49), allocs2[0].Range.End)
	assert.Equal(t, int64(50), bm.allocator.AllocatedSpace)

	// Free file1
	bm.Free("file1")
	assert.Equal(t, int64(30), bm.allocator.AllocatedSpace)
	_, exists := bm.files.Get(FileAllocationSet{Name: "file1"})
	assert.False(t, exists)

	// Allocate for file3, should reuse freed space
	allocs3 := bm.Allocate("file3", 15)
	assert.NotNil(t, allocs3)
	assert.Len(t, allocs3, 1)
	assert.Equal(t, int64(0), allocs3[0].Range.Start)
	assert.Equal(t, int64(14), allocs3[0].Range.End)
	assert.Equal(t, int64(45), bm.allocator.AllocatedSpace)
}

func TestBlockMap_AllocateAtStart(t *testing.T) {
	t.Parallel()
	bm := NewBlockMap(0, 100)
	allocs := bm.Allocate("file1", 1)
	assert.NotNil(t, allocs)
	assert.Len(t, allocs, 1)
	assert.Equal(t, int64(0), allocs[0].Range.Start)
	assert.Equal(t, int64(0), allocs[0].Range.End)
}

func TestBlockMap_AllocateFragmented(t *testing.T) {
	t.Parallel()
	bm := NewBlockMap(0, 99) // size 100
	bm.Allocate("file1", 10) // 0-9
	bm.Allocate("file2", 10) // 10-19
	bm.Allocate("file3", 10) // 20-29
	bm.Free("file2")         // free 10-19

	// This allocation needs 20 bytes.
	// The allocator will find the first free block that is large enough.
	// The free blocks are [10, 19] (size 10) and [30, 99] (size 70).
	// The first block is too small. The second is large enough.
	// So it should allocate from the second block.
	allocs := bm.Allocate("file4", 20)
	assert.NotNil(t, allocs)
	assert.Len(t, allocs, 1)

	assert.Equal(t, int64(30), allocs[0].Range.Start)
	assert.Equal(t, int64(49), allocs[0].Range.End)
}

func FuzzBlockMap(f *testing.F) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	f.Add(int64(100), int64(10), int(rng.Int63()))
	f.Add(int64(1000), int64(1000), int(rng.Int63()))
	f.Add(int64(5000), int64(947), int(rng.Int63()))

	f.Fuzz(func(t *testing.T, totalSize int64, operationCount int64, seed int) {
		t.Parallel()
		if totalSize <= 0 || totalSize > 20000 {
			t.Skip("Invalid total size")
		}

		rng := rand.New(rand.NewSource(int64(seed)))
		bm := NewBlockMap(0, totalSize-1)

		// Model of the system
		expectedFiles := make(map[string]int64) // fname -> size

		numOps := 10000
		for i := 0; i < numOps; i++ {
			op := rng.Intn(2) // 0 = allocate, 1 = free

			switch op {
			case 0: // Allocate
				fname := fmt.Sprintf("file_%d", rng.Intn(20)) // limited number of files
				size := rng.Int63n(totalSize/operationCount*10) + 1
				if _, exists := expectedFiles[fname]; exists {
					// Don't re-allocate for a file that already exists in our simple model
					continue
				}

				allocs := bm.Allocate(fname, size)
				if allocs != nil {
					var allocatedSize int64
					for _, a := range allocs {
						allocatedSize += (a.Range.End - a.Range.Start + 1)
					}
					assert.True(t, allocatedSize == size)
					expectedFiles[fname] = allocatedSize
				} else {
					assert.Less(t, bm.allocator.FreeSpace, size)
				}
			case 1: // Free
				if len(expectedFiles) == 0 {
					continue
				}
				// Pick a random file to free
				fnames := make([]string, 0, len(expectedFiles))
				for fname := range expectedFiles {
					fnames = append(fnames, fname)
				}
				fnameToFree := fnames[rng.Intn(len(fnames))]

				bm.Free(fnameToFree)
				delete(expectedFiles, fnameToFree)
			}
		}

		// Verification
		// Check total allocated space
		var expectedTotalAllocated int64
		for _, size := range expectedFiles {
			expectedTotalAllocated += size
		}
		assert.Equal(t, expectedTotalAllocated, bm.allocator.AllocatedSpace)

		// Check file map length
		assert.Equal(t, len(expectedFiles), bm.files.Len())

		// Check individual files
		for fname := range expectedFiles {
			_, ok := bm.files.Get(FileAllocationSet{Name: fname})
			assert.True(t, ok, "file %s expected but not found in blockmap", fname)
		}
	})
}
