package blockmap

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlockMap_AllocateFree(t *testing.T) {
	bm := NewBlockMap(0, 100)

	// Allocate for file1
	allocs1 := bm.Allocate("file1", 20)
	assert.NotNil(t, allocs1)
	assert.Len(t, allocs1, 1)
	assert.Equal(t, int64(0), allocs1[0].Start)
	assert.Equal(t, int64(19), allocs1[0].End)
	assert.Equal(t, int64(20), bm.allocator.AllocatedSpace)

	// Allocate for file2
	allocs2 := bm.Allocate("file2", 30)
	assert.NotNil(t, allocs2)
	assert.Len(t, allocs2, 1)
	assert.Equal(t, int64(20), allocs2[0].Start)
	assert.Equal(t, int64(49), allocs2[0].End)
	assert.Equal(t, int64(50), bm.allocator.AllocatedSpace)

	// Free file1
	bm.Free("file1")
	assert.Equal(t, int64(30), bm.allocator.AllocatedSpace)
	_, exists := bm.files["file1"]
	assert.False(t, exists)

	// Allocate for file3, should reuse freed space
	allocs3 := bm.Allocate("file3", 15)
	assert.NotNil(t, allocs3)
	assert.Len(t, allocs3, 1)
	assert.Equal(t, int64(0), allocs3[0].Start)
	assert.Equal(t, int64(14), allocs3[0].End)
	assert.Equal(t, int64(45), bm.allocator.AllocatedSpace)
}

func TestBlockMap_AllocateAtStart(t *testing.T) {
	bm := NewBlockMap(0, 100)
	allocs := bm.Allocate("file1", 1)
	assert.NotNil(t, allocs)
	assert.Len(t, allocs, 1)
	assert.Equal(t, int64(0), allocs[0].Start)
	assert.Equal(t, int64(0), allocs[0].End)
}

func TestBlockMap_AllocateFragmented(t *testing.T) {
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

	assert.Equal(t, int64(30), allocs[0].Start)
	assert.Equal(t, int64(49), allocs[0].End)
}

func TestBlockMap_SerializeDeserialize(t *testing.T) {
	bm1 := NewBlockMap(0, 1000)
	bm1.Allocate("file1", 100)
	bm1.Allocate("file2", 200)
	bm1.Allocate("file3", 50)

	var buf bytes.Buffer
	err := bm1.Serialize(&buf)
	assert.NoError(t, err)

	bm2 := NewBlockMap(0, 1000)
	err = bm2.Deserialize(&buf)
	assert.NoError(t, err)

	// Compare allocators
	assert.Equal(t, bm1.allocator.AllocatedSpace, bm2.allocator.AllocatedSpace)
	assert.Equal(t, bm1.allocator.FreeSpace, bm2.allocator.FreeSpace)

	// Compare file maps
	assert.Equal(t, len(bm1.files), len(bm2.files))
	for fname, allocs1 := range bm1.files {
		allocs2, ok := bm2.files[fname]
		assert.True(t, ok)
		assert.Equal(t, allocs1, allocs2)
	}

	// Compare underlying allocations
	iter1 := bm1.allocator.AllocationIter()
	iter2 := bm2.allocator.AllocationIter()
	allocs1 := make([]RangeAllocation[string], 0)
	allocs2 := make([]RangeAllocation[string], 0)
	for a := range iter1 {
		allocs1 = append(allocs1, a)
	}
	for a := range iter2 {
		allocs2 = append(allocs2, a)
	}
	assert.Equal(t, allocs1, allocs2)
}

func FuzzBlockMap(f *testing.F) {
	f.Add(int64(1000), 12345)

	f.Fuzz(func(t *testing.T, totalSize int64, seed int) {
		if totalSize <= 0 || totalSize > 20000 {
			t.Skip("Invalid total size")
		}

		rng := rand.New(rand.NewSource(int64(seed)))
		bm := NewBlockMap(0, totalSize-1)

		// Model of the system
		expectedFiles := make(map[string]int64) // fname -> size

		numOps := 100
		for i := 0; i < numOps; i++ {
			op := rng.Intn(2) // 0 = allocate, 1 = free

			switch op {
			case 0: // Allocate
				fname := fmt.Sprintf("file_%d", rng.Intn(20)) // limited number of files
				size := rng.Int63n(totalSize/5) + 1
				if _, exists := expectedFiles[fname]; exists {
					// Don't re-allocate for a file that already exists in our simple model
					continue
				}

				allocs := bm.Allocate(fname, size)
				if allocs != nil {
					// successful allocation
					var allocatedSize int64
					for _, a := range allocs {
						allocatedSize += (a.End - a.Start + 1)
					}
					// The allocator might give us more than we asked for if it finds a larger free block
					assert.True(t, allocatedSize >= size)
					expectedFiles[fname] = allocatedSize
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
		assert.Equal(t, len(expectedFiles), len(bm.files))

		// Check individual files
		for fname := range expectedFiles {
			_, ok := bm.files[fname]
			assert.True(t, ok, "file %s expected but not found in blockmap", fname)
		}
	})
}
