package stripealloc

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/google/btree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStripeAllocator_New(t *testing.T) {
	s := NewStripeAllocator(1000)
	require.NotNil(t, s)
	assert.Equal(t, uint64(1000), s.TotalCapacity)
	assert.Equal(t, uint64(1000), s.AvailableCapacity)
	assert.Equal(t, 1, s.FreeList.Len())
	assert.Equal(t, 1, s.FreeListBySize.Len())

	freeRange, _ := s.FreeList.Min()
	assert.Equal(t, Range{Start: 0, End: 1000}, freeRange)
}

func TestStripeAllocator_SimpleAllocFree(t *testing.T) {
	s := NewStripeAllocator(1000)

	// Allocate
	r, err := s.Allocate(100)
	require.NoError(t, err)
	assert.Equal(t, Range{Start: 0, End: 100}, r)
	assert.Equal(t, uint64(900), s.AvailableCapacity)
	assert.True(t, s.IsRangeFree(Range{Start: 100, End: 1000}))
	assert.False(t, s.IsRangeFree(Range{Start: 0, End: 100}))
	assert.False(t, s.IsRangeFree(Range{Start: 50, End: 150}))

	// Free
	err = s.Free(r)
	require.NoError(t, err)
	assert.Equal(t, uint64(1000), s.AvailableCapacity)
	assert.True(t, s.IsRangeFree(Range{Start: 0, End: 1000}))
	assert.Equal(t, 1, s.FreeList.Len())
}

func TestStripeAllocator_MergeFree(t *testing.T) {
	s := NewStripeAllocator(1000)
	require.NoError(t, s.MarkAllocated(Range{Start: 100, End: 200}))
	require.NoError(t, s.MarkAllocated(Range{Start: 300, End: 400}))
	// FreeList is now: [0, 100), [200, 300), [400, 1000)

	// Free an allocation that is between two free blocks, causing a three-way merge.
	require.NoError(t, s.Free(Range{Start: 100, End: 200}))
	// Expected FreeList: [0, 300), [400, 1000)
	assert.Equal(t, 2, s.FreeList.Len())
	min, _ := s.FreeList.Min()
	assert.Equal(t, Range{Start: 0, End: 300}, min)

	// Free the second allocation, causing a final merge.
	require.NoError(t, s.Free(Range{Start: 300, End: 400}))
	// Expected FreeList: [0, 1000)
	assert.Equal(t, 1, s.FreeList.Len())
	min, _ = s.FreeList.Min()
	assert.Equal(t, Range{Start: 0, End: 1000}, min)
}

func TestStripeAllocator_Resize(t *testing.T) {
	s := NewStripeAllocator(1000)
	require.NoError(t, s.MarkAllocated(Range{Start: 100, End: 200}))

	// Grow
	err := s.ResizeTo(2000)
	require.NoError(t, err)
	assert.Equal(t, uint64(2000), s.TotalCapacity)
	assert.Equal(t, uint64(1900), s.AvailableCapacity)
	lastFree, _ := s.FreeList.Max()
	assert.Equal(t, uint64(2000), lastFree.End)

	// Shrink
	err = s.ResizeTo(500)
	require.NoError(t, err)
	assert.Equal(t, uint64(500), s.TotalCapacity)
	assert.Equal(t, uint64(400), s.AvailableCapacity)
	lastFree, _ = s.FreeList.Max()
	assert.Equal(t, uint64(500), lastFree.End)

	// Shrink too much
	err = s.ResizeTo(150)
	require.Error(t, err)
}

func TestStripeAllocator_ShrinkToFit(t *testing.T) {
	s := NewStripeAllocator(1000)
	require.NoError(t, s.MarkAllocated(Range{Start: 100, End: 200}))
	require.NoError(t, s.MarkAllocated(Range{Start: 400, End: 500}))

	err := s.ShrinkToFit()
	require.NoError(t, err)
	assert.Equal(t, uint64(500), s.TotalCapacity)
	assert.Equal(t, uint64(300), s.AvailableCapacity)
}

func TestStripeAllocator_MultiAllocate(t *testing.T) {
	s := NewStripeAllocator(1000)
	require.NoError(t, s.MarkAllocated(Range{Start: 100, End: 200})) // 100 used
	require.NoError(t, s.MarkAllocated(Range{Start: 500, End: 800})) // 300 used
	// Available: [0, 100), [200, 500), [800, 1000)
	// Sizes: 100, 300, 200

	// Allocate 450. Should take [200, 500) and [800, 950)
	ranges, err := s.MultiAllocate(450)
	require.NoError(t, err)
	require.Len(t, ranges, 2)

	// Sort for consistent checking
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].Start < ranges[j].Start })

	assert.Equal(t, Range{Start: 200, End: 500}, ranges[0])
	assert.Equal(t, Range{Start: 800, End: 950}, ranges[1])
	assert.Equal(t, uint64(150), s.AvailableCapacity)
}

func TestStripeAllocator_IterAllocs(t *testing.T) {
	testCases := []struct {
		name           string
		capacity       uint64
		allocations    []Range
		expectedAllocs []Range
	}{
		{
			name:           "fully free",
			capacity:       1000,
			allocations:    []Range{},
			expectedAllocs: []Range{},
		},
		{
			name:     "fully allocated",
			capacity: 1000,
			allocations: []Range{
				{Start: 0, End: 1000},
			},
			expectedAllocs: []Range{
				{Start: 0, End: 1000},
			},
		},
		{
			name:     "one allocation at start",
			capacity: 1000,
			allocations: []Range{
				{Start: 0, End: 100},
			},
			expectedAllocs: []Range{
				{Start: 0, End: 100},
			},
		},
		{
			name:     "one allocation in middle",
			capacity: 1000,
			allocations: []Range{
				{Start: 100, End: 200},
			},
			expectedAllocs: []Range{
				{Start: 100, End: 200},
			},
		},
		{
			name:     "one allocation at end",
			capacity: 1000,
			allocations: []Range{
				{Start: 900, End: 1000},
			},
			expectedAllocs: []Range{
				{Start: 900, End: 1000},
			},
		},
		{
			name:     "multiple allocations",
			capacity: 1000,
			allocations: []Range{
				{Start: 100, End: 200},
				{Start: 400, End: 500},
			},
			expectedAllocs: []Range{
				{Start: 100, End: 200},
				{Start: 400, End: 500},
			},
		},
		{
			name:     "allocations touching",
			capacity: 1000,
			allocations: []Range{
				{Start: 100, End: 200},
				{Start: 200, End: 300},
			},
			expectedAllocs: []Range{
				{Start: 100, End: 300},
			},
		},
		{
			name:     "allocations at boundaries",
			capacity: 1000,
			allocations: []Range{
				{Start: 0, End: 100},
				{Start: 900, End: 1000},
			},
			expectedAllocs: []Range{
				{Start: 0, End: 100},
				{Start: 900, End: 1000},
			},
		},
		{
			name:           "zero capacity",
			capacity:       0,
			allocations:    []Range{},
			expectedAllocs: []Range{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewStripeAllocator(tc.capacity)
			if len(tc.allocations) > 0 {
				err := s.MarkAllocated(tc.allocations...)
				require.NoError(t, err)
			}

			gotAllocs := []Range{}
			for r := range s.IterAllocs() {
				gotAllocs = append(gotAllocs, r)
			}

			assert.Equal(t, tc.expectedAllocs, gotAllocs)
		})
	}
}

// --- Fuzz Test ---

type simpleAllocatorModel struct {
	allocated []bool
}

func newSimpleAllocatorModel(size uint64) *simpleAllocatorModel {
	return &simpleAllocatorModel{allocated: make([]bool, size)}
}

func (s *simpleAllocatorModel) free(r Range) {
	for i := r.Start; i < r.End; i++ {
		s.allocated[i] = false
	}
}

func (s *simpleAllocatorModel) check(t *testing.T, sa *StripeAllocator) {
	t.Helper()
	// Check capacities
	var available uint64
	for _, b := range s.allocated {
		if !b {
			available++
		}
	}
	assert.Equal(t, uint64(len(s.allocated)), sa.TotalCapacity, "TotalCapacity mismatch")
	assert.Equal(t, available, sa.AvailableCapacity, "AvailableCapacity mismatch")

	// Check free lists consistency
	expectedFreeList := btree.NewG[Range](32, func(a, b Range) bool { return a.Start < b.Start })
	inFreeBlock := false
	var currentFreeStart uint64
	for i, allocated := range s.allocated {
		if !allocated && !inFreeBlock {
			inFreeBlock = true
			currentFreeStart = uint64(i)
		} else if allocated && inFreeBlock {
			inFreeBlock = false
			expectedFreeList.ReplaceOrInsert(Range{Start: currentFreeStart, End: uint64(i)})
		}
	}
	if inFreeBlock {
		expectedFreeList.ReplaceOrInsert(Range{Start: currentFreeStart, End: uint64(len(s.allocated))})
	}

	require.Equal(t, expectedFreeList.Len(), sa.FreeList.Len(), "FreeList length mismatch")

	iter1 := sa.FreeList.Clone()
	iter2 := expectedFreeList.Clone()
	for {
		r1, ok1 := iter1.Min()
		r2, ok2 := iter2.Min()
		if !ok1 || !ok2 {
			require.Equal(t, ok1, ok2, "FreeList item count mismatch")
			break
		}
		assert.Equal(t, r2, r1, "FreeList item mismatch")
		iter1.DeleteMin()
		iter2.DeleteMin()
	}
}

func FuzzStripeAllocator(f *testing.F) {
	f.Add(uint64(1000), 100, int64(1))
	f.Add(uint64(10000), 500, time.Now().UnixNano())

	f.Fuzz(func(t *testing.T, size uint64, numOps int, seed int64) {
		if size < 100 || size > 1000000 {
			t.Skip()
		}
		if numOps > 1000 {
			numOps = 1000
		}

		rng := rand.New(rand.NewSource(seed))

		allocator := NewStripeAllocator(size)
		model := newSimpleAllocatorModel(size)
		var allocatedRanges []Range

		for i := 0; i < numOps; i++ {
			op := rng.Intn(2) // 0: Allocate, 1: Free

			switch op {
			case 0: // Allocate
				allocSize := uint64(rng.Intn(int(size/10)) + 1)
				r, err := allocator.Allocate(allocSize)
				if err == nil {
					// Verify allocation is valid in model
					for i := r.Start; i < r.End; i++ {
						require.False(t, model.allocated[i], "allocator allocated an already allocated range")
						model.allocated[i] = true
					}
					allocatedRanges = append(allocatedRanges, r)
				} else {
					// Verify that no block of this size exists in the model
					var freeCount uint64
					foundBlock := false
					for _, allocated := range model.allocated {
						if !allocated {
							freeCount++
							if freeCount >= allocSize {
								foundBlock = true
								break
							}
						} else {
							freeCount = 0
						}
					}
					assert.False(t, foundBlock, fmt.Sprintf("Allocate failed but a block of size %d exists", allocSize))
				}

			case 1: // Free
				if len(allocatedRanges) > 0 {
					idx := rng.Intn(len(allocatedRanges))
					r := allocatedRanges[idx]

					err := allocator.Free(r)
					require.NoError(t, err)

					model.free(r)
					allocatedRanges = append(allocatedRanges[:idx], allocatedRanges[idx+1:]...)
				}
			}

			// Check for consistency after each operation
			model.check(t, allocator)
		}

		// At the end of the run, test resizing or shrinking
		model.check(t, allocator) // Final check before resize/shrink

		var highestAllocated uint64
		for i := len(model.allocated) - 1; i >= 0; i-- {
			if model.allocated[i] {
				highestAllocated = uint64(i + 1)
				break
			}
		}

		// 0: ResizeTo (fail), 1: ResizeTo (succeed), 2: ShrinkToFit
		resizeOp := rng.Intn(3)

		switch resizeOp {
		case 0: // ResizeTo (fail)
			if highestAllocated > 0 {
				newSize := uint64(rng.Int63n(int64(highestAllocated)))
				err := allocator.ResizeTo(newSize)
				require.Error(t, err, "ResizeTo below highest allocated should fail")
				// Model is unchanged, check consistency again
				model.check(t, allocator)
			}
		case 1: // ResizeTo (succeed)
			// New size can be smaller or larger than current capacity, but must be >= highestAllocated
			newSize := highestAllocated + uint64(rng.Int63n(int64(size)+1))
			err := allocator.ResizeTo(newSize)
			require.NoError(t, err)

			// Update model
			currentCap := uint64(len(model.allocated))
			if newSize < currentCap {
				model.allocated = model.allocated[:newSize]
			} else if newSize > currentCap {
				newAllocated := make([]bool, newSize)
				copy(newAllocated, model.allocated)
				model.allocated = newAllocated
			}
			model.check(t, allocator)

		case 2: // ShrinkToFit
			err := allocator.ShrinkToFit()
			require.NoError(t, err)

			// Update model
			model.allocated = model.allocated[:highestAllocated]

			assert.Equal(t, highestAllocated, allocator.TotalCapacity, "ShrinkToFit should resize to highest allocated block")
			model.check(t, allocator)
		}
	})
}

func BenchmarkAllocate(b *testing.B) {
	s := NewStripeAllocator(uint64(100 * b.N))
	b.ReportAllocs()
	b.SetBytes(100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Allocate(100); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFree(b *testing.B) {
	s := NewStripeAllocator(uint64(100 * b.N))
	ranges := make([]Range, b.N)
	for i := 0; i < b.N; i++ {
		r, err := s.Allocate(100)
		if err != nil {
			b.Fatal(err)
		}
		ranges[i] = r
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := s.Free(ranges[i]); err != nil {
			b.Fatal(err)
		}
	}
}
