package blockmap

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRangeAllocator_String(t *testing.T) {
	t.Parallel()
	allocator := NewRangeAllocator[string](0, 100)

	// Test basic allocation
	start, end := allocator.Allocate(10, "first")
	assert.Equal(t, int64(0), start)
	assert.Equal(t, int64(9), end)

	start, end = allocator.Allocate(10, "second")
	assert.Equal(t, int64(10), start)
	assert.Equal(t, int64(19), end)

	// Test allocation when there is a gap
	allocator.allocatedList.ReplaceOrInsert(RangeAllocation[string]{Range: Range{Start: 50, End: 59}, Data: "gap"}) // size 10
	allocator.recomputeFreeList()
	start, end = allocator.Allocate(10, "third")
	assert.Equal(t, int64(20), start)
	assert.Equal(t, int64(29), end)

	// Test allocation in the gap
	start, end = allocator.Allocate(20, "fourth")
	assert.Equal(t, int64(30), start)
	assert.Equal(t, int64(49), end)

	// Test allocation that partially fills the gap
	start, end = allocator.Allocate(10, "fifth")
	assert.Equal(t, int64(60), start)
	assert.Equal(t, int64(69), end)

	// Test allocation at the end
	start, end = allocator.Allocate(10, "sixth")
	assert.Equal(t, int64(70), start)
	assert.Equal(t, int64(79), end)

	// Test allocation that is too large for any gap, should return largest gap
	// largest gap is from 80 to 100, which is size 21
	start, end = allocator.Allocate(100, "seventh")
	assert.Equal(t, int64(80), start)
	assert.Equal(t, int64(100), end)

	// Test allocation when there is no more space
	start, end = allocator.Allocate(1, "eighth")
	assert.True(t, end < start)

	// Verify all allocations
	var allocations []RangeAllocation[string]
	for alloc := range allocator.AllocationIter() {
		allocations = append(allocations, alloc)
	}
	expectedAllocations := []RangeAllocation[string]{
		{Range: Range{Start: 0, End: 9}, Data: "first"},
		{Range: Range{Start: 10, End: 19}, Data: "second"},
		{Range: Range{Start: 20, End: 29}, Data: "third"},
		{Range: Range{Start: 30, End: 49}, Data: "fourth"},
		{Range: Range{Start: 50, End: 59}, Data: "gap"},
		{Range: Range{Start: 60, End: 69}, Data: "fifth"},
		{Range: Range{Start: 70, End: 79}, Data: "sixth"},
		{Range: Range{Start: 80, End: 100}, Data: "seventh"},
	}
	assert.Equal(t, len(expectedAllocations), len(allocations))
	for i := range expectedAllocations {
		assert.Equal(t, expectedAllocations[i], allocations[i])
	}
}

func TestRangeAllocator_Free(t *testing.T) {
	t.Parallel()
	allocator := NewRangeAllocator[string](0, 100)
	start1st, end1st := allocator.Allocate(10, "first")
	assert.Equal(t, int64(0), start1st)
	assert.Equal(t, int64(9), end1st)
	start2nd, end2nd := allocator.Allocate(10, "second")
	assert.Equal(t, int64(10), start2nd)
	assert.Equal(t, int64(19), end2nd)

	var allocations []RangeAllocation[string]
	for alloc := range allocator.AllocationIter() {
		allocations = append(allocations, alloc)
	}
	var expectedAllocations []RangeAllocation[string] = []RangeAllocation[string]{
		{Range: Range{Start: 0, End: 9}, Data: "first"},
		{Range: Range{Start: 10, End: 19}, Data: "second"},
	}
	assert.Equal(t, expectedAllocations, allocations)
	allocator.Free(start1st, end1st)
	start3rd, end3rd := allocator.Allocate(10, "third")
	assert.Equal(t, int64(0), start3rd)
	assert.Equal(t, int64(9), end3rd)

	var allocations2 []RangeAllocation[string]
	for alloc := range allocator.AllocationIter() {
		allocations2 = append(allocations2, alloc)
	}
	var expectedAllocations2 []RangeAllocation[string] = []RangeAllocation[string]{
		{Range: Range{Start: 0, End: 9}, Data: "third"},
		{Range: Range{Start: 10, End: 19}, Data: "second"},
	}
	assert.Equal(t, expectedAllocations2, allocations2)
}

func TestRangeAllocator_SetAllocated(t *testing.T) {
	t.Parallel()
	allocator := NewRangeAllocator[string](0, 100)
	assert.True(t, allocator.SetAllocated(0, 9, "first"))
	assert.False(t, allocator.SetAllocated(0, 10, "first"))
	assert.True(t, allocator.SetAllocated(10, 19, "second"))
	assert.True(t, allocator.SetAllocated(20, 29, "third"))
	assert.True(t, allocator.SetAllocated(30, 39, "fourth"))
	assert.True(t, allocator.SetAllocated(40, 49, "fifth"))
	assert.True(t, allocator.SetAllocated(50, 59, "sixth"))
	assert.True(t, allocator.SetAllocated(60, 69, "seventh"))
	assert.True(t, allocator.SetAllocated(70, 79, "eighth"))
	assert.True(t, allocator.SetAllocated(80, 89, "ninth"))
	assert.True(t, allocator.SetAllocated(90, 99, "tenth"))
	assert.False(t, allocator.SetAllocated(100, 110, "eleventh"))
	assert.False(t, allocator.SetAllocated(110, 120, "twelfth"))
	assert.False(t, allocator.SetAllocated(-10, -1, "negative"))
	assert.False(t, allocator.SetAllocated(50, 60, "overlap"))
}

func BenchmarkRangeAllocator_Allocate(b *testing.B) {
	b.ReportAllocs()
	allocator := NewRangeAllocator[string](0, 10000000)

	// allocs := make([]RangeAllocation[string], 0)
	for i := 0; i < b.N; i++ {
		randomSize := rand.Int63n(100)
		allocator.Allocate(randomSize, fmt.Sprintf("%d", i))
		// allocs = append(allocs, RangeAllocation[string]{Start: start, End: end, Data: fmt.Sprintf("%d", i)})
	}
}

func BenchmarkRangeAllocator_Free(b *testing.B) {
	b.ReportAllocs()
	allocator := NewRangeAllocator[string](0, 10000000)
	for i := 0; i < b.N; i++ {
		allocator.Allocate(10, fmt.Sprintf("%d", i))
	}
	var allocations []RangeAllocation[string]
	for alloc := range allocator.AllocationIter() {
		allocations = append(allocations, alloc)
	}
	for _, alloc := range allocations {
		allocator.Free(alloc.Start, alloc.End)
	}
}

func BenchmarkRangeAllocator_Fragmented(b *testing.B) {
	allocator := NewRangeAllocator[string](0, 10000000)
	var allocs []RangeAllocation[string]
	for i := 0; i < b.N; i++ {
		allocator.Allocate(10, fmt.Sprintf("%d", i))
		allocs = append(allocs, RangeAllocation[string]{Range: Range{Start: int64(i), End: int64(i) + 10}, Data: fmt.Sprintf("%d", i)})

		// random chance to delete an old allocation
		if rand.Intn(100) < 20 {
			idx := rand.Intn(len(allocs))
			allocator.Free(allocs[idx].Start, allocs[idx].End)
			allocs[idx] = allocs[len(allocs)-1]
			allocs = allocs[:len(allocs)-1]
		}
	}
}

func FuzzRangeAllocator(f *testing.F) {
	seedBase := int64(time.Now().UnixNano())

	f.Add(int64(100), int64(100), seedBase+0)
	f.Add(int64(1000), int64(1000), seedBase+1)
	f.Add(int64(10000), int64(10000), seedBase+2)

	f.Fuzz(func(t *testing.T, rangeSize, operationCount, seed int64) {
		if rangeSize <= 0 || operationCount <= 0 || operationCount > 10000 {
			t.Skip("Invalid range size or operation count")
		}

		rng := rand.New(rand.NewSource(int64(seed)))
		allocator := NewRangeAllocator[string](0, rangeSize)

		// Track what we expect to be allocated
		expectedAllocs := make(map[string]RangeAllocation[string])

		// Perform random operations
		for i := 0; i < int(operationCount); i++ {
			op := rng.Intn(3) // 0=allocate, 1=free, 2=setAllocated

			switch op {
			case 0: // Allocate
				size := rng.Int63n(rangeSize/100+1) + 1
				data := fmt.Sprintf("alloc_%d", i)
				start, end := allocator.Allocate(size, data)
				if end >= start { // successful allocation
					expectedAllocs[data] = RangeAllocation[string]{Range: Range{Start: start, End: end}, Data: data}
				}
			case 1: // Free random allocation
				if len(expectedAllocs) > 0 {
					// Pick a random allocation to free
					keys := make([]string, 0, len(expectedAllocs))
					for k := range expectedAllocs {
						keys = append(keys, k)
					}
					key := keys[rng.Intn(len(keys))]
					alloc := expectedAllocs[key]
					allocator.Free(alloc.Start, alloc.End)
					delete(expectedAllocs, key)
				}
			case 2: // SetAllocated
				start := rng.Int63n(rangeSize + 1)
				size := rng.Int63n(rangeSize/10+1) + 1
				end := start + size - 1
				if end >= rangeSize {
					end = rangeSize - 1
				}
				data := fmt.Sprintf("set_%d", i)
				if allocator.SetAllocated(start, end, data) {
					expectedAllocs[data] = RangeAllocation[string]{Range: Range{Start: start, End: end}, Data: data}
				}
			}

			// Verify allocator state matches expected state
			actualAllocs := make(map[string]RangeAllocation[string])
			for alloc := range allocator.AllocationIter() {
				actualAllocs[alloc.Data] = alloc
			}

			if len(expectedAllocs) != len(actualAllocs) {
				t.Fatalf("Length mismatch: expected %d allocations, got %d", len(expectedAllocs), len(actualAllocs))
			}

			for key, expected := range expectedAllocs {
				actual, exists := actualAllocs[key]
				if !exists {
					t.Fatalf("Missing allocation: %v", expected)
				}
				if actual != expected {
					t.Fatalf("Allocation mismatch for key %s: expected %v, got %v", key, expected, actual)
				}
			}
		}
	})
}
