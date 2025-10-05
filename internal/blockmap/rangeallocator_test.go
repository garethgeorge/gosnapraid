package blockmap

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRangeAllocator_String(t *testing.T) {
	allocator := NewRangeAllocator[string](0, 100)

	// Test basic allocation
	start, end := allocator.Allocate(10, "first")
	assert.Equal(t, int64(0), start)
	assert.Equal(t, int64(9), end)

	start, end = allocator.Allocate(10, "second")
	assert.Equal(t, int64(10), start)
	assert.Equal(t, int64(19), end)

	// Test allocation when there is a gap
	allocator.allocatedList.ReplaceOrInsert(RangeAllocation[string]{Start: 50, End: 59, Data: "gap"}) // size 10
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
	assert.Equal(t, int64(0), start)
	assert.Equal(t, int64(0), end)

	// Verify all allocations
	var allocations []RangeAllocation[string]
	for alloc := range allocator.AllocationIter() {
		allocations = append(allocations, alloc)
	}
	expectedAllocations := []RangeAllocation[string]{
		{Start: 0, End: 9, Data: "first"},
		{Start: 10, End: 19, Data: "second"},
		{Start: 20, End: 29, Data: "third"},
		{Start: 30, End: 49, Data: "fourth"},
		{Start: 50, End: 59, Data: "gap"},
		{Start: 60, End: 69, Data: "fifth"},
		{Start: 70, End: 79, Data: "sixth"},
		{Start: 80, End: 100, Data: "seventh"},
	}
	assert.Equal(t, len(expectedAllocations), len(allocations))
	for i := range expectedAllocations {
		assert.Equal(t, expectedAllocations[i], allocations[i])
	}
}

func TestRangeAllocator_Free(t *testing.T) {
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
		{Start: 0, End: 9, Data: "first"},
		{Start: 10, End: 19, Data: "second"},
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
		{Start: 0, End: 9, Data: "third"},
		{Start: 10, End: 19, Data: "second"},
	}
	assert.Equal(t, expectedAllocations2, allocations2)
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
