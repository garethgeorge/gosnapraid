package blockmap

import (
	"github.com/google/btree"
)

type RangeAllocation[T any] struct {
	Range
	Data T
}

func (r RangeAllocation[T]) Overlaps(other RangeAllocation[T]) bool {
	return r.Start <= other.End && other.Start <= r.End
}

type RangeAllocator[T any] struct {
	// The start and end of the range that this allocator can allocate from
	Start int64
	End   int64

	FreeSpace      int64
	AllocatedSpace int64

	// The list of allocated ranges, we will search outside of these.
	allocatedList *btree.BTreeG[RangeAllocation[T]]
	// The list of free ranges, used for finding a free spot.
	freeList *btree.BTreeG[Range]
}

func NewRangeAllocator[T any](start int64, end int64) *RangeAllocator[T] {
	freeList := btree.NewG(32, func(a, b Range) bool {
		return a.Start < b.Start
	})
	var freeSpace int64
	if end >= start {
		freeSpace = end - start + 1
		freeList.ReplaceOrInsert(Range{Start: start, End: end})
	}

	return &RangeAllocator[T]{
		Start: start,
		End:   end,
		allocatedList: btree.NewG(32, func(a, b RangeAllocation[T]) bool {
			return a.Start < b.Start
		}),
		freeList:  freeList,
		FreeSpace: freeSpace,
	}
}

// recomputeFreeList rebuilds the free list from the allocated list.
// This is expensive and should only be used for testing or recovery.
func (r *RangeAllocator[T]) recomputeFreeList() {
	r.freeList.Clear(true)
	prevBlockEnd := r.Start - 1
	r.allocatedList.Ascend(func(item RangeAllocation[T]) bool {
		freeStart := prevBlockEnd + 1
		freeEnd := item.Start - 1
		if freeEnd >= freeStart {
			r.freeList.ReplaceOrInsert(Range{Start: freeStart, End: freeEnd})
		}
		prevBlockEnd = item.End
		return true
	})

	// Add the final free block after the last allocation
	freeStart := prevBlockEnd + 1
	freeEnd := r.End
	if freeEnd >= freeStart {
		r.freeList.ReplaceOrInsert(Range{Start: freeStart, End: freeEnd})
	}
}

func (r *RangeAllocator[T]) markAllocated(start, end int64) {
	var toUpdate Range
	var found bool

	// Find the free range that contains the allocated block.
	// The free range must have a start <= our allocation start.
	r.freeList.DescendLessOrEqual(Range{Start: start}, func(item Range) bool {
		if item.End >= end {
			toUpdate = item
			found = true
		}
		return false
	})

	if !found {
		// This indicates a bug in the allocator logic, as we should always find
		// a containing free range for an allocation.
		panic("could not find a free range for allocation")
	}

	r.freeList.Delete(toUpdate)

	// Add back the remaining parts of the free range
	// Part before the allocation
	if toUpdate.Start < start {
		r.freeList.ReplaceOrInsert(Range{Start: toUpdate.Start, End: start - 1})
	}
	// Part after the allocation
	if toUpdate.End > end {
		r.freeList.ReplaceOrInsert(Range{Start: end + 1, End: toUpdate.End})
	}
}

func (r *RangeAllocator[T]) Allocate(size int64, data T) (start int64, end int64) {
	start, end = r.findFreeRangeInternal(size)
	if end < start {
		return 0, -1
	}
	got := end - start + 1
	r.markAllocated(start, end)
	r.allocatedList.ReplaceOrInsert(RangeAllocation[T]{Range: Range{Start: start, End: end}, Data: data})
	r.FreeSpace -= got
	r.AllocatedSpace += got
	return
}

func (r *RangeAllocator[T]) SetAllocated(start, end int64, data T) bool {
	if start < r.Start || end > r.End || start > end {
		// Out of range
		return false
	}

	// First verify that it doesn't overlap with any existing allocations
	var foundOverlap bool
	r.allocatedList.DescendLessOrEqual(RangeAllocation[T]{
		Range: Range{Start: end},
	}, func(item RangeAllocation[T]) bool {
		if item.End < start {
			return false
		}
		if item.Range.Overlaps(Range{Start: start, End: end}) {
			foundOverlap = true
			return false
		}
		return true
	})
	if foundOverlap {
		return false
	}
	r.markAllocated(start, end)
	r.allocatedList.ReplaceOrInsert(RangeAllocation[T]{
		Range: Range{Start: start, End: end},
		Data:  data,
	})
	r.FreeSpace -= end - start + 1
	r.AllocatedSpace += end - start + 1
	return true
}

func (r *RangeAllocator[T]) Free(start int64, end int64) bool {
	// Find the allocation to be freed. The btree's less function only uses
	// Start, so Get will find an item with the same start. We must verify
	// the End also matches.
	itemToFind := RangeAllocation[T]{Range: Range{Start: start}}
	foundItem, found := r.allocatedList.Get(itemToFind)
	if !found || foundItem.End != end {
		return false
	}

	if _, deleted := r.allocatedList.Delete(foundItem); !deleted {
		return false
	}

	got := end - start + 1
	r.FreeSpace += got
	r.AllocatedSpace -= got

	// Add the freed range to the free list and merge with adjacent free ranges.
	newlyFreed := Range{Start: start, End: end}

	// Check for merge ranges
	var prev Range = EmptyRange
	var next Range = EmptyRange
	r.freeList.DescendLessOrEqual(Range{Start: end + 1}, func(it Range) bool {
		// Chcek for merge with range after
		if it.Start == newlyFreed.End+1 {
			newlyFreed.End = it.End
			next = it
			return true
		}
		// Check for merge with range before
		if it.End == start-1 {
			prev = it
			newlyFreed.Start = it.Start
			return true
		}
		return false
	})
	if prev != EmptyRange {
		r.freeList.Delete(prev)
	}
	if next != EmptyRange {
		r.freeList.Delete(next)
	}

	r.freeList.ReplaceOrInsert(newlyFreed)
	return true
}

func (r *RangeAllocator[T]) findFreeRangeInternal(size int64) (start int64, end int64) {
	var bestFit Range
	var foundBestFit bool

	var foundRange Range
	var found bool
	r.freeList.Ascend(func(item Range) bool {
		currentSize := item.End - item.Start + 1
		if currentSize >= size {
			foundRange = item
			found = true
			return false // stop iteration
		}
		if !foundBestFit || (currentSize > (bestFit.End - bestFit.Start + 1)) {
			bestFit = item
			foundBestFit = true
		}
		return true
	})

	if found {
		return foundRange.Start, foundRange.Start + size - 1
	}

	if foundBestFit {
		return bestFit.Start, bestFit.End
	}

	return 0, -1 // No space
}

func (r *RangeAllocator[T]) AllocationIter() func(yield func(alloc RangeAllocation[T]) bool) {
	return func(yield func(alloc RangeAllocation[T]) bool) {
		r.allocatedList.Ascend(func(item RangeAllocation[T]) bool {
			return yield(item)
		})
	}
}
