package blockmap

import (
	"github.com/google/btree"
)

type RangeAllocation[T any] struct {
	Start int64
	End   int64
	Data  T
}

type freeRange struct {
	Start int64
	End   int64
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
	freeList *btree.BTreeG[freeRange]
}

func NewRangeAllocator[T any](start int64, end int64) *RangeAllocator[T] {
	freeList := btree.NewG(32, func(a, b freeRange) bool {
		return a.Start < b.Start
	})
	var freeSpace int64
	if end >= start {
		freeSpace = end - start + 1
		freeList.ReplaceOrInsert(freeRange{Start: start, End: end})
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
			r.freeList.ReplaceOrInsert(freeRange{Start: freeStart, End: freeEnd})
		}
		prevBlockEnd = item.End
		return true
	})

	// Add the final free block after the last allocation
	freeStart := prevBlockEnd + 1
	freeEnd := r.End
	if freeEnd >= freeStart {
		r.freeList.ReplaceOrInsert(freeRange{Start: freeStart, End: freeEnd})
	}
}

func (r *RangeAllocator[T]) markAllocated(start, end int64) {
	var toUpdate freeRange
	var found bool

	// Find the free range that contains the allocated block.
	// The free range must have a start <= our allocation start.
	r.freeList.DescendLessOrEqual(freeRange{Start: start}, func(item freeRange) bool {
		// Since free ranges are sorted by start and do not overlap, the first
		// one we find that ends after our allocation starts must be the one
		// that contains it.
		if item.End >= end {
			toUpdate = item
			found = true
		}
		return false // Only need to check the first candidate.
	})

	if !found {
		// This indicates a bug in the allocator logic, as we should always find
		// a containing free range for an allocation.
		panic("could not find a free range for allocation")
	}

	// Remove the old free range
	r.freeList.Delete(toUpdate)

	// Add back the remaining parts of the free range
	// Part before the allocation
	if toUpdate.Start < start {
		r.freeList.ReplaceOrInsert(freeRange{Start: toUpdate.Start, End: start - 1})
	}
	// Part after the allocation
	if toUpdate.End > end {
		r.freeList.ReplaceOrInsert(freeRange{Start: end + 1, End: toUpdate.End})
	}
}

func (r *RangeAllocator[T]) Allocate(size int64, data T) (start int64, end int64) {
	start, end = r.findFreeRangeInternal(size)
	if end < start {
		return 0, 0
	}

	got := end - start + 1
	r.markAllocated(start, end)
	r.allocatedList.ReplaceOrInsert(RangeAllocation[T]{Start: start, End: end, Data: data})
	r.FreeSpace -= got
	r.AllocatedSpace += got
	return
}

func (r *RangeAllocator[T]) Free(start int64, end int64) bool {
	// Find the allocation to be freed. The btree's less function only uses
	// Start, so Get will find an item with the same start. We must verify
	// the End also matches.
	itemToFind := RangeAllocation[T]{Start: start}
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
	newlyFreed := freeRange{Start: start, End: end}

	// Check for merge with range after
	if next, nextFound := r.freeList.Get(freeRange{Start: end + 1}); nextFound {
		r.freeList.Delete(next)
		newlyFreed.End = next.End
	}

	// Check for merge with range before
	var prev freeRange
	var prevFound bool
	r.freeList.DescendLessOrEqual(freeRange{Start: start}, func(it freeRange) bool {
		if it.End == start-1 {
			prev = it
			prevFound = true
		}
		return false
	})
	if prevFound {
		r.freeList.Delete(prev)
		newlyFreed.Start = prev.Start
	}

	r.freeList.ReplaceOrInsert(newlyFreed)

	return true
}

func (r *RangeAllocator[T]) findFreeRangeInternal(size int64) (start int64, end int64) {
	var bestFit freeRange
	var foundBestFit bool

	var foundRange freeRange
	var found bool
	r.freeList.Ascend(func(item freeRange) bool {
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
