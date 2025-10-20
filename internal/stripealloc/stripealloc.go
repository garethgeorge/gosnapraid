package stripealloc

import (
	"fmt"

	"github.com/google/btree"
)

type freeRangeBySize struct {
	size  uint64
	start uint64
}

// StripeAllocator returns free stripes for a requested allocation amount.
// It is not thread-safe.
type StripeAllocator struct {
	TotalCapacity     uint64
	AvailableCapacity uint64

	// FreeList tracks free ranges, ordered by start address.
	FreeList *btree.BTreeG[Range]
	// FreeListBySize tracks free ranges, ordered by size, then start address.
	FreeListBySize *btree.BTreeG[freeRangeBySize]
}

func NewStripeAllocator(initialCapacity uint64) *StripeAllocator {
	s := &StripeAllocator{
		TotalCapacity:     initialCapacity,
		AvailableCapacity: initialCapacity,
		FreeList:          btree.NewG[Range](32, func(a, b Range) bool { return a.Start < b.Start }),
		FreeListBySize: btree.NewG[freeRangeBySize](32, func(a, b freeRangeBySize) bool {
			if a.size != b.size {
				return a.size < b.size
			}
			return a.start < b.start
		}),
	}
	if initialCapacity > 0 {
		s.addFreeRange(Range{Start: 0, End: initialCapacity})
	}
	return s
}

func (s *StripeAllocator) addFreeRange(r Range) {
	if r.Size() == 0 {
		return
	}
	s.FreeList.ReplaceOrInsert(r)
	s.FreeListBySize.ReplaceOrInsert(freeRangeBySize{size: r.Size(), start: r.Start})
}

func (s *StripeAllocator) removeFreeRange(r Range) {
	s.FreeList.Delete(r)
	s.FreeListBySize.Delete(freeRangeBySize{size: r.Size(), start: r.Start})
}

func (s *StripeAllocator) findContainingFreeRange(r Range) (Range, bool) {
	var containingFreeRange Range
	var found bool
	s.FreeList.DescendLessOrEqual(Range{Start: r.Start}, func(item Range) bool {
		if item.End >= r.End {
			containingFreeRange = item
			found = true
		}
		return false
	})
	return containingFreeRange, found
}

// IsRangeFree returns true if the entire given range is free.
func (s *StripeAllocator) IsRangeFree(r Range) bool {
	if r.Size() == 0 {
		return true
	}
	_, found := s.findContainingFreeRange(r)
	return found
}

// MarkAllocated marks the given ranges as allocated in the allocator.
func (s *StripeAllocator) MarkAllocated(ranges ...Range) error {
	for _, r := range ranges {
		if r.Size() == 0 {
			continue
		}

		containingFreeRange, found := s.findContainingFreeRange(r)
		if !found {
			return fmt.Errorf("range %v is not in a free block (already allocated or out of bounds)", r)
		}

		s.removeFreeRange(containingFreeRange)
		s.AvailableCapacity -= r.Size()

		// Add back remaining parts of the split free range
		if containingFreeRange.Start < r.Start {
			s.addFreeRange(Range{Start: containingFreeRange.Start, End: r.Start})
		}
		if containingFreeRange.End > r.End {
			s.addFreeRange(Range{Start: r.End, End: containingFreeRange.End})
		}
	}
	return nil
}

// Free marks a previously allocated range as free, merging with adjacent free ranges if possible.
func (s *StripeAllocator) Free(r Range) error {
	if r.Size() == 0 {
		return nil
	}

	// Check for double free
	if _, found := s.findContainingFreeRange(r); found {
		return fmt.Errorf("range %v to free is already free (double free?)", r)
	}

	mergedRange := r

	// Merge with range before
	var beforeRange Range
	var foundBefore bool
	s.FreeList.DescendLessOrEqual(Range{Start: r.Start}, func(item Range) bool {
		if item.End == r.Start {
			beforeRange = item
			foundBefore = true
		}
		return false
	})
	if foundBefore {
		s.removeFreeRange(beforeRange)
		mergedRange = mergedRange.Merge(beforeRange)
	}

	// Merge with range after
	afterRange, foundAfter := s.FreeList.Get(Range{Start: r.End})
	if foundAfter {
		s.removeFreeRange(afterRange)
		mergedRange = mergedRange.Merge(afterRange)
	}

	s.addFreeRange(mergedRange)
	s.AvailableCapacity += r.Size()

	return nil
}

// Allocate attempts to allocate numStripes stripes, returns an ErrNoCapacity if not enough space is available.
func (s *StripeAllocator) Allocate(numStripes uint64) (Range, error) {
	if numStripes == 0 {
		return EmptyRange, nil
	}

	var foundRangeBySize freeRangeBySize
	var found bool

	// Find smallest free range that is large enough
	s.FreeListBySize.AscendGreaterOrEqual(freeRangeBySize{size: numStripes}, func(item freeRangeBySize) bool {
		foundRangeBySize = item
		found = true
		return false // stop
	})

	if !found {
		return EmptyRange, ErrNoCapacity
	}

	allocatedRange := Range{Start: foundRangeBySize.start, End: foundRangeBySize.start + numStripes}

	if err := s.MarkAllocated(allocatedRange); err != nil {
		return EmptyRange, fmt.Errorf("internal allocator error: %w", err)
	}

	return allocatedRange, nil
}

// MultiAllocate attempts to allocate numStripes stripes, returning multiple ranges if necessary.
func (s *StripeAllocator) MultiAllocate(numStripes uint64) ([]Range, error) {
	if numStripes == 0 {
		return nil, nil
	}
	if numStripes > s.AvailableCapacity {
		return nil, ErrNoCapacity
	}

	var allocatedRanges []Range
	remainingToAllocate := numStripes

	for remainingToAllocate > 0 {
		largestFree, hasFree := s.FreeListBySize.Max()
		if !hasFree {
			break
		}

		freeRange := Range{Start: largestFree.start, End: largestFree.start + largestFree.size}

		var toAllocate Range
		if freeRange.Size() >= remainingToAllocate {
			toAllocate = Range{Start: freeRange.Start, End: freeRange.Start + remainingToAllocate}
			remainingToAllocate = 0
		} else {
			toAllocate = freeRange
			remainingToAllocate -= freeRange.Size()
		}

		if err := s.MarkAllocated(toAllocate); err != nil {
			panic("internal allocator error: " + err.Error())
		}
		allocatedRanges = append(allocatedRanges, toAllocate)
	}

	if remainingToAllocate > 0 {
		panic("internal allocator error: not enough capacity despite earlier check")
	}

	return allocatedRanges, nil
}

// Size reports the total size of the allocator's managed space.
func (s *StripeAllocator) Size() uint64 {
	return s.TotalCapacity
}

// ResizeTo increases or decreases the allocator's internal structures to cover at least newSize.
func (s *StripeAllocator) ResizeTo(newSize uint64) error {
	if newSize == s.TotalCapacity {
		return nil
	}

	if newSize < s.TotalCapacity { // Shrink
		var lastAllocatedOffset uint64
		if s.AvailableCapacity == s.TotalCapacity {
			lastAllocatedOffset = 0
		} else {
			lastAllocatedOffset = s.TotalCapacity - 1
			lastFree, hasLastFree := s.FreeList.Max()
			if hasLastFree && lastFree.End == s.TotalCapacity {
				if lastFree.Start > 0 {
					lastAllocatedOffset = lastFree.Start - 1
				} else {
					lastAllocatedOffset = 0
				}
			}
		}

		requiredSize := uint64(0)
		if s.AvailableCapacity < s.TotalCapacity {
			requiredSize = lastAllocatedOffset + 1
		}

		if newSize < requiredSize {
			return fmt.Errorf("new size %d is too small for current allocations which extend to %d", newSize, lastAllocatedOffset)
		}

		lastFree, hasLastFree := s.FreeList.Max()
		if hasLastFree && lastFree.End > newSize {
			s.removeFreeRange(lastFree)
			if lastFree.Start < newSize {
				s.addFreeRange(Range{Start: lastFree.Start, End: newSize})
			}
		}
		diff := s.TotalCapacity - newSize
		s.TotalCapacity = newSize
		s.AvailableCapacity -= diff

	} else { // Grow
		growth := newSize - s.TotalCapacity
		newFreeRange := Range{Start: s.TotalCapacity, End: newSize}

		// Try to merge with the last free range
		var merged bool
		lastFree, hasLastFree := s.FreeList.Max()
		if hasLastFree && lastFree.End == s.TotalCapacity {
			s.removeFreeRange(lastFree)
			mergedRange := lastFree.Merge(newFreeRange)
			s.addFreeRange(mergedRange)
			merged = true
		}

		if !merged {
			s.addFreeRange(newFreeRange)
		}

		s.TotalCapacity = newSize
		s.AvailableCapacity += growth
	}

	return nil
}

// ShrinkToFit shrinks the allocator's internal structures to fit the range used by current allocations.
func (s *StripeAllocator) ShrinkToFit() error {
	if s.AvailableCapacity == s.TotalCapacity {
		return s.ResizeTo(0)
	}

	var lastAllocatedOffset uint64 = s.TotalCapacity - 1
	lastFree, hasLastFree := s.FreeList.Max()
	if hasLastFree && lastFree.End == s.TotalCapacity {
		if lastFree.Start > 0 {
			lastAllocatedOffset = lastFree.Start - 1
		} else {
			return s.ResizeTo(0)
		}
	}

	return s.ResizeTo(lastAllocatedOffset + 1)
}
