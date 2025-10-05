package blockmap

import (
	"errors"
	"slices"
)

// needs to be able to do the following
// 1. keep previous allocation state to avoid reallocating blocks
// 2. mark blocks that are still allocated in the current snapshot as they're discovered
// 3. at the end, free any blocks that weren't marked in the current "generation"
// is that all there is to it? We maybe need to have a "generation" counter to track
// which blocks were marked in which generation.
// 4. generation is a uint16, we can set it randomly to have a high probablity of freeing blocks from previous passes after a ... pass.

// BlockID is a block ID
type BlockID uint64

// BlockRange is a range of block IDs
type BlockRange struct {
	Start BlockID
	End   BlockID
	Gen   uint16
}

// BlockSet is a sorted list of non-overlapping ranges
type BlockSet []BlockRange

type BlockMap struct {
	// TODO
}

// Implement a simple freelist allocator
type BlockAllocator struct {
	TotalRange      BlockRange
	FreeRanges      BlockSet
	AllocatedRanges BlockSet
}

func NewBlockAllocator(totalRange BlockRange) *BlockAllocator {
	return &BlockAllocator{
		FreeRanges: BlockSet{},
	}
}

func (a *BlockAllocator) Allocate(size uint64) (BlockSet, error) {
	// Loop through free ranges and see if any of them are large enough to satisfy the allocation.
	for _, frange := range a.FreeRanges {
		if uint64(frange.End-frange.Start) >= size {
			a.MarkAllocated(BlockRange{
				Start: frange.Start,
				End:   frange.Start + BlockID(size),
			})
			return BlockSet{{
				Start: frange.Start,
				End:   frange.Start + BlockID(size),
			}}, nil
		}
	}

	return BlockSet{}, errors.New("no more space")
}

func (a *BlockAllocator) MarkAllocated(brange BlockRange) {
	// Merge the range into the free ranges
	idx, _ := slices.BinarySearchFunc(a.FreeRanges, brange.Start, func(r BlockRange, id BlockID) int {
		return int(r.Start - id)
	})
	idx -= 1
	if idx < 0 {
		idx = 0
	}

	for brange.End > a.FreeRanges[idx].Start {
		if brange.Start <= a.FreeRanges[idx].Start && brange.End >= a.FreeRanges[idx].End {
			// Remove the free range entirely if contained within the range.
			a.FreeRanges = append(a.FreeRanges[:idx], a.FreeRanges[idx+1:]...)
			continue
		} else if brange.Start < a.FreeRanges[idx].Start {
			// Cover the scenario where it ends inside the current range but starts earlier, we should shrink the current range.
			a.FreeRanges[idx].Start = brange.End
		} else if brange.End > a.FreeRanges[idx].End {
			// Cover the scenario where it starts inside the current range but ends later, we should shrink the current range.
			a.FreeRanges[idx].End = brange.Start
		}
		idx++
	}
}

func (a *BlockAllocator) MarkUnallocated(brange BlockRange) {
	// Insert the range into the free ranges
	idx, _ := slices.BinarySearchFunc(a.FreeRanges, brange.Start, func(r BlockRange, id BlockID) int {
		return int(r.Start - id)
	})
	idx -= 1
	if idx < 0 {
		idx = 0
	}

	// TODO: followup on all of this datastructures and algorithms work tomorrow :)
}
