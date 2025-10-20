package blockmap

import (
	"github.com/google/btree"
)

const (
	defaultIOBufferSize = 64 * 1024
)

type FileOffset int64 // Offset within a file in chunks for the allocation

// RangeAndOffset joins a Range in the BlockMap space to an Offset in a file.
// Use FileAllocation type if you need to know which particular file.
type RangeAndOffset struct {
	// The range of the overall block space that starts at Offset in this file.
	Range Range
	// What offset in the physical file this range starts at.
	Offset FileOffset
}

type FileAllocationSet struct {
	// Name of the file these allocations are for
	Name string
	// Allocs should be in sorted order and non-overlapping
	Allocs []RangeAndOffset
}

// internal type used in the range allocator since it only needs to track offsets and file names, range is tracked implicitly.
type fileAndOffset struct {
	offset FileOffset
	name   string
}

type BlockMap struct {
	Domain    Range // domain of all allocations
	allocator *RangeAllocator[fileAndOffset]
	files     *btree.BTreeG[FileAllocationSet] // map of file name to allocations
}

func NewBlockMap(start, end int64) *BlockMap {
	return &BlockMap{
		Domain:    Range{Start: start, End: end},
		allocator: NewRangeAllocator[fileAndOffset](start, end),
		files:     btree.NewG[FileAllocationSet](32, func(a, b FileAllocationSet) bool { return a.Name < b.Name }),
	}
}

func (b *BlockMap) BlockCount() int64 {
	return b.Domain.Size()
}

func (b *BlockMap) Allocate(fname string, size int64) []RangeAndOffset {
	allocations := make([]RangeAndOffset, 0, 1) // In most cases we'll only need one allocation
	offset := int64(0)
	for size > 0 {
		start, end := b.allocator.Allocate(size, fileAndOffset{
			name:   fname,
			offset: FileOffset(offset),
		})
		// If we can't satisfy the full request, free any previous allocations and return nil
		if end < start {
			// Free the previously allocated ranges if we can't satisfy the full request
			for _, allocation := range allocations {
				b.allocator.Free(allocation.Range.Start, allocation.Range.End)
			}
			return nil
		}
		size -= (end - start + 1)
		allocations = append(allocations, RangeAndOffset{
			Range:  Range{Start: start, End: end},
			Offset: FileOffset(offset),
		})
		offset += size
	}
	// Record the allocations against the file
	b.files.ReplaceOrInsert(FileAllocationSet{
		Name:   fname,
		Allocs: allocations,
	})
	return allocations
}

func (b *BlockMap) Free(fname string) {
	allocations, ok := b.files.Get(FileAllocationSet{Name: fname})
	if !ok {
		return
	}
	for _, allocation := range allocations.Allocs {
		b.allocator.Free(allocation.Range.Start, allocation.Range.End)
	}
	b.files.Delete(FileAllocationSet{Name: fname})
}

// AllocationIter returns an iterator over all allocations in the blockmap
// note that it may yield the same file name multiple times if it has multiple allocations.
// Allocations are returned in order of their range start.
func (d *BlockMap) AllocationIter() func(yield func(string, RangeAndOffset) bool) {
	return func(yield func(string, RangeAndOffset) bool) {
		for alloc := range d.allocator.AllocationIter() {
			if !yield(alloc.Data.name, RangeAndOffset{
				Range:  alloc.Range,
				Offset: alloc.Data.offset,
			}) {
				return
			}
		}
	}
}

// FileAllocationsIter returns an iterator over all files and their allocations in the blockmap
// Each file is only yielded once with all its allocations.
// Files are returned in lexicographical order of their names.
func (d *BlockMap) FileAllocationsIter() func(yield func(string, []RangeAndOffset) bool) {
	return func(yield func(string, []RangeAndOffset) bool) {
		d.files.Ascend(func(item FileAllocationSet) bool {
			return yield(item.Name, item.Allocs)
		})
	}
}
