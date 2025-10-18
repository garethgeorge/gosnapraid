package blockmap

import (
	"bufio"
	"fmt"
	"io"

	"github.com/garethgeorge/gosnapraid/internal/binencutil"
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

func (b *BlockMap) Serialize(writer io.Writer) error {
	bufioWriter := bufio.NewWriterSize(writer, defaultIOBufferSize)
	defer bufioWriter.Flush()
	writer = bufioWriter

	// Loop over the file ID map defining a unique ID for each file
	var fid uint64 = 0
	fileIDMap := make(map[string]uint64, b.files.Len())
	binencutil.WriteUint64(writer, uint64(b.files.Len()))
	var err error
	b.files.Ascend(func(item FileAllocationSet) bool {
		fid++
		fileIDMap[item.Name] = fid
		if e := binencutil.WriteUint64(writer, fid); e != nil {
			err = e
			return false
		}
		if e := binencutil.WriteShortString(writer, item.Name); e != nil {
			err = e
			return false
		}
		return true
	})
	if err != nil {
		return fmt.Errorf("writing file ID map: %w", err)
	}

	// Write the allocations by using the iterator
	binencutil.WriteUint64(writer, uint64(b.allocator.allocatedList.Len()))
	for alloc := range b.allocator.AllocationIter() {
		var vals [4]uint64
		vals[0] = uint64(alloc.Range.Start)
		vals[1] = uint64(alloc.Range.End)
		vals[2] = fileIDMap[alloc.Data.name]
		vals[3] = uint64(alloc.Data.offset)
		if e := binencutil.WriteUint64s(writer, vals[:]); e != nil {
			return fmt.Errorf("writing allocation range: %w", e)
		}
	}
	return nil
}

func (b *BlockMap) Deserialize(reader io.Reader) error {
	reader = bufio.NewReaderSize(reader, defaultIOBufferSize)

	// Read the file ID map
	fileIDMap := make(map[uint64]string)
	fileIDMapSize, err := binencutil.ReadUint64(reader)
	if err != nil {
		return fmt.Errorf("reading file ID map size: %w", err)
	}
	for i := uint64(0); i < fileIDMapSize; i++ {
		fid, err := binencutil.ReadUint64(reader)
		if err != nil {
			return fmt.Errorf("reading file ID: %w", err)
		}
		fname, err := binencutil.ReadShortString(reader)
		if err != nil {
			return fmt.Errorf("reading file name: %w", err)
		}
		fileIDMap[fid] = fname
	}

	// Read the allocations and add them to the allocator
	allocationsSize, err := binencutil.ReadUint64(reader)
	if err != nil {
		return fmt.Errorf("reading allocations size: %w", err)
	}

	prevRange := Range{Start: -1, End: -1}
	for i := uint64(0); i < allocationsSize; i++ {
		vals, err := binencutil.ReadUint64s(reader, 4)
		if err != nil {
			return fmt.Errorf("reading allocation range: %w", err)
		}
		allocRange := Range{Start: int64(vals[0]), End: int64(vals[1])}
		if allocRange.Start <= prevRange.End {
			return fmt.Errorf("allocation ranges not in ascending order or are overlapping, ranges: %+v and %+v", prevRange, allocRange)
		}
		prevRange = allocRange
		fid := vals[2]
		offset := FileOffset(vals[3])

		// Lookup the file name
		fname, ok := fileIDMap[fid]
		if !ok {
			return fmt.Errorf("unknown file ID %d", fid)
		}

		// Set the allocation in the allocator and in the file allocations map
		b.allocator.SetAllocated(int64(allocRange.Start), int64(allocRange.End), fileAndOffset{
			name:   fname,
			offset: offset,
		})
		alloc := RangeAndOffset{
			Range:  allocRange,
			Offset: offset,
		}
		existingAllocs, ok := b.files.Get(FileAllocationSet{Name: fname})
		if !ok {
			existingAllocs = FileAllocationSet{Name: fname}
		}
		// Allocations are encoded in ascending order so we can just append
		// since they must be sorted
		existingAllocs.Allocs = append(existingAllocs.Allocs, alloc)
		b.files.ReplaceOrInsert(existingAllocs)
	}

	return nil
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
