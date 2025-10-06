package blockmap

import (
	"bufio"
	"fmt"
	"io"

	"github.com/garethgeorge/gosnapraid/internal/binencutil"
)

const (
	defaultIOBufferSize = 64 * 1024
)

type FileOffset int64 // Offset within a file in chunks for the allocation
type FileRangeAllocation RangeAllocation[FileOffset]

type FileAllocationAndName struct {
	FileRangeAllocation
	Name string
}

type fileAndOffset struct {
	offset FileOffset
	name   string
}

type BlockMap struct {
	Domain    Range // domain of all allocations
	allocator *RangeAllocator[fileAndOffset]
	files     map[string][]FileRangeAllocation
}

func NewBlockMap(start, end int64) *BlockMap {
	return &BlockMap{
		Domain:    Range{Start: start, End: end},
		allocator: NewRangeAllocator[fileAndOffset](start, end),
		files:     make(map[string][]FileRangeAllocation),
	}
}

func (b *BlockMap) BlockCount() int64 {
	return b.Domain.Size()
}

func (b *BlockMap) Allocate(fname string, size int64) []FileRangeAllocation {
	allocations := make([]FileRangeAllocation, 0, 1) // In most cases we'll only need one allocation
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
				b.allocator.Free(allocation.Start, allocation.End)
			}
			return nil
		}
		size -= (end - start + 1)
		allocations = append(allocations, FileRangeAllocation{
			Range: Range{Start: start, End: end},
			Data:  FileOffset(offset),
		})
		offset += size
	}
	b.files[fname] = allocations
	return allocations
}

func (b *BlockMap) Free(fname string) {
	allocations, ok := b.files[fname]
	if !ok {
		return
	}
	for _, allocation := range allocations {
		b.allocator.Free(allocation.Start, allocation.End)
	}
	delete(b.files, fname)
}

func (b *BlockMap) Serialize(writer io.Writer) error {
	bufioWriter := bufio.NewWriterSize(writer, defaultIOBufferSize)
	defer bufioWriter.Flush()
	writer = bufioWriter

	// Loop over the file ID map defining a unique ID for each file
	var fid uint64 = 0
	fileIDMap := make(map[string]uint64, len(b.files))
	for fname := range b.files {
		fid++
		fileIDMap[fname] = fid
	}

	// Write the file ID map
	binencutil.WriteUint64(writer, uint64(len(fileIDMap)))
	for fname, fid := range fileIDMap {
		if err := binencutil.WriteUint64(writer, fid); err != nil {
			return err
		}
		if err := binencutil.WriteShortString(writer, fname); err != nil {
			return err
		}
	}

	// Write the allocations by using the iterator
	binencutil.WriteUint64(writer, uint64(b.allocator.allocatedList.Len()))
	for alloc := range b.allocator.AllocationIter() {
		if err := alloc.Range.Serialize(writer); err != nil {
			return err
		}
		if err := binencutil.WriteUint64(writer, fileIDMap[alloc.Data.name]); err != nil {
			return err
		}
		if err := binencutil.WriteUint64(writer, uint64(alloc.Data.offset)); err != nil {
			return err
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
	for i := uint64(0); i < allocationsSize; i++ {
		allocRange, err := DeserializeRange(reader)
		if err != nil {
			return fmt.Errorf("reading allocation range: %w", err)
		}
		fid, err := binencutil.ReadUint64(reader)
		if err != nil {
			return fmt.Errorf("reading file ID: %w", err)
		}
		offsetUnsigned, err := binencutil.ReadUint64(reader)
		if err != nil {
			return fmt.Errorf("reading file offset: %w", err)
		}
		offset := FileOffset(offsetUnsigned)

		fname := fileIDMap[fid]
		b.allocator.SetAllocated(int64(allocRange.Start), int64(allocRange.End), fileAndOffset{
			name:   fname,
			offset: offset,
		})
		alloc := FileRangeAllocation{
			Range: allocRange,
			Data:  offset,
		}
		b.files[fname] = append(b.files[fname], alloc)
	}
	return nil
}

// AllocationIter returns an iterator over all allocations in the blockmap
// note that it may yield the same file name multiple times if it has multiple allocations.
// Allocations are returned in order of their range start.
func (d *BlockMap) AllocationIter() func(yield func(FileAllocationAndName) bool) {
	return func(yield func(FileAllocationAndName) bool) {
		for alloc := range d.allocator.AllocationIter() {
			if !yield(FileAllocationAndName{
				FileRangeAllocation: FileRangeAllocation{Range: alloc.Range, Data: alloc.Data.offset},
				Name:                alloc.Data.name,
			}) {
				return
			}
		}
	}
}
