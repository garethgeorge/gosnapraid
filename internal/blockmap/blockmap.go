package blockmap

import (
	"bufio"
	"io"

	"github.com/garethgeorge/gosnapraid/internal/binencutil"
)

const (
	defaultIOBufferSize = 64 * 1024
)

type BlockMap struct {
	allocator *RangeAllocator[string]
	files     map[string][]RangeAllocation[struct{}]
}

func NewBlockMap(start, end int64) *BlockMap {
	return &BlockMap{
		allocator: NewRangeAllocator[string](start, end),
		files:     make(map[string][]RangeAllocation[struct{}]),
	}
}

func (b *BlockMap) Allocate(fname string, size int64) []RangeAllocation[struct{}] {
	allocations := make([]RangeAllocation[struct{}], 0, 1) // In most cases we'll only need one allocation
	for size > 0 {
		start, end := b.allocator.Allocate(size, fname)
		if end < start {
			return nil
		}
		size -= (end - start + 1)
		allocations = append(allocations, RangeAllocation[struct{}]{Start: start, End: end})
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
		if err := binencutil.WriteUint64(writer, uint64(alloc.Start)); err != nil {
			return err
		}
		if err := binencutil.WriteUint64(writer, uint64(alloc.End)); err != nil {
			return err
		}
		if err := binencutil.WriteUint64(writer, uint64(fileIDMap[alloc.Data])); err != nil {
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
		return err
	}
	for i := uint64(0); i < fileIDMapSize; i++ {
		fid, err := binencutil.ReadUint64(reader)
		if err != nil {
			return err
		}
		fname, err := binencutil.ReadShortString(reader)
		if err != nil {
			return err
		}
		fileIDMap[fid] = fname
	}

	// Read the allocations and add them to the allocator
	allocationsSize, err := binencutil.ReadUint64(reader)
	if err != nil {
		return err
	}
	for i := uint64(0); i < allocationsSize; i++ {
		start, err := binencutil.ReadUint64(reader)
		if err != nil {
			return err
		}
		end, err := binencutil.ReadUint64(reader)
		if err != nil {
			return err
		}
		fid, err := binencutil.ReadUint64(reader)
		if err != nil {
			return err
		}
		fname := fileIDMap[fid]
		b.allocator.SetAllocated(int64(start), int64(end), fname)
		b.files[fname] = append(b.files[fname], RangeAllocation[struct{}]{Start: int64(start), End: int64(end)})
	}
	return nil
}
