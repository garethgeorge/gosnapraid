package blockmap

type BlockMap struct {
	allocator *RangeAllocator[string]
}

func NewBlockMap(start, end int64) *BlockMap {
	return &BlockMap{
		allocator: NewRangeAllocator[string](start, end),
	}
}

func (b *BlockMap) Allocate(fname string, size int64) []RangeAllocation[string] {
	allocations := make([]RangeAllocation[string], 0, 1) // In most cases we'll only need one allocation
	for size > 0 {
		start, end := b.allocator.Allocate(size, fname)
		if end == 0 {
			return nil
		}
		size -= end - start
		allocations = append(allocations, RangeAllocation[string]{Start: start, End: end, Data: fname})
	}
	return allocations
}

func (b *BlockMap) Free(allocations []RangeAllocation[string]) {
	for _, allocation := range allocations {
		b.allocator.Free(allocation.Start, allocation.End)
	}
}
