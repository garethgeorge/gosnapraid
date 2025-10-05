package blockmap

import (
	"github.com/google/btree"
)

type freeBlockRange struct {
	Start int64
	End   int64
}

type allocatedBlockRange[T any] struct {
	Start int64
	End   int64
	Data  T
}

type RangeAllocator[T any] struct {
	allocatedList *btree.BTreeG[allocatedBlockRange[T]]
}

func NewRangeAllocator[T any]() RangeAllocator[T] {
	return RangeAllocator[T]{
		allocatedList: btree.NewG[allocatedBlockRange[T]](256, func(a, b allocatedBlockRange[T]) bool {
			return a.Start < b.Start
		}),
	}
}

func (r *RangeAllocator[T]) Allocate(size int64) (start int64, end int64, data T, error error) {
	var zero T

	for {
		r.allocatedList.DescendRange(
	}

	return 0, 0, data, nil
}
