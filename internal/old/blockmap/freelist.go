package blockmap

import "github.com/google/btree"

// FreeList keeps track of ranges of free space within a blockmap.
type FreeList struct {
	FreeSpace      int64
	AllocatedSpace int64

	// The list of free ranges, used for finding a free spot.
	freeList *btree.BTreeG[Range]
}
