package snapshot

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"iter"

	"github.com/cespare/xxhash/v2"

	"github.com/garethgeorge/gosnapraid/internal/sliceutil"
	"github.com/garethgeorge/gosnapraid/internal/snapshot/fsscan"
	gosnapraidpb "github.com/garethgeorge/gosnapraid/proto/gosnapraid"
)

type SnapshotStats struct {
	Unchanged     int
	NewOrModified int
	Errors        int
}

type Snapshotter struct {
	fs       fs.FS
	hashFunc gosnapraidpb.HashType
}

func NewSnapshotter(fs fs.FS) *Snapshotter {
	return &Snapshotter{
		fs:       fs,
		hashFunc: gosnapraidpb.HashType_HASH_XXHASH64,
	}
}

// TOOD: implement move detection by storing by hash
// in particular the fileformat will simply be [hashhi, hashlo, proto size, <encoded proto>]
//  1. load old snapshot and sort it into path major order
//  2. generate new snapshot and add entries to a hash sorter as already encoded byte segments.
//  3. after snapshot is generated, zip it with the old snapshot. Merge logic is a bit complicated when there are two values
//     for the same hash (ie moved files).
//  4. write out snapshot in hash sorted order.
func (s *Snapshotter) Create(writer *SnapshotWriter, oldSnapshot *SnapshotReader) (SnapshotStats, error) {
	dirTreeIter := fsscan.WalkFS(s.fs)
	oldSnapshotIter := emptySnapshotIter()
	if oldSnapshot != nil {
		oldSnapshotIter = oldSnapshot.Iter()
	}

	leftJoin := sliceutil.LeftJoinIters(dirTreeIter, transformErrIter(oldSnapshotIter), func(a fsscan.FileMetadata, b struct {
		Value *gosnapraidpb.SnapshotNode
		Error error
	}) int {
		if b.Error != nil {
			return -1 // treat error as "less than" so that we process it first
		}
		if a.Path < b.Value.Path {
			return -1
		} else if a.Path > b.Value.Path {
			return 1
		}
		return 0
	})

	var stats SnapshotStats

	for diskFile, oldSnapshotItem := range leftJoin {
		if oldSnapshotItem.Error != nil {
			return stats, fmt.Errorf("reading old snapshot: %w", oldSnapshotItem.Error)
		}

		if diskFile.Error != nil {
			stats.Errors++
			continue
		}

		node := gosnapraidpb.SnapshotNode{
			Path:  diskFile.Path,
			Size:  uint64(diskFile.Size),
			Mtime: uint64(diskFile.Mtime),
			Mode:  uint32(diskFile.Mode),
		}

		if oldSnapshotItem.Value != nil &&
			node.Size == oldSnapshotItem.Value.Size &&
			node.Mtime == oldSnapshotItem.Value.Mtime &&
			node.Mode == oldSnapshotItem.Value.Mode {
			// Unchanged file, copy over hash
			node.Hashtype = oldSnapshotItem.Value.Hashtype
			node.Hashhi = oldSnapshotItem.Value.Hashhi
			node.Hashlo = oldSnapshotItem.Value.Hashlo
			stats.Unchanged++
		} else {
			// New or changed file, if it's an ordinary file try to read it and populate the hash
			if diskFile.Mode.IsRegular() {
				err := s.populateFileHash(diskFile.Path, &node)
				if err != nil {
					return stats, fmt.Errorf("hashing file %q: %w", diskFile.Path, err)
				}
			}
			stats.NewOrModified++
		}

		err := writer.Write(&node)
		if err != nil {
			return stats, fmt.Errorf("write node %q: %w", diskFile.Path, err)
		}
	}
	return stats, nil
}

func (s *Snapshotter) populateFileHash(path string, node *gosnapraidpb.SnapshotNode) error {
	f, err := s.fs.Open(path)
	if err != nil {
		return fmt.Errorf("opening file %q: %w", path, err)
	}
	defer f.Close()

	switch s.hashFunc {
	case gosnapraidpb.HashType_HASH_XXHASH64:
		hash := xxhash.New()
		_, err = io.CopyBuffer(hash, f, make([]byte, 32*1024))
		if err != nil {
			return fmt.Errorf("reading file %q: %w", path, err)
		}
		node.Hashtype = gosnapraidpb.HashType_HASH_XXHASH64
		node.Hashlo = binary.LittleEndian.Uint64(hash.Sum(nil))
		node.Hashhi = 0 // not used for xxhash64
		return nil
	default:
		return fmt.Errorf("unsupported hash type: %v", s.hashFunc)
	}
}

func emptySnapshotIter() iter.Seq2[*gosnapraidpb.SnapshotNode, error] {
	return func(yield func(*gosnapraidpb.SnapshotNode, error) bool) {
	}
}

func transformErrIter[T any](it iter.Seq2[T, error]) iter.Seq[struct {
	Value T
	Error error
}] {
	return func(yield func(struct {
		Value T
		Error error
	}) bool) {
		for val, err := range it {
			if !yield(struct {
				Value T
				Error error
			}{
				Value: val,
				Error: err,
			}) {
				return
			}
		}
	}
}
