package snapshot

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"iter"

	"github.com/cespare/xxhash/v2"
	"github.com/google/btree"

	"github.com/garethgeorge/gosnapraid/internal/sliceutil"
	"github.com/garethgeorge/gosnapraid/internal/snapshot/fsscan"
	gosnapraidpb "github.com/garethgeorge/gosnapraid/proto/gosnapraid"
	sha256 "github.com/minio/sha256-simd"
	"github.com/zeebo/blake3"
)

type SnapshotStats struct {
	Unchanged      int
	NewOrModified  int
	MovedOrDeduped int
	Errors         int
}

type Snapshotter struct {
	fs       fs.FS
	hashFunc gosnapraidpb.HashType

	useDedupe  bool
	dedupeTree *btree.BTreeG[hashToRange]
}

func NewSnapshotter(fs fs.FS) *Snapshotter {
	return &Snapshotter{
		fs:        fs,
		hashFunc:  gosnapraidpb.HashType_HASH_BLAKE3,
		useDedupe: false,
		dedupeTree: btree.NewG[hashToRange](64, func(a, b hashToRange) bool {
			return a.hashhi < b.hashhi || (a.hashhi == b.hashhi && a.hashlo < b.hashlo)
		}),
	}
}

func (s *Snapshotter) UseMoveDetection(oldSnapshot *SnapshotReader) error {
	var rangeStartBlock []uint64
	var rangeEndBlock []uint64
	for node, err := range oldSnapshot.Iter() {
		if err != nil {
			return fmt.Errorf("reading old snapshot: %w", err)
		}
		if len(node.StripeRangeStarts) != len(node.StripeRangeEnds) {
			return fmt.Errorf("invalid stripe ranges for file %q", node.Path)
		}

		// Only add regular files to the dedupe tree (directories don't have meaningful hashes)
		if !fs.FileMode(node.Mode).IsRegular() {
			continue
		}

		// Start a new block if there isn't enough capacity for this set of ranges.
		if len(rangeStartBlock)+len(node.StripeRangeStarts) > cap(rangeStartBlock) {
			newBlockSize := 16 * 1024
			if len(node.StripeRangeStarts) > newBlockSize {
				newBlockSize = len(node.StripeRangeStarts)
			}
			rangeStartBlock = make([]uint64, 0, newBlockSize)
		}
		if len(rangeEndBlock)+len(node.StripeRangeEnds) > cap(rangeEndBlock) {
			newBlockSize := 16 * 1024
			if len(node.StripeRangeEnds) > newBlockSize {
				newBlockSize = len(node.StripeRangeEnds)
			}
			rangeEndBlock = make([]uint64, 0, newBlockSize)
		}
		sliceStart := len(rangeStartBlock)
		rangeStartBlock = append(rangeStartBlock, node.StripeRangeStarts...)
		sliceEnd := len(rangeStartBlock)
		sliceStart2 := len(rangeEndBlock)
		rangeEndBlock = append(rangeEndBlock, node.StripeRangeEnds...)
		sliceEnd2 := len(rangeEndBlock)

		// Insert into dedupe tree
		hashEntry := hashToRange{
			hashlo:      node.Hashlo,
			hashhi:      node.Hashhi,
			sliceStarts: rangeStartBlock[sliceStart:sliceEnd],
			sliceEnds:   rangeEndBlock[sliceStart2:sliceEnd2],
		}
		s.dedupeTree.ReplaceOrInsert(hashEntry)
	}

	s.useDedupe = true

	return nil
}

func (s *Snapshotter) Create(writer *SnapshotWriter, oldSnapshot *SnapshotReader) (SnapshotStats, error) {
	dirTreeIter := fsscan.WalkFS(s.fs)

	// Load the old snapshot which will be in order by hashes.
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

	node := &gosnapraidpb.SnapshotNode{}

	for diskFile, oldSnapshotItem := range leftJoin {
		if oldSnapshotItem.Error != nil {
			return stats, fmt.Errorf("reading old snapshot: %w", oldSnapshotItem.Error)
		}

		if diskFile.Error != nil {
			stats.Errors++
			continue
		}

		*node = gosnapraidpb.SnapshotNode{
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
			node.StripeRangeStarts = oldSnapshotItem.Value.StripeRangeStarts
			node.StripeRangeEnds = oldSnapshotItem.Value.StripeRangeEnds
			stats.Unchanged++
		} else {
			// New or changed file, if it's an ordinary file try to read it and populate the hash
			if diskFile.Mode.IsRegular() {
				err := s.populateFileHash(diskFile.Path, node)
				if err != nil {
					stats.Errors++
					continue
				}

				// Check for deduplication (moved/duplicate files)
				if s.useDedupe {
					if existing, found := s.dedupeTree.Get(hashToRange{
						hashlo: node.Hashlo,
						hashhi: node.Hashhi,
					}); found {
						// Found existing file with same hash, reuse its stripe ranges
						node.StripeRangeStarts = existing.sliceStarts
						node.StripeRangeEnds = existing.sliceEnds
						stats.MovedOrDeduped++
					}
				}
			}

			stats.NewOrModified++
		}

		err := writer.Write(node)
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

	// Use a large buffer for copying
	buffer := make([]byte, 128*1024)

	switch s.hashFunc {
	case gosnapraidpb.HashType_HASH_XXHASH64:
		hash := xxhash.New()
		_, err = io.CopyBuffer(hash, f, buffer)
		if err != nil {
			return fmt.Errorf("reading file %q: %w", path, err)
		}
		node.Hashtype = gosnapraidpb.HashType_HASH_XXHASH64
		node.Hashlo = binary.LittleEndian.Uint64(hash.Sum(nil))
		node.Hashhi = 0 // not used for xxhash64
		return nil
	case gosnapraidpb.HashType_HASH_BLAKE3:
		hasher := blake3.New()
		buffer := make([]byte, hasher.BlockSize())
		_, err = io.CopyBuffer(hasher, f, buffer)
		if err != nil {
			return fmt.Errorf("reading file %q: %w", path, err)
		}
		node.Hashtype = gosnapraidpb.HashType_HASH_BLAKE3
		hash := hasher.Sum(nil)
		node.Hashlo = binary.LittleEndian.Uint64(hash[:8])
		node.Hashhi = binary.LittleEndian.Uint64(hash[8:])
		return nil
	case gosnapraidpb.HashType_HASH_SHA256:
		hasher := sha256.New()
		_, err = io.CopyBuffer(hasher, f, buffer)
		if err != nil {
			return fmt.Errorf("reading file %q: %w", path, err)
		}
		node.Hashtype = gosnapraidpb.HashType_HASH_SHA256
		hash := hasher.Sum(nil)
		node.Hashlo = binary.LittleEndian.Uint64(hash[:8])
		node.Hashhi = binary.LittleEndian.Uint64(hash[8:16])
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
