package snapshot

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/google/btree"

	"github.com/garethgeorge/gosnapraid/internal/buffers"
	"github.com/garethgeorge/gosnapraid/internal/sliceutil"
	"github.com/garethgeorge/gosnapraid/internal/snapshot/fsscan"
	"github.com/garethgeorge/gosnapraid/internal/stripealloc"
	gosnapraidpb "github.com/garethgeorge/gosnapraid/proto/gosnapraid"
	sha256 "github.com/minio/sha256-simd"
	"github.com/zeebo/blake3"
)

const (
	Version   = 1
	BlockSize = 64 * 1024 // 64 KiB blocks
)

type SnapshotStats struct {
	Unchanged      int // unchanged files
	NewOrModified  int // new or modified files
	Missing        int // missing files from previous snapshot, may have been deleted or moved
	MovedOrDeduped int // includes moved and duplicated files detected by hash
	Errors         int
}

type Snapshotter struct {
	fs             fs.FS
	snapshotBuf    buffers.BufferHandle
	oldSnapshotBuf buffers.BufferHandle

	hashFunc gosnapraidpb.HashType
}

func NewSnapshotter(fs fs.FS, snapshotBuf buffers.BufferHandle, oldSnapshotBuf buffers.BufferHandle) *Snapshotter {
	return &Snapshotter{
		fs:             fs,
		snapshotBuf:    snapshotBuf,
		oldSnapshotBuf: oldSnapshotBuf,

		hashFunc: gosnapraidpb.HashType_HASH_BLAKE3,
	}
}

// Update creates a new snapshot by comparing the current state of the filesystem
// against the old snapshot. It populates file hashes for new or modified files,
// and reuses hashes and stripe ranges for unchanged files. If move detection
// is enabled, it also detects moved or duplicate files based on their hashes.
func (s *Snapshotter) Update(tempBuf buffers.BufferHandle) (SnapshotStats, error) {
	if tempBuf == nil {
		tempBuf = buffers.CreateCompressedHandle(buffers.InMemoryBufferHandle(nil))
	}
	dedupeTree, err := s.buildDedupeTree()
	if err != nil {
		return SnapshotStats{}, fmt.Errorf("building dedupe/move detection tree: %w", err)
	}

	alloc := stripealloc.NewStripeAllocator(1 << 63)

	stats, err := s.updateHashesAndMarkAllocations(tempBuf, alloc, dedupeTree, s.oldSnapshotBuf)

	// FOR NOW: copy tempBuf to s.snapshotBuf
	if err == nil {
		writer, err := s.snapshotBuf.GetWriter()
		if err != nil {
			return stats, fmt.Errorf("getting snapshot writer: %w", err)
		}
		defer writer.Close()
		reader, err := tempBuf.GetReader()
		if err != nil {
			return stats, fmt.Errorf("getting temp buffer reader: %w", err)
		}
		defer reader.Close()
		_, err = io.Copy(writer, reader)
		if err != nil {
			return stats, fmt.Errorf("copying temp buffer to snapshot buffer: %w", err)
		}
	}

	return stats, err
}

func (s *Snapshotter) buildDedupeTree() (*btree.BTreeG[hashToRange], error) {
	dedupeTree := btree.NewG[hashToRange](32, func(a, b hashToRange) bool {
		return a.hashhi < b.hashhi || (a.hashhi == b.hashhi && a.hashlo < b.hashlo)
	})

	if s.oldSnapshotBuf == nil {
		return dedupeTree, nil
	}

	var rangeStartBlock []uint64
	var rangeEndBlock []uint64

	// Open the old snapshot for reading
	bytesReader, err := s.oldSnapshotBuf.GetReader()
	if err != nil {
		return nil, fmt.Errorf("getting old snapshot reader: %w", err)
	}
	defer bytesReader.Close()
	reader, header, err := NewSnapshotReader(bytesReader)
	if err != nil {
		return nil, fmt.Errorf("creating old snapshot reader: %w", err)
	}
	if header.Version != Version {
		return nil, fmt.Errorf("old snapshot version %d does not match expected version %d", header.Version, Version)
	}

	// Iterate through the old snapshot and build the dedupe tree
	for node, err := range reader.Iter() {
		if err != nil {
			return nil, fmt.Errorf("reading old snapshot: %w", err)
		}
		if len(node.StripeRangeStarts) != len(node.StripeRangeEnds) {
			return nil, fmt.Errorf("invalid stripe ranges for file %q", node.Path)
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
		dedupeTree.ReplaceOrInsert(hashEntry)
	}

	return dedupeTree, nil
}

// updateHashesAndMarkAllocations updates file hashes for new or modified files
// and loads allocated stripe ranges for unchanged files from the old snapshot.
//
// Provide an empty allocator and a dedupeTree (which may be empty in the case of no prior snapshot).
// The allocator will be populated with new allocations for new or modified files.
//
// The old snapshot is read from oldSnapshotBuf (optional), and the new snapshot is written to newSnapshotBuf.
// It is expected that the sort orders must match to allow effective left-join processing.
func (s *Snapshotter) updateHashesAndMarkAllocations(
	newSnapshotBuf buffers.BufferHandle,
	allocator *stripealloc.StripeAllocator,
	dedupeTree *btree.BTreeG[hashToRange],
	oldSnapshotBuf buffers.BufferHandle,
) (SnapshotStats, error) {
	dirTreeIter := fsscan.WalkFS(s.fs)

	// Load the old snapshot which will be in order by hashes.
	oldSnapshotIter := emptySnapshotIter()
	if oldSnapshotBuf != nil {
		bytesReader, err := s.oldSnapshotBuf.GetReader()
		if err != nil {
			return SnapshotStats{}, fmt.Errorf("getting old snapshot reader: %w", err)
		}
		defer bytesReader.Close()
		reader, header, err := NewSnapshotReader(bytesReader)
		if err != nil {
			return SnapshotStats{}, fmt.Errorf("creating old snapshot reader: %w", err)
		}
		if header.Version != Version {
			return SnapshotStats{}, fmt.Errorf("old snapshot version %d does not match expected version %d", header.Version, Version)
		}
		oldSnapshotIter = reader.Iter()
	}

	// Create the new snapshot writer
	bytesWriter, err := newSnapshotBuf.GetWriter()
	if err != nil {
		return SnapshotStats{}, fmt.Errorf("getting new snapshot writer: %w", err)
	}
	defer bytesWriter.Close()
	writer, err := NewSnapshotWriter(bytesWriter, &gosnapraidpb.SnapshotHeader{
		Version:   Version,
		Timestamp: uint64(time.Now().UnixNano()),
	})
	if err != nil {
		return SnapshotStats{}, fmt.Errorf("creating new snapshot writer: %w", err)
	}

	fullOuterJoin := sliceutil.FullOuterJoinIters(dirTreeIter, transformErrIter(oldSnapshotIter), func(a fsscan.FileMetadata, b struct {
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

	var rangeList []stripealloc.Range

	for diskFile, oldSnapshotItem := range fullOuterJoin {
		if oldSnapshotItem.Error != nil {
			return stats, fmt.Errorf("reading old snapshot: %w", oldSnapshotItem.Error)
		}
		if diskFile.Error != nil {
			stats.Errors++
			continue
		}
		if diskFile == (fsscan.FileMetadata{}) {
			stats.Missing++
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
				if existing, found := dedupeTree.Get(hashToRange{
					hashlo: node.Hashlo,
					hashhi: node.Hashhi,
				}); found {
					// Found existing file with same hash, reuse its stripe ranges
					node.StripeRangeStarts = existing.sliceStarts
					node.StripeRangeEnds = existing.sliceEnds
					stats.MovedOrDeduped++
				} else {
					stats.NewOrModified++
				}
			} else {
				stats.NewOrModified++
			}
		}

		// Mark the range as allocated in the allocator
		rangeList, err := stripealloc.RangeListFromSlices(node.StripeRangeStarts, node.StripeRangeEnds, rangeList[:0])
		if err != nil {
			return stats, fmt.Errorf("creating range list for file %q: %w", diskFile.Path, err)
		}
		if err := allocator.MarkAllocated(rangeList...); err != nil {
			return stats, fmt.Errorf("marking range allocated for file %q: %w", diskFile.Path, err)
		}

		// Write the node to the new snapshot
		if err := writer.Write(node); err != nil {
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

// func (s *Snapshotter) allocateMissingRanges(outSnapBuf buffers.BufferHandle, inSnapBuf buffers.BufferHandle, allocator *stripealloc.StripeAllocator) error {
// 	// Create a snapshot reader and writer
// 	bytesReader, err := inSnapBuf.GetReader()
// 	if err != nil {
// 		return fmt.Errorf("getting snapshot reader: %w", err)
// 	}
// 	defer bytesReader.Close()
// 	reader, header, err := NewSnapshotReader(bytesReader)
// 	if err != nil {
// 		return fmt.Errorf("creating snapshot reader: %w", err)
// 	}
// 	if header.Version != Version {
// 		return fmt.Errorf("snapshot version %d does not match expected version %d", header.Version, Version)
// 	}

// 	bytesWriter, err := outSnapBuf.GetWriter()
// 	if err != nil {
// 		return fmt.Errorf("getting temp snapshot writer: %w", err)
// 	}
// 	defer bytesWriter.Close()
// 	writer, err := NewSnapshotWriter(bytesWriter, &gosnapraidpb.SnapshotHeader{
// 		Version:   Version,
// 		Timestamp: uint64(time.Now().UnixNano()),
// 	})
// 	if err != nil {
// 		return fmt.Errorf("creating temp snapshot writer: %w", err)
// 	}

// 	for node, err := range reader.Iter() {
// 		if err != nil {
// 			return fmt.Errorf("reading snapshot: %w", err)
// 		}

// 		if !fs.FileMode(node.Mode).IsRegular() {
// 			continue
// 		}

// 		// Determine how many stripes are needed
// 		numStripes := (node.Size + BlockSize - 1) / BlockSize

// 		// Todo: if there are existing ranges, verify that they cover the expected size as a sanity check
// 		// Todo: if ranges need to be allocated, allocate them using the range allocator.

// 		if len(node.StripeRangeStarts) == 0 || len(node.StripeRangeEnds) == 0 { {
// 			// Allocate new ranges
// 			rangeList, err := allocator.Allocate(numStripes)
// 			if err != nil {
// 				return fmt.Errorf("allocating ranges for file %q: %w", node.Path, err)
// 			}
// 			// Convert to slices
// 			node.StripeRangeStarts = make([]uint64, len(rangeList))
// 			node.StripeRangeEnds = make([]uint64, len(rangeList))
// 			for i, r := range rangeList {
// 				node.StripeRangeStarts[i] = uint64(r.Start)
// 				node.StripeRangeEnds[i] = uint64(r.End)
// 			}
// 		}

// 	}

// }

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
