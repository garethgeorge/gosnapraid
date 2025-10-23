package snapshot

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"testing/fstest"
	"time"

	"github.com/garethgeorge/gosnapraid/internal/buffers"
	"github.com/garethgeorge/gosnapraid/internal/snapshot/testutil"
	"github.com/garethgeorge/gosnapraid/internal/stripealloc"
	gosnapraidpb "github.com/garethgeorge/gosnapraid/proto/gosnapraid"
	"github.com/google/btree"
	"github.com/stretchr/testify/assert"
)

func tempBuf(t *testing.T) buffers.BufferHandle {
	t.Helper()
	return buffers.CreateCompressedHandle(buffers.InMemoryBufferHandle(nil))
}

func TestCreateSnapshot(t *testing.T) {
	treeConfig := testutil.DefaultTreeConfig()
	memFS := testutil.GenerateMapFS(treeConfig)
	snapshotBuf := tempBuf(t)
	snapshotter := NewSnapshotter(memFS, snapshotBuf, nil)
	stats, err := snapshotter.Update(tempBuf(t))
	if err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}
	expectedEntries := testutil.CountExpectedEntries(treeConfig)
	assert.Equal(t, expectedEntries, stats.NewOrModified, "NewOrModified")
	assert.Equal(t, 0, stats.MovedOrDeduped, "MovedOrDeduped")
	assert.Equal(t, 0, stats.Missing, "Missing")
	assert.Equal(t, 0, stats.Errors, "Errors")
}

func TestUpdateSnapshotNewEntries(t *testing.T) {
	treeConfig := testutil.DefaultTreeConfig()
	memFS := testutil.GenerateMapFS(treeConfig)
	snapshotBuf := tempBuf(t)
	snapshotter := NewSnapshotter(memFS, snapshotBuf, nil)
	_, err := snapshotter.Update(tempBuf(t))
	if err != nil {
		t.Fatalf("Initial CreateSnapshot failed: %v", err)
	}

	// Now modify the FS to add more files
	newTreeConfig := testutil.DefaultTreeConfig()
	newTreeConfig.FilesPerDir += 2 // Add 2 more files per directory
	memFS = testutil.GenerateMapFS(newTreeConfig)

	// Create a new snapshot buffer and update the snapshot
	newSnapshotBuf := tempBuf(t)
	snapshotter = NewSnapshotter(memFS, newSnapshotBuf, snapshotBuf)
	stats, err := snapshotter.Update(tempBuf(t))
	if err != nil {
		t.Fatalf("UpdateSnapshot failed: %v", err)
	}
	expectedNewEntries := testutil.CountExpectedEntries(newTreeConfig) - testutil.CountExpectedEntries(treeConfig)
	expectedUnchanged := testutil.CountExpectedEntries(treeConfig)
	assert.Equal(t, expectedNewEntries, stats.NewOrModified, "NewOrModified")
	assert.Equal(t, expectedUnchanged, stats.Unchanged, "Unchanged")
	assert.Equal(t, 0, stats.MovedOrDeduped, "MovedOrDeduped")
	assert.Equal(t, 0, stats.Errors, "Errors")
}

func TestUpdateSnapshotDeleteEntries(t *testing.T) {
	treeConfig := testutil.DefaultTreeConfig()
	memFS := testutil.GenerateMapFS(treeConfig)
	snapshotBuf := tempBuf(t)
	snapshotter := NewSnapshotter(memFS, snapshotBuf, nil)
	_, err := snapshotter.Update(tempBuf(t))
	if err != nil {
		t.Fatalf("Initial CreateSnapshot failed: %v", err)
	}

	// Now modify the FS to remove some files
	newTreeConfig := testutil.DefaultTreeConfig()
	newTreeConfig.FilesPerDir -= 2 // Remove 2 files per directory
	memFS = testutil.GenerateMapFS(newTreeConfig)

	// Create a new snapshot buffer and update the snapshot
	newSnapshotBuf := tempBuf(t)
	snapshotter = NewSnapshotter(memFS, newSnapshotBuf, snapshotBuf)
	stats, err := snapshotter.Update(tempBuf(t))
	if err != nil {
		t.Fatalf("UpdateSnapshot failed: %v", err)
	}
	expectedMissingEntries := testutil.CountExpectedEntries(treeConfig) - testutil.CountExpectedEntries(newTreeConfig)
	expectedUnchanged := testutil.CountExpectedEntries(newTreeConfig)
	assert.Equal(t, expectedMissingEntries, stats.Missing, "Missing")
	assert.Equal(t, expectedUnchanged, stats.Unchanged, "Unchanged")
	assert.Equal(t, 0, stats.NewOrModified, "NewOrModified")
	assert.Equal(t, 0, stats.MovedOrDeduped, "MovedOrDeduped")
	assert.Equal(t, 0, stats.Errors, "Errors")
}

func TestUpdateSnapshotMoveEntries(t *testing.T) {
	treeConfig := testutil.DefaultTreeConfig()
	memFS := testutil.GenerateMapFS(treeConfig)
	snapshotBuf := tempBuf(t)
	snapshotter := NewSnapshotter(memFS, snapshotBuf, nil)
	_, err := snapshotter.Update(tempBuf(t))
	if err != nil {
		t.Fatalf("Initial CreateSnapshot failed: %v", err)
	}

	// Now modify the FS to move files (simulate by regenerating with different names)
	copy := make(fstest.MapFS)
	for path, file := range memFS {
		copy["moved_"+path] = file
	}

	// Create a new snapshot buffer and update the snapshot
	newSnapshotBuf := tempBuf(t)
	snapshotter = NewSnapshotter(copy, newSnapshotBuf, snapshotBuf)
	stats, err := snapshotter.Update(tempBuf(t))
	if err != nil {
		t.Fatalf("UpdateSnapshot failed: %v", err)
	}
	expectMoved := testutil.CountExpectedFiles(treeConfig)
	expectNewOrModified := testutil.CountExpectedDirs(treeConfig)
	assert.Equal(t, expectMoved, stats.MovedOrDeduped, "MovedOrDeduped")
	assert.Equal(t, expectNewOrModified-1, stats.NewOrModified, "NewOrModified")           // -1 for the root dir
	assert.Equal(t, testutil.CountExpectedEntries(treeConfig)-1, stats.Missing, "Missing") // -1 for the root dir
	assert.Equal(t, 0, stats.Errors, "Errors")
}

func TestUpdateSnapshotModifyEntries(t *testing.T) {
	treeConfig := testutil.DefaultTreeConfig()
	memFS := testutil.GenerateMapFS(treeConfig)
	snapshotBuf := tempBuf(t)
	snapshotter := NewSnapshotter(memFS, snapshotBuf, nil)
	_, err := snapshotter.Update(tempBuf(t))
	if err != nil {
		t.Fatalf("Initial CreateSnapshot failed: %v", err)
	}

	// Now modify the FS to change file contents
	modifiedFS := make(fstest.MapFS)
	for path, file := range memFS {
		if file.Mode.IsRegular() {
			modifiedFS[path] = &fstest.MapFile{
				Data: []byte("modified content"),
				Mode: file.Mode,
			}
		} else {
			modifiedFS[path] = file
		}
	}

	// Create a new snapshot buffer and update the snapshot
	newSnapshotBuf := tempBuf(t)
	snapshotter = NewSnapshotter(modifiedFS, newSnapshotBuf, snapshotBuf)
	stats, err := snapshotter.Update(tempBuf(t))
	if err != nil {
		t.Fatalf("UpdateSnapshot failed: %v", err)
	}
	expectedModifiedEntries := testutil.CountExpectedFiles(treeConfig)
	assert.Equal(t, expectedModifiedEntries, stats.NewOrModified, "NewOrModified")
	assert.Equal(t, 0, stats.MovedOrDeduped, "MovedOrDeduped")
	assert.Equal(t, 0, stats.Missing, "Missing")
	assert.Equal(t, 0, stats.Errors, "Errors")
}

func TestUpdateHashesLoadingAllocations(t *testing.T) {
	// Covers seeding a buffer with a snapshot written directly with readerwriter
	// that marks allocations for us, then loading that snapshot and verifying allocations
	// are correctly loaded.
	snapshotBuf := tempBuf(t)
	snapshotBufWriter, _ := snapshotBuf.GetWriter()
	snapshotWriter, err := NewSnapshotWriter(snapshotBufWriter, &gosnapraidpb.SnapshotHeader{
		Version: Version,
	})
	if err != nil {
		t.Fatalf("Creating snapshot writer: %v", err)
	}

	var ranges []stripealloc.Range
	mapFS := make(fstest.MapFS)
	for i := uint64(0); i < 1000; i += 10 {
		// Create nodes where each uses a distinct range
		curRange := stripealloc.Range{
			Start: i,
			End:   i + 5,
		}
		ranges = append(ranges, curRange)
		fileName := fmt.Sprintf("file_%05d.dat", i)
		fileData := &fstest.MapFile{
			Data:    []byte("dummydata"),
			Mode:    0644,
			ModTime: time.Now(),
		}
		mapFS[fileName] = fileData
		node := &gosnapraidpb.SnapshotNode{
			Path: fileName,
			// Size, Mode, and Mtime must match the fileData above for diffing
			// to correctly copy the stripe ranges for the purpose of the test.
			Size:              uint64(len(fileData.Data)),
			Mode:              uint32(fileData.Mode),
			Mtime:             uint64(fileData.ModTime.UnixNano()),
			StripeRangeStarts: []uint64{curRange.Start},
			StripeRangeEnds:   []uint64{curRange.End},
		}
		err := snapshotWriter.Write(node)
		if err != nil {
			t.Fatalf("Writing snapshot node: %v", err)
		}
	}
	if err := snapshotBufWriter.Close(); err != nil {
		t.Fatalf("Closing snapshot writer: %v", err)
	}

	// Now load the snapshot with a snapshotter and verify allocations are loaded
	// correctly.
	snapshotter := NewSnapshotter(mapFS, tempBuf(t), snapshotBuf)
	stripeAlloc := stripealloc.NewStripeAllocator(1 << 63)
	dedupeTree := btree.NewG[hashToRange](32, func(a, b hashToRange) bool {
		return a.hashhi < b.hashhi || (a.hashhi == b.hashhi && a.hashlo < b.hashlo)
	})
	stats, err := snapshotter.updateHashesAndMarkAllocations(tempBuf(t), stripeAlloc, dedupeTree, snapshotBuf)
	if err != nil {
		t.Fatalf("Updating hashes and loading allocations: %v", err)
	}
	expectedEntries := len(ranges)
	assert.Equal(t, expectedEntries, stats.Unchanged, "Expected entries to be unchanged")

	// Verify all ranges are marked allocated
	for _, r := range ranges {
		if stripeAlloc.IsRangeFree(r) {
			t.Errorf("Expected range %d-%d to be allocated, but it is not", r.Start, r.End)
		}
	}

	// Loop over ranges and collect the set, then assert it matches what we expect
	allocatedRanges := []stripealloc.Range{}
	for r := range stripeAlloc.IterAllocs() {
		allocatedRanges = append(allocatedRanges, r)
	}
	if !reflect.DeepEqual(allocatedRanges, ranges) {
		t.Errorf("Expected allocated ranges %v, got %v", ranges, allocatedRanges)
	}
	t.Logf("Allocated ranges: %v", allocatedRanges)
}

func FuzzUpdateHashesLoadingAllocations(f *testing.F) {
	// Add seed corpus
	f.Add(int64(12345), 0.5, uint64(1000))
	f.Add(int64(67890), 0.1, uint64(10000))
	f.Add(int64(11111), 0.9, uint64(500))
	f.Add(int64(99999), 0.0, uint64(100))
	f.Add(int64(55555), 1.0, uint64(5000))

	f.Fuzz(func(t *testing.T, seed int64, allocFraction float64, rangeSize uint64) {
		// Validate inputs
		if allocFraction < 0 || allocFraction > 1 {
			t.Skip("allocFraction must be between 0 and 1")
		}
		if rangeSize < 100 || rangeSize > 10000 {
			t.Skip("rangeSize must be between 100 and 10000")
		}

		// Use seed for reproducible randomness
		rng := testutil.NewSeededRNG(seed)

		// Subdivide the range into random subslices
		type allocInfo struct {
			rng       stripealloc.Range
			allocated bool
			path      string
		}

		var allocs []allocInfo
		currentPos := uint64(0)

		for currentPos < rangeSize {
			// Random slice length from 1 to 100
			sliceLen := uint64(rng.Intn(100) + 1)
			if currentPos+sliceLen > rangeSize {
				sliceLen = rangeSize - currentPos
			}

			// Randomly decide if this slice should be allocated
			shouldAllocate := rng.Float64() < allocFraction

			// Generate a random 32-byte hex string for the path
			randomBytes := make([]byte, 32)
			if _, err := rand.Read(randomBytes); err != nil {
				t.Fatalf("Failed to generate random path: %v", err)
			}
			randomPath := hex.EncodeToString(randomBytes)

			allocs = append(allocs, allocInfo{
				rng: stripealloc.Range{
					Start: currentPos,
					End:   currentPos + sliceLen,
				},
				allocated: shouldAllocate,
				path:      randomPath,
			})

			currentPos += sliceLen
		}

		// Sort by path (lexicographic order)
		sort.Slice(allocs, func(i, j int) bool {
			return allocs[i].path < allocs[j].path
		})

		// Create snapshot nodes for allocated ranges
		snapshotBuf := tempBuf(t)
		snapshotBufWriter, _ := snapshotBuf.GetWriter()
		snapshotWriter, err := NewSnapshotWriter(snapshotBufWriter, &gosnapraidpb.SnapshotHeader{
			Version: Version,
		})
		if err != nil {
			t.Fatalf("Creating snapshot writer: %v", err)
		}

		// Build mapFS and write snapshot nodes
		mapFS := make(fstest.MapFS)
		var expectedAllocatedRanges []stripealloc.Range

		for _, alloc := range allocs {
			fileData := &fstest.MapFile{
				Data:    []byte("dummydata"),
				Mode:    0644,
				ModTime: time.Now(),
			}
			mapFS[alloc.path] = fileData

			node := &gosnapraidpb.SnapshotNode{
				Path:  alloc.path,
				Size:  uint64(len(fileData.Data)),
				Mode:  uint32(fileData.Mode),
				Mtime: uint64(fileData.ModTime.UnixNano()),
			}

			// Only add stripe ranges if this allocation is marked as allocated
			if alloc.allocated {
				node.StripeRangeStarts = []uint64{alloc.rng.Start}
				node.StripeRangeEnds = []uint64{alloc.rng.End}
				expectedAllocatedRanges = append(expectedAllocatedRanges, alloc.rng)
			}

			err := snapshotWriter.Write(node)
			if err != nil {
				t.Fatalf("Writing snapshot node: %v", err)
			}
		}

		if err := snapshotBufWriter.Close(); err != nil {
			t.Fatalf("Closing snapshot writer: %v", err)
		}

		// Now load the snapshot with a snapshotter and verify allocations
		snapshotter := NewSnapshotter(mapFS, tempBuf(t), snapshotBuf)
		stripeAlloc := stripealloc.NewStripeAllocator(rangeSize)
		dedupeTree := btree.NewG[hashToRange](32, func(a, b hashToRange) bool {
			return a.hashhi < b.hashhi || (a.hashhi == b.hashhi && a.hashlo < b.hashlo)
		})
		stats, err := snapshotter.updateHashesAndMarkAllocations(tempBuf(t), stripeAlloc, dedupeTree, snapshotBuf)
		if err != nil {
			t.Fatalf("Updating hashes and loading allocations: %v", err)
		}

		// Verify stats - all entries should be unchanged since we matched file metadata
		expectedEntries := len(allocs)
		if stats.Unchanged != expectedEntries {
			t.Logf("Stats: %+v", stats)
			t.Errorf("Expected %d unchanged entries, got %d", expectedEntries, stats.Unchanged)
		}

		// Verify all expected ranges are marked allocated
		for _, r := range expectedAllocatedRanges {
			if stripeAlloc.IsRangeFree(r) {
				t.Errorf("Expected range %d-%d to be allocated, but it is not", r.Start, r.End)
			}
		}

		// Collect all allocated ranges and verify they match expected
		allocatedRanges := []stripealloc.Range{}
		for r := range stripeAlloc.IterAllocs() {
			allocatedRanges = append(allocatedRanges, r)
		}

		// Sort expectedAllocatedRanges by start position before merging
		sort.Slice(expectedAllocatedRanges, func(i, j int) bool {
			return expectedAllocatedRanges[i].Start < expectedAllocatedRanges[j].Start
		})

		// Merge adjacent ranges in expectedAllocatedRanges to match what the allocator does
		mergedExpected := mergeAdjacentRanges(expectedAllocatedRanges)

		// Handle nil vs empty slice comparison
		if len(allocatedRanges) == 0 && len(mergedExpected) == 0 {
			// Both are empty, test passes
		} else if !reflect.DeepEqual(allocatedRanges, mergedExpected) {
			t.Errorf("Expected allocated ranges %v, got %v", mergedExpected, allocatedRanges)
		}
	})
}

// mergeAdjacentRanges merges adjacent ranges in a sorted list of ranges.
func mergeAdjacentRanges(ranges []stripealloc.Range) []stripealloc.Range {
	if len(ranges) == 0 {
		return ranges
	}

	merged := []stripealloc.Range{ranges[0]}
	for i := 1; i < len(ranges); i++ {
		last := &merged[len(merged)-1]
		current := ranges[i]

		// If current range is adjacent to the last merged range, extend it
		if last.End == current.Start {
			last.End = current.End
		} else {
			merged = append(merged, current)
		}
	}
	return merged
}

func BenchmarkCreateSnapshot(b *testing.B) {
	treeConfig := testutil.DefaultTreeConfig()
	// A larger tree for a more realistic benchmark
	treeConfig.Depth = 3
	treeConfig.BreadthPerDir = 5
	treeConfig.FilesPerDir = 10
	memFS := testutil.GenerateMapFS(treeConfig)

	b.Logf("BenchmarkCreateSnapshot: FS with %d files/directories", testutil.CountExpectedEntries(treeConfig))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		snapshotBuf := buffers.CreateCompressedHandle(buffers.InMemoryBufferHandle(nil))
		snapshotter := NewSnapshotter(memFS, snapshotBuf, nil)
		// The argument to Update is a temp buffer for hashes
		_, err := snapshotter.Update(buffers.CreateCompressedHandle(buffers.InMemoryBufferHandle(nil)))
		if err != nil {
			b.Fatalf("CreateSnapshot failed: %v", err)
		}
	}
}

func BenchmarkUpdateSnapshot(b *testing.B) {
	treeConfig := testutil.DefaultTreeConfig()
	// A larger tree for a more realistic benchmark
	treeConfig.Depth = 3
	treeConfig.BreadthPerDir = 5
	treeConfig.FilesPerDir = 10
	memFS := testutil.GenerateMapFS(treeConfig)

	priorSnapshotBuf := buffers.CreateCompressedHandle(buffers.InMemoryBufferHandle(nil))
	priorSnapshotter := NewSnapshotter(memFS, priorSnapshotBuf, nil)
	_, err := priorSnapshotter.Update(buffers.CreateCompressedHandle(buffers.InMemoryBufferHandle(nil)))
	if err != nil {
		b.Fatalf("Create prior snapshot failed: %v", err)
	}

	b.Logf("BenchmarkUpdateSnapshot: FS with %d files/directories", testutil.CountExpectedEntries(treeConfig))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		snapshotBuf := buffers.CreateCompressedHandle(buffers.InMemoryBufferHandle(nil))
		snapshotter := NewSnapshotter(memFS, snapshotBuf, priorSnapshotBuf)
		// The argument to Update is a temp buffer for hashes
		_, err := snapshotter.Update(buffers.CreateCompressedHandle(buffers.InMemoryBufferHandle(nil)))
		if err != nil {
			b.Fatalf("CreateSnapshot failed: %v", err)
		}
	}
}
