package snapshot

import (
	"fmt"
	"reflect"
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
