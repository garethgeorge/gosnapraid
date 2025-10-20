package snapshot

import (
	"testing"
	"testing/fstest"

	"github.com/garethgeorge/gosnapraid/internal/buffers"
	"github.com/garethgeorge/gosnapraid/internal/snapshot/testutil"
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
