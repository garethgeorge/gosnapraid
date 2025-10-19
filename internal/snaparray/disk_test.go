package snaparray

import (
	"os"
	"testing"

	"github.com/garethgeorge/gosnapraid/internal/progress"
	"github.com/garethgeorge/gosnapraid/internal/snapshot"
	"github.com/garethgeorge/gosnapraid/internal/snapshot/testutil"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDisk_UpdateAndVerifySnapshots(t *testing.T) {
	// 1. Create a fake filesystem
	fsConfig := testutil.DefaultTreeConfig()
	fakeFS := testutil.GenerateMapFS(fsConfig)

	// 2. Create two temporary directories for snapshots
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	dirs := []string{dir1, dir2}

	// 3. Create a Disk instance
	d := &Disk{
		ID: "testdisk",
		FS: fakeFS,
	}

	// 4. Update snapshots
	spinnerProg := progress.NoopSpinnerProgressTracker{}
	stats, finalize, err := d.updateSnapshots(dirs, "", spinnerProg)
	require.NoError(t, err)
	require.NotNil(t, finalize)

	expectedEntries := testutil.CountExpectedEntries(fsConfig)
	assert.Equal(t, expectedEntries, stats.NewOrModified)
	assert.Equal(t, 0, stats.Unchanged)
	assert.Equal(t, 0, stats.Errors)

	err = finalize()
	require.NoError(t, err)

	// 5. Verify snapshots
	barProg := progress.NoopBarProgressTracker{}
	verifiedPath, err := d.verifySnapshots(dirs, barProg)
	require.NoError(t, err)
	require.NotEmpty(t, verifiedPath)

	// 6. Assert that the path is one of the expected snapshot paths
	snapPath1 := d.snapshotPath(dir1)
	snapPath2 := d.snapshotPath(dir2)
	assert.Contains(t, []string{snapPath1, snapPath2}, verifiedPath)

	// 7. Read back the verified snapshot and check its contents
	f, err := os.Open(verifiedPath)
	require.NoError(t, err)
	defer f.Close()

	zstdReader, err := zstd.NewReader(f)
	require.NoError(t, err)
	defer zstdReader.Close()

	snapReader, header, err := snapshot.NewSnapshotReader(zstdReader)
	require.NoError(t, err)
	require.NotNil(t, snapReader)
	assert.Equal(t, uint32(Version), header.Version)

	// Count files in the snapshot and compare with the source filesystem
	count := 0
	for _, err := range snapReader.Iter() {
		require.NoError(t, err)
		count++
	}
	assert.Equal(t, expectedEntries, count)
}

func TestDisk_VerifySnapshots_Mismatch(t *testing.T) {
	// 1. Create two temporary directories
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	dirs := []string{dir1, dir2}

	// 2. Create a Disk instance
	d := &Disk{ID: "testdisk"}

	// 3. Create two different snapshot files
	snapPath1 := d.snapshotPath(dir1)
	err := os.WriteFile(snapPath1, []byte("snapshot1"), 0644)
	require.NoError(t, err)

	snapPath2 := d.snapshotPath(dir2)
	err = os.WriteFile(snapPath2, []byte("snapshot2"), 0644)
	require.NoError(t, err)

	// 4. Verify snapshots
	barProg := progress.NoopBarProgressTracker{}
	_, err = d.verifySnapshots(dirs, barProg)

	// 5. Assert that an error is returned
	require.Error(t, err)
	var errMap *ErrorMap
	require.ErrorAs(t, err, &errMap)
	assert.Contains(t, err.Error(), "snapshot checksum mismatch for disk testdisk")
}

func TestDisk_UpdateAndVerifySnapshots_WithPrior(t *testing.T) {
	// 1. Create a fake filesystem for the prior snapshot
	priorFsConfig := testutil.DefaultTreeConfig()
	priorFsConfig.FilesPerDir = 5
	priorFS := testutil.GenerateMapFS(priorFsConfig)

	// 2. Create a temporary directory for the prior snapshot
	priorDir := t.TempDir()

	// 3. Create a Disk instance for the prior snapshot
	priorDisk := &Disk{
		ID: "testdisk",
		FS: priorFS,
	}

	// 4. Create the prior snapshot
	spinnerProg := progress.NoopSpinnerProgressTracker{}
	priorStats, finalizePrior, err := priorDisk.updateSnapshots([]string{priorDir}, "", spinnerProg)
	require.NoError(t, err)
	require.NotNil(t, finalizePrior)

	priorExpectedEntries := testutil.CountExpectedEntries(priorFsConfig)
	assert.Equal(t, priorExpectedEntries, priorStats.NewOrModified)
	assert.Equal(t, 0, priorStats.Unchanged)

	err = finalizePrior()
	require.NoError(t, err)
	priorSnapshotPath := priorDisk.snapshotPath(priorDir)

	// 5. Create a new fake filesystem with more files
	updatedFsConfig := testutil.DefaultTreeConfig()
	updatedFsConfig.FilesPerDir = 10
	updatedFS := testutil.GenerateMapFS(updatedFsConfig)

	// 6. Create two temporary directories for the new snapshots
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	dirs := []string{dir1, dir2}

	// 7. Create a new Disk instance with the updated filesystem
	d := &Disk{
		ID: "testdisk",
		FS: updatedFS,
	}

	// 8. Update snapshots using the prior snapshot
	stats, finalize, err := d.updateSnapshots(dirs, priorSnapshotPath, spinnerProg)
	require.NoError(t, err)
	require.NotNil(t, finalize)

	updatedExpectedEntries := testutil.CountExpectedEntries(updatedFsConfig)
	assert.Equal(t, updatedExpectedEntries-priorExpectedEntries, stats.NewOrModified)
	assert.Equal(t, priorExpectedEntries, stats.Unchanged)

	err = finalize()
	require.NoError(t, err)

	// 9. Verify snapshots
	barProg := progress.NoopBarProgressTracker{}
	verifiedPath, err := d.verifySnapshots(dirs, barProg)
	require.NoError(t, err)
	require.NotEmpty(t, verifiedPath)

	// 10. Assert that the path is one of the expected snapshot paths
	snapPath1 := d.snapshotPath(dir1)
	snapPath2 := d.snapshotPath(dir2)
	assert.Contains(t, []string{snapPath1, snapPath2}, verifiedPath)

	// 11. Read back the verified snapshot and check its contents
	f, err := os.Open(verifiedPath)
	require.NoError(t, err)
	defer f.Close()

	zstdReader, err := zstd.NewReader(f)
	require.NoError(t, err)
	defer zstdReader.Close()

	snapReader, header, err := snapshot.NewSnapshotReader(zstdReader)
	require.NoError(t, err)
	require.NotNil(t, snapReader)
	assert.Equal(t, uint32(Version), header.Version)

	// Count files in the snapshot and compare with the source filesystem
	count := 0
	for _, err := range snapReader.Iter() {
		require.NoError(t, err)
		count++
	}
	assert.Equal(t, updatedExpectedEntries, count)
}
