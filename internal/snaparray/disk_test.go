package snaparray

import (
	"testing"

	"github.com/garethgeorge/gosnapraid/internal/buffers"
	"github.com/garethgeorge/gosnapraid/internal/progress"
	"github.com/garethgeorge/gosnapraid/internal/snapshot"
	"github.com/garethgeorge/gosnapraid/internal/snapshot/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDisk_UpdateAndVerifySnapshots(t *testing.T) {
	// 1. Create a fake filesystem
	fsConfig := testutil.DefaultTreeConfig()
	fakeFS := testutil.GenerateMapFS(fsConfig)

	// 2. Create two in-memory buffers for snapshots
	buf1 := buffers.CreateCompressedHandle(buffers.InMemoryBufferHandle(nil))
	buf2 := buffers.CreateCompressedHandle(buffers.InMemoryBufferHandle(nil))
	outbuffers := []buffers.CompressedBufferHandle{buf1, buf2}

	// 3. Create a Disk instance
	d := &Disk{
		ID: "testdisk",
		FS: fakeFS,
	}

	// 4. Update snapshots
	spinnerProg := progress.NoopSpinnerProgressTracker{}
	stats, err := d.updateSnapshotsHelper(outbuffers, nil, spinnerProg)
	require.NoError(t, err)

	expectedEntries := testutil.CountExpectedEntries(fsConfig)
	assert.Equal(t, expectedEntries, stats.NewOrModified)
	assert.Equal(t, 0, stats.Unchanged)
	assert.Equal(t, 0, stats.Errors)

	// 5. Verify snapshots
	barProg := progress.NoopBarProgressTracker{}
	verifiedPath, err := d.verifySnapshots(outbuffers, barProg)
	require.NoError(t, err)
	require.NotEmpty(t, verifiedPath)

	// 6. Assert that the path is one of the expected snapshot paths
	assert.Contains(t, []string{buf1.Name(), buf2.Name()}, verifiedPath)

	// 7. Read back the verified snapshot and check its contents
	// Find the buffer that corresponds to the verified path
	var verifiedBuf buffers.CompressedBufferHandle
	for _, buf := range outbuffers {
		if buf.Name() == verifiedPath {
			verifiedBuf = buf
			break
		}
	}
	require.NotNil(t, verifiedBuf)

	reader, err := verifiedBuf.GetReader()
	require.NoError(t, err)
	defer reader.Close()

	snapReader, header, err := snapshot.NewSnapshotReader(reader)
	require.NoError(t, err)
	require.NotNil(t, snapReader)
	assert.Equal(t, uint32(Version), header.Version)

	// Count files in the snapshot and compare with the source filesystem
	count := 0
	for entry, err := range snapReader.Iter() {
		require.NoError(t, err)
		if entry != nil {
			count++
		}
	}
	assert.Equal(t, expectedEntries, count)
}

func TestDisk_VerifySnapshots_Mismatch(t *testing.T) {
	// 1. Create a Disk instance
	d := &Disk{ID: "testdisk"}

	// 2. Create two different snapshot buffers
	rawBuf1 := buffers.InMemoryBufferHandle([]byte("snapshot1"))
	compBuf1 := buffers.CreateCompressedHandle(rawBuf1)

	rawBuf2 := buffers.InMemoryBufferHandle([]byte("snapshot2"))
	compBuf2 := buffers.CreateCompressedHandle(rawBuf2)

	// 3. Verify snapshots
	barProg := progress.NoopBarProgressTracker{}
	_, err := d.verifySnapshots([]buffers.CompressedBufferHandle{compBuf1, compBuf2}, barProg)

	// 4. Assert that an error is returned
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

	// 2. Create a Disk instance for the prior snapshot
	priorDisk := &Disk{
		ID: "testdisk",
		FS: priorFS,
	}

	// 3. Create the prior snapshot in an in-memory buffer
	spinnerProg := progress.NoopSpinnerProgressTracker{}
	priorBuf := buffers.CreateCompressedHandle(buffers.InMemoryBufferHandle(nil))
	priorStats, err := priorDisk.updateSnapshotsHelper([]buffers.CompressedBufferHandle{priorBuf}, nil, spinnerProg)
	require.NoError(t, err)

	priorExpectedEntries := testutil.CountExpectedEntries(priorFsConfig)
	assert.Equal(t, priorExpectedEntries, priorStats.NewOrModified)
	assert.Equal(t, 0, priorStats.Unchanged)

	// 4. Create a new fake filesystem with more files
	updatedFsConfig := testutil.DefaultTreeConfig()
	updatedFsConfig.FilesPerDir = 10
	updatedFS := testutil.GenerateMapFS(updatedFsConfig)

	// 5. Create two in-memory buffers for the new snapshots
	buf1 := buffers.CreateCompressedHandle(buffers.InMemoryBufferHandle(nil))
	buf2 := buffers.CreateCompressedHandle(buffers.InMemoryBufferHandle(nil))
	outbuffers := []buffers.CompressedBufferHandle{buf1, buf2}

	// 6. Create a new Disk instance with the updated filesystem
	d := &Disk{
		ID: "testdisk",
		FS: updatedFS,
	}

	// 7. Update snapshots using the prior snapshot
	stats, err := d.updateSnapshotsHelper(outbuffers, priorBuf, spinnerProg)
	require.NoError(t, err)

	updatedExpectedEntries := testutil.CountExpectedEntries(updatedFsConfig)
	assert.Equal(t, updatedExpectedEntries-priorExpectedEntries, stats.NewOrModified)
	assert.Equal(t, priorExpectedEntries, stats.Unchanged)

	// 8. Verify snapshots
	barProg := progress.NoopBarProgressTracker{}
	verifiedPath, err := d.verifySnapshots(outbuffers, barProg)
	require.NoError(t, err)
	require.NotEmpty(t, verifiedPath)

	// 9. Assert that the path is one of the expected snapshot paths
	assert.Contains(t, []string{buf1.Name(), buf2.Name()}, verifiedPath)

	// 10. Read back the verified snapshot and check its contents
	var verifiedBuf buffers.CompressedBufferHandle
	for _, buf := range outbuffers {
		if buf.Name() == verifiedPath {
			verifiedBuf = buf
			break
		}
	}
	require.NotNil(t, verifiedBuf)

	reader, err := verifiedBuf.GetReader()
	require.NoError(t, err)
	defer reader.Close()

	snapReader, header, err := snapshot.NewSnapshotReader(reader)
	require.NoError(t, err)
	require.NotNil(t, snapReader)
	assert.Equal(t, uint32(Version), header.Version)

	// Count files in the snapshot and compare with the source filesystem
	count := 0
	for entry, err := range snapReader.Iter() {
		require.NoError(t, err)
		if entry != nil {
			count++
		}
	}
	assert.Equal(t, updatedExpectedEntries, count)
}