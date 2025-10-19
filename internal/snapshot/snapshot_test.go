package snapshot

import (
	"bytes"
	"io/fs"
	"testing"
	"testing/fstest"
	"time"

	"github.com/garethgeorge/gosnapraid/internal/snapshot/fsscan"
	gosnapraidpb "github.com/garethgeorge/gosnapraid/proto/gosnapraid"

	"github.com/garethgeorge/gosnapraid/internal/snapshot/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateAndReadSnapshot(t *testing.T) {
	// 1. Generate a filesystem in memory
	config := testutil.DefaultTreeConfig()
	mapFS := testutil.GenerateMapFS(config)

	// 2. Create a snapshot into a buffer
	var buf bytes.Buffer
	snapshotter := NewSnapshotter(mapFS)
	writer, err := NewSnapshotWriter(&buf, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	stats, err := snapshotter.Create(writer, nil)
	require.NoError(t, err)

	expectedEntries := testutil.CountExpectedEntries(config)
	assert.Equal(t, expectedEntries, stats.NewOrModified)
	assert.Equal(t, 0, stats.Unchanged)
	assert.Equal(t, 0, stats.Errors)

	// 3. Read the snapshot back
	reader, _, err := NewSnapshotReader(&buf)
	require.NoError(t, err)

	// 4. Verify the contents
	var fileCount, dirCount int
	for node, err := range reader.Iter() {
		require.NoError(t, err)
		if fs.FileMode(node.Mode).IsDir() {
			dirCount++
		} else {
			fileCount++
		}
	}

	expectedFiles := testutil.CountExpectedFiles(config)
	expectedDirs := testutil.CountExpectedDirs(config)

	assert.Equal(t, expectedFiles, fileCount, "mismatch in number of files")
	assert.Equal(t, expectedDirs, dirCount, "mismatch in number of directories")
}

func TestIncrementalSnapshot(t *testing.T) {
	// 1. Generate the initial filesystem state
	config := testutil.DefaultTreeConfig()
	mapFS := testutil.GenerateMapFS(config)

	// 2. Create a "prior" snapshot with fake hashes
	var priorBuf bytes.Buffer
	priorWriter, err := NewSnapshotWriter(&priorBuf, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	for file := range fsscan.WalkFS(mapFS) {
		node := &gosnapraidpb.SnapshotNode{
			Path:  file.Path,
			Size:  uint64(file.Size),
			Mtime: uint64(file.Mtime),
			Mode:  uint32(file.Mode),
		}
		if !fs.FileMode(node.Mode).IsDir() {
			node.Hashtype = gosnapraidpb.HashType_HASH_XXHASH64
			node.Hashlo = 12345 // Fake hash
		}
		err = priorWriter.Write(node)
		require.NoError(t, err)
	}

	// 3. Modify the filesystem
	const unchangedFile = "file0.txt"
	const modifiedMtimeFile = "dir0/file0.txt"
	const modifiedSizeFile = "dir0/file1.txt"
	const deletedFile = "file1.txt"
	const newFile = "newfile.txt"

	// Modify mtime
	mapFS[modifiedMtimeFile].ModTime = time.Now()

	// Modify size
	mapFS[modifiedSizeFile].Data = []byte("new data")

	// Delete file
	delete(mapFS, deletedFile)

	// Add file
	mapFS[newFile] = &fstest.MapFile{
		Data:    []byte("new file data"),
		ModTime: time.Now(),
	}

	// 4. Create the new (incremental) snapshot
	priorReader, _, err := NewSnapshotReader(&priorBuf)
	require.NoError(t, err)

	var newBuf bytes.Buffer
	newWriter, err := NewSnapshotWriter(&newBuf, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	snapshotter := NewSnapshotter(mapFS)
	stats, err := snapshotter.Create(newWriter, priorReader)
	require.NoError(t, err)

	expectedEntries := testutil.CountExpectedEntries(config)
	assert.Equal(t, 3, stats.NewOrModified)
	assert.Equal(t, expectedEntries-3, stats.Unchanged)
	assert.Equal(t, 0, stats.Errors)

	// 5. Verify the new snapshot
	newReader, _, err := NewSnapshotReader(&newBuf)
	require.NoError(t, err)

	newNodes := make(map[string]*gosnapraidpb.SnapshotNode)
	for node, err := range newReader.Iter() {
		require.NoError(t, err)
		// Make a copy of the node because the iterator reuses the memory
		nodeCopy := *node
		newNodes[node.Path] = &nodeCopy
	}

	// Verify unchanged file
	assert.Equal(t, uint64(12345), newNodes[unchangedFile].Hashlo, "unchanged file should have fake hash")

	// Verify modified mtime file
	assert.NotEqual(t, uint64(12345), newNodes[modifiedMtimeFile].Hashlo, "modified mtime file should have new hash")

	// Verify modified size file
	assert.NotEqual(t, uint64(12345), newNodes[modifiedSizeFile].Hashlo, "modified size file should have new hash")

	// Verify new file
	assert.Contains(t, newNodes, newFile, "new file should be in snapshot")
	assert.NotEqual(t, uint64(0), newNodes[newFile].Hashlo, "new file should have a computed hash")

	// Verify deleted file
	assert.NotContains(t, newNodes, deletedFile, "deleted file should not be in snapshot")
}
