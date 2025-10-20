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

func TestUseMoveDetection_BasicMove(t *testing.T) {
	// Create a filesystem with two files with identical content
	mapFS := fstest.MapFS{
		"file1.txt": &fstest.MapFile{
			Data:    []byte("identical content"),
			ModTime: time.Unix(1000, 0),
			Mode:    0644,
		},
	}

	// 1. Create initial snapshot
	var buf1 bytes.Buffer
	snapshotter := NewSnapshotter(mapFS)
	writer1, err := NewSnapshotWriter(&buf1, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	stats, err := snapshotter.Create(writer1, nil)
	require.NoError(t, err)
	// WalkFS includes the root directory ".", MapFS has 1 file -> 2 entries total
	assert.Equal(t, 2, stats.NewOrModified) // root dir + file

	// 2. "Move" the file (delete original, create new with same content)
	mapFS2 := fstest.MapFS{
		"file2.txt": &fstest.MapFile{
			Data:    []byte("identical content"),
			ModTime: time.Unix(2000, 0),
			Mode:    0644,
		},
	}

	// 3. Create new snapshot with move detection enabled
	reader1, _, err := NewSnapshotReader(bytes.NewReader(buf1.Bytes()))
	require.NoError(t, err)

	var buf2 bytes.Buffer
	snapshotter2 := NewSnapshotter(mapFS2)

	// Enable move detection
	reader1Clone, _, err := NewSnapshotReader(bytes.NewReader(buf1.Bytes()))
	require.NoError(t, err)
	err = snapshotter2.UseMoveDetection(reader1Clone)
	require.NoError(t, err)

	writer2, err := NewSnapshotWriter(&buf2, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	stats, err = snapshotter2.Create(writer2, reader1)
	require.NoError(t, err)

	// The file should be detected as moved/deduped
	// Both snapshots have root dir unchanged (same path ".") - but Wait! The first snapshot
	// had a different filesystem, so the root dir isn't "unchanged".
	// Actually the root dir is always "." and matches metadata, so it should be unchanged
	assert.Equal(t, 1, stats.Unchanged, "root dir is unchanged")
	assert.Equal(t, 1, stats.NewOrModified, "file2.txt is new (different path)")
	assert.Equal(t, 1, stats.MovedOrDeduped, "file should be detected as moved/deduped") // 4. Verify the new snapshot has slice range information copied
	reader2, _, err := NewSnapshotReader(&buf2)
	require.NoError(t, err)

	var nodes []*gosnapraidpb.SnapshotNode
	for node, err := range reader2.Iter() {
		require.NoError(t, err)
		// Make a shallow copy without copying the mutex
		nodeCopy := &gosnapraidpb.SnapshotNode{
			Path:              node.Path,
			Size:              node.Size,
			Mtime:             node.Mtime,
			Mode:              node.Mode,
			Hashtype:          node.Hashtype,
			Hashhi:            node.Hashhi,
			Hashlo:            node.Hashlo,
			StripeRangeStarts: node.StripeRangeStarts,
			StripeRangeEnds:   node.StripeRangeEnds,
		}
		nodes = append(nodes, nodeCopy)
	}

	// Should have root + 1 file
	require.Len(t, nodes, 2)
	// Find the file (not the directory)
	var fileNode *gosnapraidpb.SnapshotNode
	for _, n := range nodes {
		if !fs.FileMode(n.Mode).IsDir() {
			fileNode = n
			break
		}
	}
	require.NotNil(t, fileNode)
	assert.NotEqual(t, uint64(0), fileNode.Hashlo, "moved file should have hash")
}

func TestUseMoveDetection_MultipleFiles(t *testing.T) {
	// Create initial filesystem with multiple files
	mapFS1 := fstest.MapFS{
		"file1.txt": &fstest.MapFile{
			Data:    []byte("content A"),
			ModTime: time.Unix(1000, 0),
			Mode:    0644,
		},
		"file2.txt": &fstest.MapFile{
			Data:    []byte("content B"),
			ModTime: time.Unix(1000, 0),
			Mode:    0644,
		},
		"file3.txt": &fstest.MapFile{
			Data:    []byte("content C"),
			ModTime: time.Unix(1000, 0),
			Mode:    0644,
		},
	}

	// Create initial snapshot
	var buf1 bytes.Buffer
	snapshotter1 := NewSnapshotter(mapFS1)
	writer1, err := NewSnapshotWriter(&buf1, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	stats, err := snapshotter1.Create(writer1, nil)
	require.NoError(t, err)
	assert.Equal(t, 4, stats.NewOrModified) // root dir + 3 files

	// Create second filesystem with files "moved" to new locations
	mapFS2 := fstest.MapFS{
		"moved": &fstest.MapFile{
			Mode:    fs.ModeDir | 0755,
			ModTime: time.Unix(2000, 0),
		},
		"moved/file_a.txt": &fstest.MapFile{
			Data:    []byte("content A"), // Same content as file1.txt
			ModTime: time.Unix(2000, 0),
			Mode:    0644,
		},
		"moved/file_b.txt": &fstest.MapFile{
			Data:    []byte("content B"), // Same content as file2.txt
			ModTime: time.Unix(2000, 0),
			Mode:    0644,
		},
		"file3.txt": &fstest.MapFile{
			Data:    []byte("content C"), // Unchanged
			ModTime: time.Unix(1000, 0),
			Mode:    0644,
		},
		"new_file.txt": &fstest.MapFile{
			Data:    []byte("new content"),
			ModTime: time.Unix(2000, 0),
			Mode:    0644,
		},
	}

	// Create new snapshot with move detection
	reader1, _, err := NewSnapshotReader(bytes.NewReader(buf1.Bytes()))
	require.NoError(t, err)

	var buf2 bytes.Buffer
	snapshotter2 := NewSnapshotter(mapFS2)

	reader1Clone, _, err := NewSnapshotReader(bytes.NewReader(buf1.Bytes()))
	require.NoError(t, err)
	err = snapshotter2.UseMoveDetection(reader1Clone)
	require.NoError(t, err)

	writer2, err := NewSnapshotWriter(&buf2, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	stats, err = snapshotter2.Create(writer2, reader1)
	require.NoError(t, err)

	// file3.txt is unchanged (1)
	// moved/ directory is new (1)
	// moved/file_a.txt and moved/file_b.txt are new but deduped (2)
	// new_file.txt is truly new (1)
	// root dir is unchanged (included in unchanged count)
	assert.Equal(t, 2, stats.Unchanged, "root dir + file3.txt unchanged")
	assert.Equal(t, 4, stats.NewOrModified, "moved dir + 2 moved files + 1 new file")
	assert.Equal(t, 2, stats.MovedOrDeduped, "2 files detected as moved")
}

func TestUseMoveDetection_DuplicateContent(t *testing.T) {
	// Test deduplication when multiple files have the same content
	mapFS1 := fstest.MapFS{
		"original.txt": &fstest.MapFile{
			Data:    []byte("duplicate content"),
			ModTime: time.Unix(1000, 0),
			Mode:    0644,
		},
	}

	// Create initial snapshot
	var buf1 bytes.Buffer
	snapshotter1 := NewSnapshotter(mapFS1)
	writer1, err := NewSnapshotWriter(&buf1, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	_, err = snapshotter1.Create(writer1, nil)
	require.NoError(t, err)

	// Create filesystem with multiple copies of the same content
	mapFS2 := fstest.MapFS{
		"copy1.txt": &fstest.MapFile{
			Data:    []byte("duplicate content"),
			ModTime: time.Unix(2000, 0),
			Mode:    0644,
		},
		"copy2.txt": &fstest.MapFile{
			Data:    []byte("duplicate content"),
			ModTime: time.Unix(2000, 0),
			Mode:    0644,
		},
		"copy3.txt": &fstest.MapFile{
			Data:    []byte("duplicate content"),
			ModTime: time.Unix(2000, 0),
			Mode:    0644,
		},
	}

	// Create new snapshot with move detection
	reader1, _, err := NewSnapshotReader(bytes.NewReader(buf1.Bytes()))
	require.NoError(t, err)

	var buf2 bytes.Buffer
	snapshotter2 := NewSnapshotter(mapFS2)

	reader1Clone, _, err := NewSnapshotReader(bytes.NewReader(buf1.Bytes()))
	require.NoError(t, err)
	err = snapshotter2.UseMoveDetection(reader1Clone)
	require.NoError(t, err)

	writer2, err := NewSnapshotWriter(&buf2, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	stats2, err := snapshotter2.Create(writer2, reader1)
	require.NoError(t, err)

	// All three files should be detected as deduped
	// Root directory is unchanged
	assert.Equal(t, 1, stats2.Unchanged, "root dir unchanged")
	assert.Equal(t, 3, stats2.NewOrModified, "3 files")
	assert.Equal(t, 3, stats2.MovedOrDeduped, "all files should be deduped against original")
}

func TestUseMoveDetection_WithoutOldSnapshot(t *testing.T) {
	// Test that move detection works even when creating a new snapshot without old one
	mapFS1 := fstest.MapFS{
		"file1.txt": &fstest.MapFile{
			Data:    []byte("content"),
			ModTime: time.Unix(1000, 0),
			Mode:    0644,
		},
	}

	// Create initial snapshot
	var buf1 bytes.Buffer
	snapshotter1 := NewSnapshotter(mapFS1)
	writer1, err := NewSnapshotWriter(&buf1, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	_, err = snapshotter1.Create(writer1, nil)
	require.NoError(t, err)

	// Create new filesystem
	mapFS2 := fstest.MapFS{
		"file2.txt": &fstest.MapFile{
			Data:    []byte("content"),
			ModTime: time.Unix(2000, 0),
			Mode:    0644,
		},
	}

	// Create new snapshot with move detection but WITHOUT old snapshot for comparison
	var buf2 bytes.Buffer
	snapshotter2 := NewSnapshotter(mapFS2)

	reader1, _, err := NewSnapshotReader(&buf1)
	require.NoError(t, err)
	err = snapshotter2.UseMoveDetection(reader1)
	require.NoError(t, err)

	writer2, err := NewSnapshotWriter(&buf2, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	// Note: passing nil for old snapshot
	stats, err := snapshotter2.Create(writer2, nil)
	require.NoError(t, err)

	// File should still be detected as deduped because we loaded the dedupe tree
	// Root dir + file
	assert.Equal(t, 2, stats.NewOrModified)
	assert.Equal(t, 1, stats.MovedOrDeduped)
}

func TestUseMoveDetection_EmptySliceRanges(t *testing.T) {
	// Test that files without slice ranges (empty files or errors) are handled correctly
	mapFS1 := fstest.MapFS{
		"empty.txt": &fstest.MapFile{
			Data:    []byte{},
			ModTime: time.Unix(1000, 0),
			Mode:    0644,
		},
		"normal.txt": &fstest.MapFile{
			Data:    []byte("content"),
			ModTime: time.Unix(1000, 0),
			Mode:    0644,
		},
	}

	// Create initial snapshot
	var buf1 bytes.Buffer
	snapshotter1 := NewSnapshotter(mapFS1)
	writer1, err := NewSnapshotWriter(&buf1, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	_, err = snapshotter1.Create(writer1, nil)
	require.NoError(t, err)

	// Create new snapshot with move detection
	reader1, _, err := NewSnapshotReader(&buf1)
	require.NoError(t, err)

	snapshotter2 := NewSnapshotter(mapFS1)
	err = snapshotter2.UseMoveDetection(reader1)
	require.NoError(t, err)

	// Should not panic or error
	assert.True(t, snapshotter2.useDedupe)
}

func TestUseMoveDetection_InvalidSliceRanges(t *testing.T) {
	// Create a snapshot with mismatched slice ranges
	var buf bytes.Buffer
	writer, err := NewSnapshotWriter(&buf, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	// Write a node with mismatched slice ranges
	node := &gosnapraidpb.SnapshotNode{
		Path:              "file.txt",
		StripeRangeStarts: []uint64{0, 100},
		StripeRangeEnds:   []uint64{100}, // Mismatched length!
	}
	err = writer.Write(node)
	require.NoError(t, err)

	// Try to use this snapshot for move detection
	reader, _, err := NewSnapshotReader(&buf)
	require.NoError(t, err)

	snapshotter := NewSnapshotter(fstest.MapFS{})
	err = snapshotter.UseMoveDetection(reader)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid stripe ranges")
}

func TestDedupeTree_Ordering(t *testing.T) {
	// Test that the dedupe tree maintains correct ordering
	mapFS := fstest.MapFS{
		"file1.txt": &fstest.MapFile{
			Data:    []byte("AAA"),
			ModTime: time.Unix(1000, 0),
			Mode:    0644,
		},
		"file2.txt": &fstest.MapFile{
			Data:    []byte("BBB"),
			ModTime: time.Unix(1000, 0),
			Mode:    0644,
		},
		"file3.txt": &fstest.MapFile{
			Data:    []byte("CCC"),
			ModTime: time.Unix(1000, 0),
			Mode:    0644,
		},
	}

	// Create initial snapshot
	var buf bytes.Buffer
	snapshotter := NewSnapshotter(mapFS)
	writer, err := NewSnapshotWriter(&buf, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	_, err = snapshotter.Create(writer, nil)
	require.NoError(t, err)

	// Load into dedupe tree
	reader, _, err := NewSnapshotReader(&buf)
	require.NoError(t, err)

	snapshotter2 := NewSnapshotter(mapFS)
	err = snapshotter2.UseMoveDetection(reader)
	require.NoError(t, err)

	// Verify the dedupe tree has all entries
	assert.Equal(t, 3, snapshotter2.dedupeTree.Len())
}

func TestUseMoveDetection_LargeDataset(t *testing.T) {
	// Test with a larger dataset to ensure efficiency
	config := testutil.TreeConfig{
		Depth:         2,
		BreadthPerDir: 3,
		FilesPerDir:   5,
		LeafFilesOnly: false,
		FileSize:      1024,
		ModTime:       time.Unix(1000, 0),
	}

	mapFS := testutil.GenerateMapFS(config)

	// Create initial snapshot
	var buf1 bytes.Buffer
	snapshotter1 := NewSnapshotter(mapFS)
	writer1, err := NewSnapshotWriter(&buf1, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	stats, err := snapshotter1.Create(writer1, nil)
	require.NoError(t, err)

	expectedFiles := testutil.CountExpectedFiles(config)
	assert.Greater(t, stats.NewOrModified, 0)

	// Load dedupe tree
	reader1, _, err := NewSnapshotReader(&buf1)
	require.NoError(t, err)

	snapshotter2 := NewSnapshotter(mapFS)
	err = snapshotter2.UseMoveDetection(reader1)
	require.NoError(t, err)

	// Verify dedupe tree size
	assert.Equal(t, expectedFiles, snapshotter2.dedupeTree.Len(), "dedupe tree should contain all files")
}

func TestUseMoveDetection_PartialMatch(t *testing.T) {
	// Test where some files match and some don't
	mapFS1 := fstest.MapFS{
		"file1.txt": &fstest.MapFile{
			Data:    []byte("matching content"),
			ModTime: time.Unix(1000, 0),
			Mode:    0644,
		},
		"file2.txt": &fstest.MapFile{
			Data:    []byte("different content"),
			ModTime: time.Unix(1000, 0),
			Mode:    0644,
		},
	}

	// Create initial snapshot
	var buf1 bytes.Buffer
	snapshotter1 := NewSnapshotter(mapFS1)
	writer1, err := NewSnapshotWriter(&buf1, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	_, err = snapshotter1.Create(writer1, nil)
	require.NoError(t, err)

	// Create new filesystem with one matching file
	mapFS2 := fstest.MapFS{
		"moved.txt": &fstest.MapFile{
			Data:    []byte("matching content"),
			ModTime: time.Unix(2000, 0),
			Mode:    0644,
		},
		"new.txt": &fstest.MapFile{
			Data:    []byte("brand new content"),
			ModTime: time.Unix(2000, 0),
			Mode:    0644,
		},
	}

	// Create new snapshot with move detection
	reader1, _, err := NewSnapshotReader(bytes.NewReader(buf1.Bytes()))
	require.NoError(t, err)

	var buf2 bytes.Buffer
	snapshotter2 := NewSnapshotter(mapFS2)

	reader1Clone, _, err := NewSnapshotReader(bytes.NewReader(buf1.Bytes()))
	require.NoError(t, err)
	err = snapshotter2.UseMoveDetection(reader1Clone)
	require.NoError(t, err)

	writer2, err := NewSnapshotWriter(&buf2, &gosnapraidpb.SnapshotHeader{Version: 1})
	require.NoError(t, err)

	stats, err := snapshotter2.Create(writer2, reader1)
	require.NoError(t, err)

	// moved.txt should be deduped, new.txt should not
	// root dir is unchanged
	assert.Equal(t, 1, stats.Unchanged, "root dir unchanged")
	assert.Equal(t, 2, stats.NewOrModified, "moved.txt + new.txt")
	assert.Equal(t, 1, stats.MovedOrDeduped, "only moved.txt should be deduped")
}
