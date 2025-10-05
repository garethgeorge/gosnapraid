package snapshot

import (
	"bufio"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateSnapshotNoPrior(t *testing.T) {
	// 1. Setup a simple directory tree
	tmpDir, err := os.MkdirTemp("", "snapshot_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create a directory and some files
	createDirForTest(t, filepath.Join(tmpDir, "dir1"))
	createFileForTest(t, filepath.Join(tmpDir, "file1.txt"), "hello")
	createFileForTest(t, filepath.Join(tmpDir, "file2.txt"), "world")

	// 2. Create a SnapshotReader and a SnapshotWriter
	oldSnapshotReader := &EmptySnapshotReader{}
	var buf bytes.Buffer
	newSnapshotWriter := NewBinarySnapshotWriter(bufio.NewWriter(&buf))

	// 3. Call GenerateSnapshot
	err = GenerateSnapshot(tmpDir, oldSnapshotReader, newSnapshotWriter)
	require.NoError(t, newSnapshotWriter.Close())

	// 4. Assert that we can call GenerateSnapshot without error
	// and that the output buffer is non-empty
	require.NoError(t, err)
	assert.NotEmpty(t, buf.Len(), "output buffer should not be empty")
}

func TestGeneratedSnapshotCanBeRead(t *testing.T) {
	// 1. Setup a simple directory tree
	tmpDir, err := os.MkdirTemp("", "snapshot_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create a directory and some files
	createDirForTest(t, filepath.Join(tmpDir, "dir1"))
	createFileForTest(t, filepath.Join(tmpDir, "file1.txt"), "hello")
	createFileForTest(t, filepath.Join(tmpDir, "file2.txt"), "world")

	// 2. Create a SnapshotReader and a SnapshotWriter
	oldSnapshotReader := &EmptySnapshotReader{}
	var buf bytes.Buffer
	newSnapshotWriter := NewBinarySnapshotWriter(bufio.NewWriter(&buf))

	// 3. Call GenerateSnapshot
	err = GenerateSnapshot(tmpDir, oldSnapshotReader, newSnapshotWriter)
	require.NoError(t, newSnapshotWriter.Close())

	// 4. Assert that we can call GenerateSnapshot without error
	// and that the output buffer is non-empty
	require.NoError(t, err)
	assert.NotEmpty(t, buf.Len(), "output buffer should not be empty")

	// 5. Read the snapshot
	newSnapshotReader := NewBinarySnapshotReader(bufio.NewReader(bytes.NewReader(buf.Bytes())))

	// 6. Assert that we can read the snapshot without error
	require.NoError(t, newSnapshotReader.ReadHeader())
	gotSiblings := [][]string{}
	for {
		if err := newSnapshotReader.NextDirectory(); err != nil {
			if err == ErrNoMoreNodes {
				break
			}
			require.NoError(t, err)
		}

		names := make([]string, 0, len(newSnapshotReader.Siblings()))
		for _, sibling := range newSnapshotReader.Siblings() {
			names = append(names, sibling.Name)
		}
		gotSiblings = append(gotSiblings, names)
	}
	require.Equal(t, [][]string{[]string{}, {"dir1", "file1.txt", "file2.txt"}, {}}, gotSiblings)
}

func TestDeeplyNestedSnapshot(t *testing.T) {
	// 1. Setup a simple directory tree
	tmpDir, err := os.MkdirTemp("", "snapshot_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create a directory and some files
	createFileForTest(t, filepath.Join(tmpDir, "a/aa/aaa.txt"), "hello")
	createFileForTest(t, filepath.Join(tmpDir, "a/aa/aab.txt"), "world")
	createFileForTest(t, filepath.Join(tmpDir, "a/ab/aba.txt"), "hello")
	createFileForTest(t, filepath.Join(tmpDir, "a/ab/abb.txt"), "world")
	createFileForTest(t, filepath.Join(tmpDir, "b/ba/baa.txt"), "hello")
	createFileForTest(t, filepath.Join(tmpDir, "b/ba/bab.txt"), "world")
	createFileForTest(t, filepath.Join(tmpDir, "b/bb/bba.txt"), "hello")
	createFileForTest(t, filepath.Join(tmpDir, "b/bb/bbb.txt"), "world")

	// 2. Create a SnapshotReader and a SnapshotWriter
	oldSnapshotReader := &EmptySnapshotReader{}
	var buf bytes.Buffer
	newSnapshotWriter := NewBinarySnapshotWriter(bufio.NewWriter(&buf))

	// 3. Call GenerateSnapshot
	err = GenerateSnapshot(tmpDir, oldSnapshotReader, newSnapshotWriter)
	require.NoError(t, newSnapshotWriter.Close())

	// 4. Assert that we can call GenerateSnapshot without error
	// and that the output buffer is non-empty
	require.NoError(t, err)
	assert.NotEmpty(t, buf.Len(), "output buffer should not be empty")

	// 5. Read the snapshot
	newSnapshotReader := NewBinarySnapshotReader(bufio.NewReader(bytes.NewReader(buf.Bytes())))

	// 6. Assert that we can read the snapshot without error
	require.NoError(t, newSnapshotReader.ReadHeader())
	gotSiblings := [][]string{}
	for {
		if err := newSnapshotReader.NextDirectory(); err != nil {
			if err == ErrNoMoreNodes {
				break
			}
			require.NoError(t, err)
		}

		names := make([]string, 0, len(newSnapshotReader.Siblings()))
		for _, sibling := range newSnapshotReader.Siblings() {
			names = append(names, sibling.Name)
		}
		gotSiblings = append(gotSiblings, names)
	}
	require.Equal(t, [][]string{
		{"aaa.txt", "aab.txt"},
		{"aba.txt", "abb.txt"},
		{"aa", "ab"},
		{"baa.txt", "bab.txt"},
		{"bba.txt", "bbb.txt"},
		{"ba", "bb"},
		{"a", "b"},
		{},
	}, gotSiblings)
}

func createFileForTest(t *testing.T, path string, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		require.NoError(t, err)
	}
	err := os.WriteFile(path, []byte(content), 0644)
	require.NoError(t, err)
}

func createDirForTest(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0755); err != nil {
		require.NoError(t, err)
	}
}
