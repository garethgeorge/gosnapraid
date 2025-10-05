package snapshot

import (
	"bufio"
	"bytes"
	"os"
	"testing"

	"github.com/garethgeorge/gosnapraid/internal/hashing"
	"github.com/stretchr/testify/assert"
)

func TestEncodeDecodeNode(t *testing.T) {
	// Create a test node
	testNode := SnapshotNodeMetadata{
		Name:       "test_file.txt",
		Type:       SNAPSHOT_NODE_TYPE_FILE,
		Ino:        12345,
		Gen:        67890,
		Size:       1024,
		Mode:       os.FileMode(0644),
		AccessTime: 1609459300,
		ChangeTime: 1609459400,
		BirthTime:  1609459500,
		UID:        1000,
		GID:        1000,
		DeviceID:   2049,
		ContentHash: hashing.ContentHash{
			Lo: 0x1234567890abcdef,
			Hi: 0xfedcba0987654321,
		},
	}

	// Encode the node
	encodedBytes, err := encodeNode(testNode)
	if err != nil {
		t.Fatalf("Failed to encode node: %v", err)
	}

	// Decode the node
	decodedNode, err := decodeNode(encodedBytes)
	if err != nil {
		t.Fatalf("Failed to decode node: %v", err)
	}

	// Compare the nodes
	assert.Equal(t, testNode, decodedNode)
}

func TestBinarySnapshotWriter_SingleNode(t *testing.T) {
	var buf bytes.Buffer
	writer := NewBinarySnapshotWriter(bufio.NewWriter(&buf))

	node := createTestFile("file-a", 1)

	// Write the hierarchy
	assert.NoError(t, writer.WriteHeader())
	assert.NoError(t, writer.WriteNode(node))
	assert.NoError(t, writer.Close())

	// Read and verify the hierarchy
	reader := NewBinarySnapshotReader(bufio.NewReader(&buf))
	assert.NoError(t, reader.ReadHeader())

	err := reader.NextDirectory()
	assert.NoError(t, err)
	siblings := reader.Siblings()
	assert.Equal(t, 1, len(siblings))
	assert.Equal(t, "file-a", siblings[0].Name)

	// There should be no more directories.
	err = reader.NextDirectory()
	assert.Equal(t, ErrNoMoreNodes, err)
}

func TestBinarySnapshotWriter_ReadWrite(t *testing.T) {
	var buf bytes.Buffer
	writer := NewBinarySnapshotWriter(bufio.NewWriter(&buf))

	// Define a fake node hierarchy
	nodes := createTestNodes()

	// Write the hierarchy
	assert.NoError(t, writer.WriteHeader())
	writeNodesRecursive(t, writer, nodes)
	assert.NoError(t, writer.Close())

	// Read and verify the hierarchy
	reader := NewBinarySnapshotReader(bufio.NewReader(&buf))
	assert.NoError(t, reader.ReadHeader())

	// The reader processes directories in a depth-first manner.
	// So, the first directory we get is the children of "dir-b".
	err := reader.NextDirectory()
	assert.NoError(t, err)
	dirBChildren := reader.Siblings()
	assert.Equal(t, 1, len(dirBChildren))
	assert.Equal(t, "file-c", dirBChildren[0].Name)

	// The next directory is the root.
	err = reader.NextDirectory()
	assert.NoError(t, err)
	rootChildren := reader.Siblings()
	assert.Equal(t, 3, len(rootChildren))
	assert.Equal(t, "file-a", rootChildren[0].Name)
	assert.Equal(t, "dir-b", rootChildren[1].Name)
	assert.Equal(t, "file-d", rootChildren[2].Name)

	// There should be no more directories.
	err = reader.NextDirectory()
	assert.Equal(t, ErrNoMoreNodes, err)
}

type testNode struct {
	Metadata SnapshotNodeMetadata
	Children []testNode
}

func createTestNodes() []testNode {
	return []testNode{
		{Metadata: createTestFile("file-a", 1)},
		{
			Metadata: createTestDir("dir-b", 2),
			Children: []testNode{
				{Metadata: createTestFile("file-c", 3)},
			},
		},
		{Metadata: createTestFile("file-d", 4)},
	}
}

func createTestFile(name string, ino uint64) SnapshotNodeMetadata {
	return SnapshotNodeMetadata{
		Name: name,
		Type: SNAPSHOT_NODE_TYPE_FILE,
		Ino:  ino,
		Size: 1024,
		Mode: 0644,
	}
}

func createTestDir(name string, ino uint64) SnapshotNodeMetadata {
	return SnapshotNodeMetadata{
		Name: name,
		Type: SNAPSHOT_NODE_TYPE_DIR,
		Ino:  ino,
		Mode: 0755,
	}
}

func writeNodesRecursive(t *testing.T, writer SnapshotWriter, nodes []testNode) {
	for _, node := range nodes {
		assert.NoError(t, writer.WriteNode(node.Metadata))
		if node.Metadata.Type == SNAPSHOT_NODE_TYPE_DIR {
			assert.NoError(t, writer.BeginChildren())
			writeNodesRecursive(t, writer, node.Children)
			assert.NoError(t, writer.EndChildren())
		}
	}
}
