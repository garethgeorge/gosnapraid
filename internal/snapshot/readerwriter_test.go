package snapshot

import (
	"bytes"
	"io"
	"testing"
	"time"

	gosnapraidpb "github.com/garethgeorge/gosnapraid/proto/gosnapraid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshotReadWrite(t *testing.T) {
	var buf bytes.Buffer

	header := &gosnapraidpb.SnapshotHeader{
		Version:   1,
		Timestamp: uint64(time.Now().UnixNano()),
	}

	writer, err := NewSnapshotWriter(&buf, header)
	require.NoError(t, err)

	nodes := []*gosnapraidpb.SnapshotNode{
		{
			Path:  "file1.txt",
			Size:  1024,
			Mtime: uint64(time.Now().UnixNano()),
		},
		{
			Path:  "file2.txt",
			Size:  2048,
			Mtime: uint64(time.Now().UnixNano()),
		},
	}

	for _, node := range nodes {
		err := writer.Write(node)
		require.NoError(t, err)
	}

	reader, readHeader, err := NewSnapshotReader(&buf)
	require.NoError(t, err)

	assert.Equal(t, header.Version, readHeader.Version)
	assert.InDelta(t, header.Timestamp, readHeader.Timestamp, 1000)

	i := 0
	for readNode, err := range reader.Iter() {
		require.NoError(t, err, "node %d", i)
		expectedNode := nodes[i]
		assert.Equal(t, expectedNode.Path, readNode.Path, "node %d", i)
		assert.Equal(t, expectedNode.Size, readNode.Size, "node %d", i)
		assert.InDelta(t, expectedNode.Mtime, readNode.Mtime, 1000, "node %d", i)
		i++
	}
}

func FuzzSnapshotNodeRoundtrip(f *testing.F) {
	f.Add("file.txt", uint64(1024), uint64(time.Now().UnixNano()), uint32(0))
	f.Fuzz(func(t *testing.T, path string, size uint64, mtime uint64, flags uint32) {
		var buf bytes.Buffer

		header := &gosnapraidpb.SnapshotHeader{
			Version:   1,
			Timestamp: uint64(time.Now().UnixNano()),
		}

		writer, err := NewSnapshotWriter(&buf, header)
		require.NoError(t, err)

		node := &gosnapraidpb.SnapshotNode{
			Path:  path,
			Size:  size,
			Mtime: mtime,
			Flags: flags,
		}

		err = writer.Write(node)
		// Writing a node with a very large path can result in a node that is too large to be written.
		// This is expected, so we just return in that case.
		if err == ErrNodeTooLarge {
			return
		}
		require.NoError(t, err)

		reader, _, err := NewSnapshotReader(&buf)
		require.NoError(t, err)

		for readNode, err := range reader.Iter() {
			require.NoError(t, err)
			assert.Equal(t, node.Path, readNode.Path)
			assert.Equal(t, node.Size, readNode.Size)
			assert.Equal(t, node.Mtime, readNode.Mtime)
			assert.Equal(t, node.Flags, readNode.Flags)
		}
	})
}

func FuzzCorruptedData(f *testing.F) {
	// Seed with a valid snapshot
	var validBuf bytes.Buffer
	header := &gosnapraidpb.SnapshotHeader{Version: 1, Timestamp: 12345}
	writer, _ := NewSnapshotWriter(&validBuf, header)
	writer.Write(&gosnapraidpb.SnapshotNode{Path: "file1", Size: 10})
	writer.Write(&gosnapraidpb.SnapshotNode{Path: "file2", Size: 20})
	f.Add(validBuf.Bytes())

	// Seed with some interesting corrupted values
	f.Add([]byte{0x00})                   // single byte
	f.Add([]byte{0xff, 0xff, 0xff, 0xff}) // large size

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bytes.NewReader(data)
		reader, _, err := NewSnapshotReader(r)
		if err != nil {
			// If header parsing fails, that's fine
			return
		}

		// Try to read all nodes from the potentially corrupted stream.
		// The reader should not panic.
		for _, err := range reader.Iter() {
			if err != nil {
				// We expect errors, so we just break
				break
			}
		}
	})
}

// mockErrorReadWriter is an io.ReadWriter that returns an error at a specified point.

type mockErrorReadWriter struct {
	data        bytes.Buffer
	readErrPos  int
	writeErrPos int
	readCount   int
	writeCount  int
}

func (m *mockErrorReadWriter) Read(p []byte) (n int, err error) {
	if m.readErrPos >= 0 && m.readCount >= m.readErrPos {
		return 0, io.ErrUnexpectedEOF
	}
	n, err = m.data.Read(p)
	m.readCount++
	return
}

func (m *mockErrorReadWriter) Write(p []byte) (n int, err error) {
	if m.writeErrPos >= 0 && m.writeCount >= m.writeErrPos {
		return 0, io.ErrShortWrite
	}
	n, err = m.data.Write(p)
	m.writeCount++
	return
}

func FuzzIOErrors(f *testing.F) {
	f.Add(int(0), int(-1)) // error on first read
	f.Add(int(-1), int(0)) // error on first write
	f.Add(int(1), int(1))  // error on second read/write

	f.Fuzz(func(t *testing.T, readErrPos int, writeErrPos int) {
		// Test write errors
		mockWriter := &mockErrorReadWriter{writeErrPos: writeErrPos}
		header := &gosnapraidpb.SnapshotHeader{Version: 1, Timestamp: 12345}
		writer, err := NewSnapshotWriter(mockWriter, header)
		if err != nil {
			// error could happen in the header write
			return
		}
		err = writer.Write(&gosnapraidpb.SnapshotNode{Path: "file1", Size: 10})
		if err != nil {
			// error could happen in the node write
			return
		}

		// Test read errors
		mockReader := &mockErrorReadWriter{readErrPos: readErrPos}
		// copy the valid data written to the mock writer
		mockReader.data.Write(mockWriter.data.Bytes())

		reader, _, err := NewSnapshotReader(mockReader)
		if err != nil {
			return
		}
		for _, err := range reader.Iter() {
			if err != nil {
				return
			}
		}
	})
}
