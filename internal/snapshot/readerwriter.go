package snapshot

import (
	"encoding/binary"
	"errors"
	"io"
	"iter"

	gosnapraidpb "github.com/garethgeorge/gosnapraid/proto/gosnapraid"
)

var (
	ErrNodeTooLarge = errors.New("snapshot node > 65535 bytes")
)

type SnapshotWriter struct {
	w   io.Writer
	buf []byte
}

func (sw *SnapshotWriter) writeHeader(header *gosnapraidpb.SnapshotHeader) error {
	size := header.SizeVT()
	if size >= 1<<16 {
		return ErrNodeTooLarge
	}
	if cap(sw.buf) < size {
		sw.buf = make([]byte, max(size, cap(sw.buf)*2))
	}
	n, err := header.MarshalToSizedBufferVT(sw.buf[:size])
	if err != nil {
		return err
	}
	var sizeBuf [2]byte
	binary.LittleEndian.PutUint16(sizeBuf[:], uint16(n))
	_, err = sw.w.Write(sizeBuf[:])
	if err != nil {
		return err
	}
	_, err = sw.w.Write(sw.buf[:n])
	if err != nil {
		return err
	}
	return nil
}

func (sw *SnapshotWriter) Write(metadata *gosnapraidpb.SnapshotNode) error {
	size := metadata.SizeVT()
	if size >= 1<<16 {
		return ErrNodeTooLarge
	}
	if cap(sw.buf) < size {
		sw.buf = make([]byte, max(size, cap(sw.buf)*2))
	}
	n, err := metadata.MarshalToSizedBufferVT(sw.buf[:size])
	if err != nil {
		return err
	}
	var sizeBuf [2]byte
	binary.LittleEndian.PutUint16(sizeBuf[:], uint16(n))
	_, err = sw.w.Write(sizeBuf[:])
	if err != nil {
		return err
	}
	_, err = sw.w.Write(sw.buf[:n])
	if err != nil {
		return err
	}
	return nil
}

func NewSnapshotWriter(w io.Writer, header *gosnapraidpb.SnapshotHeader) (*SnapshotWriter, error) {
	sw := &SnapshotWriter{
		w:   w,
		buf: make([]byte, 1024),
	}
	err := sw.writeHeader(header)
	if err != nil {
		return nil, err
	}
	return sw, nil
}

type SnapshotReader struct {
	r   io.Reader
	buf []byte
}

func (sr *SnapshotReader) readHeader() (*gosnapraidpb.SnapshotHeader, error) {
	var sizeBuf [2]byte
	_, err := io.ReadFull(sr.r, sizeBuf[:])
	if err != nil {
		return nil, err
	}
	size := binary.LittleEndian.Uint16(sizeBuf[:])
	if cap(sr.buf) < int(size) {
		sr.buf = make([]byte, max(int(size), cap(sr.buf)*2))
	}
	buf := sr.buf[:size]
	_, err = io.ReadFull(sr.r, buf)
	if err != nil {
		return nil, err
	}
	var header gosnapraidpb.SnapshotHeader
	err = header.UnmarshalVT(buf)
	if err != nil {
		return nil, err
	}
	return &header, nil
}

// Iter returns an iterator that yields references to SnapshotNodes.
// Nodes are allocated in batches of 1024 for better performance.
// The yielded nodes are only valid until the next iteration.
func (sr *SnapshotReader) Iter() iter.Seq2[*gosnapraidpb.SnapshotNode, error] {
	const batchSize = 1024

	return func(yield func(*gosnapraidpb.SnapshotNode, error) bool) {
		// Pre-allocate a batch of nodes
		nodes := make([]gosnapraidpb.SnapshotNode, batchSize)
		nodeIdx := 0

		for {
			// Get the current node from the batch
			node := &nodes[nodeIdx]

			// Read the size prefix
			var sizeBuf [2]byte
			_, err := io.ReadFull(sr.r, sizeBuf[:])
			if err != nil {
				if err == io.EOF {
					return // Normal end of stream
				}
				yield(nil, err)
				return
			}

			size := binary.LittleEndian.Uint16(sizeBuf[:])

			// Ensure buffer is large enough
			if cap(sr.buf) < int(size) {
				sr.buf = make([]byte, max(int(size), cap(sr.buf)*2))
			}
			buf := sr.buf[:size]

			// Read the message data
			_, err = io.ReadFull(sr.r, buf)
			if err != nil {
				yield(nil, err)
				return
			}

			// Unmarshal into the pre-allocated node
			err = node.UnmarshalVT(buf)
			if err != nil {
				yield(nil, err)
				return
			}

			// Yield the node reference
			if !yield(node, nil) {
				return // Consumer stopped iteration
			}

			// Move to next slot in batch, wrapping around
			nodeIdx = (nodeIdx + 1) % batchSize
		}
	}
}

func NewSnapshotReader(r io.Reader) (*SnapshotReader, *gosnapraidpb.SnapshotHeader, error) {
	sr := &SnapshotReader{
		r:   r,
		buf: make([]byte, 1024),
	}
	header, err := sr.readHeader()
	if err != nil {
		return nil, nil, err
	}
	return sr, header, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
