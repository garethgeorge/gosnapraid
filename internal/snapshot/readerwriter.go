package snapshot

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/garethgeorge/gosnapraid/internal/binencutil"
)

// encodeNode encodes a SnapshotNodeMetadata into bytes
func encodeNode(node SnapshotNodeMetadata) ([]byte, error) {
	if len(node.Name) > 4096 {
		// on most systems the actual limit is 256 bytes so this is plenty generous.
		return nil, fmt.Errorf("node name too long: %s", node.Name)
	}

	bytes := make([]byte, 0, 1024)
	bytes = append(bytes, byte(node.Type))
	bytes = binary.LittleEndian.AppendUint16(bytes, uint16(len(node.Name)))
	bytes = append(bytes, node.Name...)
	bytes = binary.LittleEndian.AppendUint64(bytes, node.Ino)
	bytes = binary.LittleEndian.AppendUint64(bytes, node.Gen)
	bytes = binary.LittleEndian.AppendUint64(bytes, node.Size)
	bytes = binary.LittleEndian.AppendUint32(bytes, uint32(node.Mode))
	bytes = binary.LittleEndian.AppendUint64(bytes, uint64(node.AccessTime))
	bytes = binary.LittleEndian.AppendUint64(bytes, uint64(node.ChangeTime))
	bytes = binary.LittleEndian.AppendUint64(bytes, uint64(node.BirthTime))
	bytes = binary.LittleEndian.AppendUint32(bytes, node.UID)
	bytes = binary.LittleEndian.AppendUint32(bytes, node.GID)
	bytes = binary.LittleEndian.AppendUint64(bytes, node.DeviceID)
	bytes = binary.LittleEndian.AppendUint64(bytes, node.ContentHash.Lo)
	bytes = binary.LittleEndian.AppendUint64(bytes, node.ContentHash.Hi)

	return bytes, nil
}

// decodeNode decodes bytes into a SnapshotNodeMetadata
func decodeNode(bytes []byte) (SnapshotNodeMetadata, error) {
	node := SnapshotNodeMetadata{}
	var err error
	var nodeType uint8
	nodeType, bytes, err = binencutil.BytesReadUint8(bytes)
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}
	node.Type = SnapshotNodeType(nodeType)
	var nameLen uint16
	nameLen, bytes, err = binencutil.BytesReadUint16(bytes)
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}
	if len(bytes) < int(nameLen) {
		return SnapshotNodeMetadata{}, fmt.Errorf("insufficient data for name")
	}
	node.Name = string(bytes[:nameLen])
	bytes = bytes[nameLen:]
	node.Ino, bytes, err = binencutil.BytesReadUint64(bytes)
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}
	node.Gen, bytes, err = binencutil.BytesReadUint64(bytes)
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}
	node.Size, bytes, err = binencutil.BytesReadUint64(bytes)
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}
	var mode uint32
	mode, bytes, err = binencutil.BytesReadUint32(bytes)
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}
	node.Mode = os.FileMode(mode)
	node.AccessTime, bytes, err = binencutil.BytesReadInt64(bytes)
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}
	node.ChangeTime, bytes, err = binencutil.BytesReadInt64(bytes)
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}
	node.BirthTime, bytes, err = binencutil.BytesReadInt64(bytes)
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}
	var uid uint32
	uid, bytes, err = binencutil.BytesReadUint32(bytes)
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}
	node.UID = uid
	var gid uint32
	gid, bytes, err = binencutil.BytesReadUint32(bytes)
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}
	node.GID = gid
	node.DeviceID, bytes, err = binencutil.BytesReadUint64(bytes)
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}
	node.ContentHash.Lo, bytes, err = binencutil.BytesReadUint64(bytes)
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}
	node.ContentHash.Hi, bytes, err = binencutil.BytesReadUint64(bytes)
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}
	return node, nil
}

const (
	version                      uint32 = 1
	controlSequenceMask          uint16 = 1 << 15
	controlSequenceStartChildren uint16 = controlSequenceMask | 1
	controlSequenceEndChildren   uint16 = controlSequenceMask | 2
)

type SnapshotWriter interface {
	WriteHeader() error
	WriteNode(node SnapshotNodeMetadata) error
	BeginChildren() error
	EndChildren() error
	Close() error
}

type BinarySnapshotWriter struct {
	writer *bufio.Writer
}

func NewBinarySnapshotWriter(writer *bufio.Writer) *BinarySnapshotWriter {
	return &BinarySnapshotWriter{
		writer: writer,
	}
}

func (w *BinarySnapshotWriter) WriteHeader() error {
	if err := binencutil.WriteUint32(w.writer, version); err != nil {
		return err
	}
	return nil
}

func (w *BinarySnapshotWriter) WriteNode(node SnapshotNodeMetadata) error {
	bytes, err := encodeNode(node)
	if err != nil {
		return err
	}

	if uint16(len(bytes))&controlSequenceMask != 0 {
		return fmt.Errorf("node data too long: %d bytes", len(bytes))
	}

	// Write the node data length to the writer
	if err := binencutil.WriteUint16(w.writer, uint16(len(bytes))); err != nil {
		return err
	}
	// Write the actual data to the writer
	if _, err := w.writer.Write(bytes); err != nil {
		return err
	}
	return nil
}

func (w *BinarySnapshotWriter) BeginChildren() error {
	if err := binencutil.WriteUint16(w.writer, uint16(controlSequenceStartChildren)); err != nil {
		return err
	}
	return nil
}

func (w *BinarySnapshotWriter) EndChildren() error {
	if err := binencutil.WriteUint16(w.writer, uint16(controlSequenceEndChildren)); err != nil {
		return err
	}
	return nil
}

func (w *BinarySnapshotWriter) Close() error {
	if err := w.EndChildren(); err != nil {
		return err
	}
	return w.writer.Flush()
}

type SnapshotReader interface {
	ReadHeader() error

	// NextDirectory moves the reader into the next directory
	NextDirectory() error

	// Gets the parents of the current batch of nodes including the directory we are currently in (at the end of the list)
	Parents() []SnapshotNodeMetadata

	// Gets the current batch of nodes
	Siblings() []SnapshotNodeMetadata
}

// BinarySnapshotReader is a SnapshotReader that reads from a binary snapshot file
type BinarySnapshotReader struct {
	reader      *bufio.Reader
	partialDirs [][]SnapshotNodeMetadata
	currentDir  []SnapshotNodeMetadata
}

var _ SnapshotReader = (*BinarySnapshotReader)(nil)

func NewBinarySnapshotReader(reader *bufio.Reader) *BinarySnapshotReader {
	return &BinarySnapshotReader{
		reader:      reader,
		partialDirs: [][]SnapshotNodeMetadata{{}}, // Start with an empty directory
	}
}

func (r *BinarySnapshotReader) ReadHeader() error {
	v, err := binencutil.ReadUint32(r.reader)
	if err != nil {
		return err
	}
	if v != version {
		return fmt.Errorf("unknown version: %d", v)
	}
	return nil
}

func (r *BinarySnapshotReader) Parents() []SnapshotNodeMetadata {
	// The parent is the last directory in the partials list at each level
	parents := make([]SnapshotNodeMetadata, len(r.partialDirs))
	for i := range r.partialDirs {
		parents[i] = r.partialDirs[i][len(r.partialDirs[i])-1]
	}
	return parents
}

func (r *BinarySnapshotReader) Siblings() []SnapshotNodeMetadata {
	return r.currentDir
}

func (r *BinarySnapshotReader) NextDirectory() error {
	// Loop over control sequences until we find an end children sequence
	for len(r.partialDirs) > 0 {
		controlSequence, err := binencutil.ReadUint16(r.reader)
		if err != nil {
			return err
		}
		if controlSequence&controlSequenceMask != 0 {
			switch controlSequence {
			case controlSequenceEndChildren:
				// We have found the end of a directory, allow the caller to process it
				r.currentDir = r.partialDirs[len(r.partialDirs)-1]
				r.partialDirs = r.partialDirs[:len(r.partialDirs)-1]
				return nil
			case controlSequenceStartChildren:
				// We have found a new directory, add it to the list of partial directories.
				r.partialDirs = append(r.partialDirs, []SnapshotNodeMetadata{})
			default:
				return fmt.Errorf("unknown control sequence: %d", controlSequence)
			}
		} else {
			node, err := r.deserializeNode(controlSequence)
			if err != nil {
				return err
			}
			r.partialDirs[len(r.partialDirs)-1] = append(r.partialDirs[len(r.partialDirs)-1], node)
		}
	}
	return ErrNoMoreNodes
}

func (r *BinarySnapshotReader) deserializeNode(length uint16) (SnapshotNodeMetadata, error) {
	bytes := make([]byte, length)
	if _, err := r.reader.Read(bytes); err != nil {
		return SnapshotNodeMetadata{}, err
	}
	return decodeNode(bytes)
}

// EmptySnapshotReader is a SnapshotReader that always returns no more nodes
type EmptySnapshotReader struct {
}

var _ SnapshotReader = (*EmptySnapshotReader)(nil)

func (r *EmptySnapshotReader) ReadHeader() error {
	return nil
}

func (r *EmptySnapshotReader) NextDirectory() error {
	return ErrNoMoreNodes
}

func (r *EmptySnapshotReader) Parents() []SnapshotNodeMetadata {
	return nil
}

func (r *EmptySnapshotReader) Siblings() []SnapshotNodeMetadata {
	return nil
}
