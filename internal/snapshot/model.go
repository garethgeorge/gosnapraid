package snapshot

import (
	"os"
	"strings"

	"github.com/garethgeorge/gosnapraid/internal/hashing"
)

// SnapshotNodeType is the type of a node in the snapshot
type SnapshotNodeType uint8

const (
	SNAPSHOT_NODE_TYPE_UNKNOWN SnapshotNodeType = 0
	SNAPSHOT_NODE_TYPE_FILE    SnapshotNodeType = 1
	SNAPSHOT_NODE_TYPE_DIR     SnapshotNodeType = 2
)

type SnapshotNodeMetadata struct {
	// Name of the file in its directory
	Name string

	// Type of the node (file or directory)
	Type SnapshotNodeType

	// Inode number
	Ino uint64

	// Generation number
	Gen uint64

	// File size in bytes
	Size uint64

	// File mode and permission bits
	Mode os.FileMode

	// Access time (in microseconds since the epoch, if available on the filesystem)
	AccessTime int64

	// Change time (metadata change time, in microseconds since the epoch, if available on the filesystem)
	ChangeTime int64

	// Birth time (creation time, in microseconds since the epoch, if available on the filesystem)
	BirthTime int64

	// User ID of owner (Unix systems)
	UID uint32

	// Group ID of owner (Unix systems)
	GID uint32

	// Device ID (for special files)
	DeviceID uint64

	// Content hash
	ContentHash hashing.ContentHash
}

func SnapshotNodeMetadataFromFinfo(finfo os.FileInfo) (SnapshotNodeMetadata, error) {
	metadata, err := getUnixMetadata(finfo)
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}

	entryType := SNAPSHOT_NODE_TYPE_UNKNOWN
	if finfo.IsDir() {
		entryType = SNAPSHOT_NODE_TYPE_DIR
	} else if finfo.Mode().IsRegular() {
		entryType = SNAPSHOT_NODE_TYPE_FILE
	}

	return SnapshotNodeMetadata{
		Name:       finfo.Name(),
		Type:       entryType,
		Mode:       finfo.Mode(),
		AccessTime: metadata.AccessTime,
		ChangeTime: metadata.ChangeTime,
		BirthTime:  metadata.BirthTime,
		Size:       uint64(finfo.Size()),
		Ino:        metadata.Ino,
		Gen:        metadata.Gen,
		UID:        metadata.UID,
		GID:        metadata.GID,
		DeviceID:   metadata.DeviceID,
	}, nil
}

func SnapshotNodeMetadataFromDirEntry(entry os.DirEntry) (SnapshotNodeMetadata, error) {
	finfo, err := entry.Info()
	if err != nil {
		return SnapshotNodeMetadata{}, err
	}
	return SnapshotNodeMetadataFromFinfo(finfo)
}

func (m *SnapshotNodeMetadata) ComparePartialFields(other *SnapshotNodeMetadata) int {
	if m.Name != other.Name {
		return strings.Compare(m.Name, other.Name)
	}
	if m.Type != other.Type {
		return int(m.Type - other.Type)
	}
	if m.Ino != other.Ino {
		return int(m.Ino - other.Ino)
	}
	if m.Gen != other.Gen {
		return int(m.Gen - other.Gen)
	}
	if m.Size != other.Size {
		return int(m.Size - other.Size)
	}
	if m.Mode != other.Mode {
		return int(m.Mode - other.Mode)
	}
	if m.AccessTime != other.AccessTime {
		return int(m.AccessTime - other.AccessTime)
	}
	return 0
}

func (m *SnapshotNodeMetadata) Compare(other *SnapshotNodeMetadata) int {
	cmpPartialFields := m.ComparePartialFields(other)
	if cmpPartialFields != 0 {
		return cmpPartialFields
	}
	return m.ContentHash.Compare(other.ContentHash)
}
